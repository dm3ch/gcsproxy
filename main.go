package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/option"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	bind             = kingpin.Flag("bind-address", "Bind address").Short('b').Envar("GCSPROXY_BIND_ADDRESS").Default("127.0.0.1:8080").String()
	verbose          = kingpin.Flag("verbose", "Show access log").Short('v').Envar("GCSPROXY_VERBOSE").Default("true").Bool()
	credentials      = kingpin.Flag("credentials", "The path to the keyfile. If not present, client will use your default application credentials.").Short('c').Envar("GCSPROXY_CREDENTIALS").String()
	readinessBuckets = kingpin.Flag("readiness-buckets", "Comma-separated list of bucket names to ping for readiness checks").Short('r').Envar("GCSPROXY_READINESS_BUCKETS").Default("gcp-public-data-landsat,gcp-public-data-nexrad-l2,gcp-public-data-sentinel-2").String()
	signedUrlGet     = kingpin.Flag("get-signed-url", "Returns pre-signed url on get requests instead of actual data").Envar("GCSPROXY_GET_SIGNED_URL").Default("false").Bool()
)

var (
	client    *storage.Client
	jwtConfig *jwt.Config
	ctx       = context.Background()
)

func handleError(w http.ResponseWriter, err error) {
	if err != nil {
		if err == storage.ErrObjectNotExist {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

func header(r *http.Request, key string) (string, bool) {
	if r.Header == nil {
		return "", false
	}
	if candidate := r.Header[key]; len(candidate) > 0 {
		return candidate[0], true
	}
	return "", false
}

func setStrHeader(w http.ResponseWriter, key string, value string) {
	if value != "" {
		w.Header().Add(key, value)
	}
}

func setIntHeader(w http.ResponseWriter, key string, value int64) {
	if value > 0 {
		w.Header().Add(key, strconv.FormatInt(value, 10))
	}
}

type wrapResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *wrapResponseWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.status = status
}

func wrapper(fn func(w http.ResponseWriter, r *http.Request)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		proc := time.Now()
		writer := &wrapResponseWriter{
			ResponseWriter: w,
			status:         http.StatusOK,
		}
		fn(writer, r)
		addr := r.RemoteAddr
		if ip, found := header(r, "X-Forwarded-For"); found {
			addr = ip
		}
		if *verbose {
			log.Printf("[%s] %.3f %d %s %s",
				addr,
				time.Now().Sub(proc).Seconds(),
				writer.status,
				r.Method,
				r.URL,
			)
		}
	}
}

// generateV4GetObjectSignedURL generates object signed URL with GET method.
func generateV4GetObjectSignedURL(bucket, object string) (string, error) {
	opts := &storage.SignedURLOptions{
		Scheme:         storage.SigningSchemeV4,
		Method:         "GET",
		GoogleAccessID: jwtConfig.Email,
		PrivateKey:     jwtConfig.PrivateKey,
		Expires:        time.Now().Add(1 * time.Hour),
	}
	u, err := storage.SignedURL(bucket, object, opts)
	if err != nil {
		return "", fmt.Errorf("storage.SignedURL: %v", err)
	}

	fmt.Println("Generated GET signed URL:")
	fmt.Printf("%q\n", u)
	fmt.Println("You can use this URL with any user agent, for example:")
	fmt.Printf("curl %q\n", u)
	return u, nil
}

func proxy(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	obj := client.Bucket(params["bucket"]).Object(params["object"])
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		attr, err := obj.Attrs(ctx)
		if err != nil {
			handleError(w, err)
			return
		}
		objr, err := obj.NewReader(ctx)
		if err != nil {
			handleError(w, err)
			return
		}
		_, err = url.Parse(r.RequestURI)
		if err != nil {
			handleError(w, err)
			return
		}

		if *signedUrlGet {
			u, err := generateV4GetObjectSignedURL(params["bucket"], params["object"])
			if err != nil {
				handleError(w, err)
				return
			}
			http.Redirect(w, r, u, 307)
		} else {
			setStrHeader(w, "Content-Type", attr.ContentType)
			setStrHeader(w, "Content-Language", attr.ContentLanguage)
			setStrHeader(w, "Cache-Control", attr.CacheControl)
			setStrHeader(w, "Content-Encoding", attr.ContentEncoding)
			setStrHeader(w, "Content-Disposition", attr.ContentDisposition)
			setIntHeader(w, "Content-Length", attr.Size)
			io.Copy(w, objr)
		}

	case http.MethodPost, http.MethodPut:
		wc := obj.NewWriter(ctx)
		fileData, err := ioutil.ReadAll(r.Body)
		if err != nil {
			handleError(w, err)
			return
		}
		if _, err := wc.Write(fileData); err != nil {
			handleError(w, err)
			return
		}
		if err := wc.Close(); err != nil {
			handleError(w, err)
			return
		}
	case http.MethodDelete:
		if err := obj.Delete(ctx); err != nil {
			handleError(w, err)
			return
		}
	default:
		http.Error(w, "Method Not Allowed", http.StatusNotFound)
		return
	}
}

// livenessProbeHandler tests whether the http server has hung by attempting to return a simple request
func livenessProbeHandler(w http.ResponseWriter, r *http.Request) {
	return
}

// readinessProbeHandler attempts to get the metadata from each of a list of Google buckets provided as an argument via
// --readiness-buckets. If it fails to retrieve metadata from ALL buckets provided the handler will return a 503
// response. The default list of buckets provided contains the three public Google buckets (see
// https://cloud.google.com/storage/docs/public-datasets).
func readinessProbeHandler(w http.ResponseWriter, r *http.Request) {

	type bucketResponse struct {
		bucketName string
		err        error
	}

	var bucketList = strings.Split(*readinessBuckets, ",")
	var ch = make(chan bucketResponse, len(bucketList))

	for _, bucket := range bucketList {
		go func(bucket string) {
			var br bucketResponse
			_, err := client.Bucket(bucket).Attrs(ctx)
			br.bucketName, br.err = bucket, err
			ch <- br
		}(bucket)
	}

	for range bucketList {
		br := <-ch
		if br.err == nil {
			if *verbose {
				log.Printf("received metadata for bucket %s",
					br.bucketName,
				)
			}
			return
		}
		log.Printf("error receiving metadata for bucket %s: %s",
			br.bucketName,
			br.err,
		)
	}
	w.WriteHeader(http.StatusServiceUnavailable)
}

func initClient() {
	var err error
	if *credentials != "" {
		client, err = storage.NewClient(ctx, option.WithCredentialsFile(*credentials))
		log.Printf("Starting gcsproxy with credentials")
	} else {
		client, err = storage.NewClient(ctx)
		log.Printf("Starting gcsproxy without credentials")
	}
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	if *signedUrlGet {
		jsonKey, err := ioutil.ReadFile(*credentials)
		if err != nil {
			log.Fatalf("ioutil.ReadFile: %v", err)
		}
		jwtConfig, err = google.JWTConfigFromJSON(jsonKey)
		if err != nil {
			log.Fatalf("google.JWTConfigFromJSON: %v", err)
		}
	}
}

func main() {

	kingpin.Parse()

	initClient()

	r := mux.NewRouter()
	r.HandleFunc("/{bucket:[0-9a-zA-Z-_.]+}/{object:.*}", wrapper(proxy)).Methods("GET", "HEAD", "PUT", "POST", "DELETE")
	r.HandleFunc("/healthz", wrapper(livenessProbeHandler)).Methods("GET")
	r.HandleFunc("/readiness", wrapper(readinessProbeHandler)).Methods("GET")

	log.Printf("[service] listening on %s", *bind)
	if err := http.ListenAndServe(*bind, r); err != nil {
		log.Fatal(err)
	}
}
