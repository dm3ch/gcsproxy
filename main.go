package main

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gorilla/mux"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	"google.golang.org/api/iterator"
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
	dirTpl    = `
<html><head><title>Index of {{ .Prefix }}</title></head>
<body>
<h1>Index of {{ .Prefix }}</h1><hr><pre>
{{ if ne .Prefix "/"}}<a href="../">../</a>{{ end }}
<table>
{{- range .Items }}
<tr><td><a href="{{ .RelativePath }}">{{ .RelativePath }}</a></td><td>{{ .ModifiedDate }}</td><td>{{ .SizeBytes }}</td></tr>
{{- end -}}
</table>
</pre><hr>
</body></html>`
)

type dirItem struct {
	RelativePath string
	ModifiedDate string
	SizeBytes    string
}

type dirTplArgs struct {
	Prefix string
	Items  []dirItem
}

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

	return u, nil
}

func getFile(w http.ResponseWriter, r *http.Request, bucket, object string) {
	obj := client.Bucket(bucket).Object(object)
	attr, err := obj.Attrs(ctx)
	if err != nil {
		handleError(w, err)
		return
	}
	objr, err := obj.NewReader(ctx)
	defer objr.Close()

	if err != nil {
		handleError(w, err)
		return
	}

	if *signedUrlGet {
		u, err := generateV4GetObjectSignedURL(bucket, object)
		if err != nil {
			handleError(w, err)
			return
		}
		http.Redirect(w, r, u, http.StatusTemporaryRedirect)
	} else {
		setStrHeader(w, "Content-Type", attr.ContentType)
		setStrHeader(w, "Content-Language", attr.ContentLanguage)
		setStrHeader(w, "Cache-Control", attr.CacheControl)
		setStrHeader(w, "Content-Encoding", attr.ContentEncoding)
		setStrHeader(w, "Content-Disposition", attr.ContentDisposition)
		setIntHeader(w, "Content-Length", attr.Size)
		io.Copy(w, objr)
	}
}

func getDir(w http.ResponseWriter, r *http.Request, bucket, prefix string) {
	bkt := client.Bucket(bucket)

	if len(prefix) != 0 && prefix[len(prefix)-1:] != "/" {
		http.Redirect(w, r, r.RequestURI+"/", http.StatusTemporaryRedirect)
	}

	query := &storage.Query{Prefix: prefix}
	var items []dirItem
	lastName := ""
	it := bkt.Objects(ctx, query)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		name := strings.TrimPrefix(attrs.Name, prefix)
		// Skips directory itself
		if len(name) == 0 {
			continue
		}

		//Get only items in this dir, not in sub dir
		nameParts := strings.Split(name, "/")
		name = nameParts[0]
		if name == lastName {
			continue
		} else {
			lastName = name
		}

		var item dirItem
		item.RelativePath = name
		item.ModifiedDate = attrs.Created.Format("02-Jan-2006 15:04 UTC")
		if len(nameParts) > 1 {
			item.SizeBytes = "-"
		} else {
			item.SizeBytes = strconv.FormatInt(attrs.Size, 10)
		}

		items = append(items, item)
	}

	if prefix == "" {
		prefix = "/"
	}

	tmpl := template.Must(template.New("dir").Parse(dirTpl))
	tmpl.Execute(w, dirTplArgs{
		Prefix: prefix,
		Items:  items,
	})

	return
}

func isDirectory(bucket, prefix string) bool {
	if len(prefix) != 0 && prefix[len(prefix)-1:] != "/" {
		prefix += "/"
	}
	bkt := client.Bucket(bucket)
	query := &storage.Query{Prefix: prefix}
	item, _ := bkt.Objects(ctx, query).Next()
	return item != nil
}

func proxy(w http.ResponseWriter, r *http.Request) {
	params := mux.Vars(r)
	bkt := client.Bucket(params["bucket"])
	obj := bkt.Object(params["object"])
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if isDirectory(params["bucket"], params["object"]) {
			getDir(w, r, params["bucket"], params["object"])
		} else {
			getFile(w, r, params["bucket"], params["object"])
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
