FROM golang:1.14-buster AS builder

ENV GO111MODULE=on
WORKDIR /app
COPY go.mod .
COPY go.sum .
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build . && \
    mv gcsproxy /usr/local/bin/gcsproxy


FROM gcr.io/distroless/base-debian10 AS release

COPY --from=builder /usr/local/bin/gcsproxy /usr/local/bin/gcsproxy

EXPOSE 8080

ENTRYPOINT ["gcsproxy"]
CMD ["-b", "0.0.0.0:8080"]
