FROM golang:1.18-alpine as builder

ADD . /koinos-bridge
WORKDIR /koinos-bridge

RUN apk update && \
    apk add \
        gcc \
        musl-dev \
        linux-headers

RUN go get ./... && \
    go build -o koinos_bridge cmd/koinos-bridge-validator/main.go

FROM alpine:latest
COPY --from=builder /koinos-bridge/koinos_bridge /usr/local/bin
ENTRYPOINT [ "/usr/local/bin/koinos_bridge" ]
