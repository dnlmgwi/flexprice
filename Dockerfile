# syntax=docker/dockerfile:1

#####################################
# 1) Builder stage
#####################################
FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS builder
WORKDIR /app

ARG TARGETOS=linux
ARG TARGETARCH=amd64

# cache Go module downloads
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# copy source & build binary for target platform
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -ldflags="-w -s" -trimpath \
      -o server cmd/server/main.go

#####################################
# 2) Typst binary stage
#####################################
FROM ghcr.io/typst/typst:v0.13.1 AS typst

#####################################
# 3) Runtime stage
#####################################
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/server /app/server
COPY --from=typst /bin/typst /usr/local/bin/

COPY internal/config ./config
COPY assets/fonts ./assets/fonts
COPY assets/typst-templates ./assets/typst-templates
COPY assets/email-templates ./assets/email-templates

RUN chmod +x /app/server

EXPOSE 8080
CMD ["/app/server"]
