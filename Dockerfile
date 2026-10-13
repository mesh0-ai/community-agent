# syntax=docker/dockerfile:1.7
FROM golang:1.24-alpine AS build
WORKDIR /src
ARG VERSION=dev
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY trace ./trace
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w -X main.Version=${VERSION}" \
    -o /out/mesh0-community-agent ./cmd/community-agent

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/mesh0-community-agent /mesh0-community-agent
# The agent listens on a Unix-domain socket (MESH0_LISTEN_PATH, mode 0666);
# only the health server listens on TCP.
EXPOSE 8126/tcp
# 65532 is distroless "nonroot", the same UID metrics-agent runs as.
USER 65532:65532
ENTRYPOINT ["/mesh0-community-agent"]
