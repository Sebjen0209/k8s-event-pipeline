# syntax=docker/dockerfile:1
# One Dockerfile builds both services: --build-arg SERVICE=ingest-api|worker

FROM golang:1.25-alpine AS build
WORKDIR /src
# Dependency layer caches independently of source changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SERVICE
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${SERVICE}

# Distroless static: no shell, no package manager, ~2 MB attack surface.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
