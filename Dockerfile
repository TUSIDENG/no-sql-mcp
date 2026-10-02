# Build stage: official Go image.
FROM golang:1.24-alpine AS build

WORKDIR /src

# Cache dependencies first.
COPY go.mod go.sum ./
RUN go mod download

# Build a static binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/server ./cmd/server

# Runtime stage: minimal image.
FROM alpine:3.20

RUN apk add --no-cache ca-certificates && \
    addgroup -S app && adduser -S app -G app

WORKDIR /app
COPY --from=build /out/server /app/server

USER app

ENTRYPOINT ["/app/server"]
CMD ["--config", "/app/config.json", "--lazy-loading"]
