# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git (required for go-git)
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the server binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-X main.Version=${VERSION}" -o commitdb-server ./cmd/server

# Runtime stage
FROM alpine:3.20

WORKDIR /data

# Install git for repository operations
RUN apk add --no-cache git ca-certificates

# Copy binary from builder
COPY --from=builder /app/commitdb-server /usr/local/bin/

# Default port
EXPOSE 3306

# Default data directory
VOLUME /data

# Run server with file persistence
ENTRYPOINT ["commitdb-server"]
CMD ["--baseDir", "/data"]
