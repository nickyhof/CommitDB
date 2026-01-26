# Installation

## Go CLI & Server

### From Source

```bash
# Clone the repository
git clone https://github.com/nickyhof/CommitDB.git
cd CommitDB

# Build CLI
go build -o commitdb-cli ./cmd/cli

# Build Server
go build -o commitdb-server ./cmd/server
```

### Using Go Install

```bash
go install github.com/nickyhof/CommitDB/cmd/cli@latest
go install github.com/nickyhof/CommitDB/cmd/server@latest
```

## Docker

```bash
# Pull from GitHub Container Registry
docker pull ghcr.io/nickyhof/commitdb:latest

# Run with in-memory storage
docker run -p 3306:3306 ghcr.io/nickyhof/commitdb

# Run with persistent storage
docker run -p 3306:3306 -v /path/to/data:/data ghcr.io/nickyhof/commitdb
```

### With TLS

```bash
docker run -p 3306:3306 \
  -v /path/to/certs:/certs \
  ghcr.io/nickyhof/commitdb \
  --tls-cert /certs/cert.pem --tls-key /certs/key.pem
```

### With JWT Authentication

```bash
docker run -p 3306:3306 \
  ghcr.io/nickyhof/commitdb \
  --jwt-secret "your-secret" --jwt-issuer "https://auth.example.com"
```

## Python Driver

```bash
pip install commitdb
```

See [Python Driver](python-driver.md) for usage.

## Go Library

```bash
go get github.com/nickyhof/CommitDB
```

See [Go API](go-api.md) for usage.
