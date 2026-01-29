# CLI & Server Reference

CommitDB provides two ways to interact with your database: the **CLI** for direct local access and the **Server** for network access.

## When to Use Which

| Scenario | CLI | Server |
|----------|-----|--------|
| Local development & debugging | ✅ | |
| Exploring data interactively | ✅ | |
| Running SQL scripts | ✅ | |
| Bulk data import/export | ✅ | |
| Application integration | | ✅ |
| Multi-user access | | ✅ |
| Remote connections | | ✅ |
| Production deployments | | ✅ |
| Python client access | | ✅ |

---

## CLI (`commitdb-cli`)

The CLI provides a REPL for direct database interaction.

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `-baseDir` | Directory for file persistence | *(memory mode)* |
| `-gitUrl` | Git remote URL for sync | *(none)* |
| `-name` | User name for Git commits | `CommitDB` |
| `-email` | User email for Git commits | `cli@commitdb.local` |
| `-sqlFile` | SQL file to execute (non-interactive) | *(none)* |

### Examples

**Memory mode** (data lost on exit):
```bash
./commitdb-cli
```

**File persistence** (data stored in Git repo):
```bash
./commitdb-cli -baseDir=/path/to/data
```

**With custom identity** (for Git commits):
```bash
./commitdb-cli -baseDir=/path/to/data -name="Alice" -email="alice@example.com"
```

**Execute SQL file**:
```bash
./commitdb-cli -baseDir=/path/to/data -sqlFile=schema.sql
```

**Clone from remote Git repo**:
```bash
./commitdb-cli -baseDir=/path/to/data -gitUrl=git@github.com:user/repo.git
```

### Interactive Commands

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.quit`, `.exit` | Exit the CLI |
| `.databases` | List all databases |
| `.tables <db>` | List tables in a database |
| `.use <db>` | Set default database |
| `.import <file>` | Execute SQL from file |
| `.history` | Show command history |
| `.clear` | Clear screen |
| `.version` | Show version |

---

## Server (`commitdb-server`)

The server exposes a TCP protocol for network access, allowing connections from the Python driver and other clients.

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `-port` | TCP port to listen on | `3306` |
| `-baseDir` | Directory for file persistence | *(memory mode)* |
| `-gitUrl` | Git remote URL for sync | *(none)* |
| `-version` | Show version and exit | |

**TLS Options:**

| Flag | Description |
|------|-------------|
| `-tls-cert` | Path to TLS certificate file |
| `-tls-key` | Path to TLS private key file |

**JWT Authentication Options:**

| Flag | Description | Default |
|------|-------------|---------|
| `-jwt-secret` | JWT shared secret for HS256 (enables auth) | *(none)* |
| `-jwt-issuer` | Expected JWT issuer claim | *(any)* |
| `-jwt-audience` | Expected JWT audience claim | *(any)* |
| `-jwt-name-claim` | JWT claim for user name | `name` |
| `-jwt-email-claim` | JWT claim for user email | `email` |

### Examples

**Basic server** (memory mode):
```bash
./commitdb-server -port 3306
```

**With file persistence**:
```bash
./commitdb-server -port 3306 -baseDir=/var/lib/commitdb
```

**With TLS encryption**:
```bash
./commitdb-server -port 3306 -baseDir=/var/lib/commitdb \
    -tls-cert=/path/to/cert.pem \
    -tls-key=/path/to/key.pem
```

**With JWT authentication**:
```bash
./commitdb-server -port 3306 -baseDir=/var/lib/commitdb \
    -jwt-secret="your-secret-key" \
    -jwt-issuer="https://auth.example.com"
```

### Connecting to the Server

**Python:**
```python
from commitdb import CommitDB

db = CommitDB('localhost', 3306)
db.connect()
result = db.execute("SELECT * FROM mydb.users")
db.close()
```

**With TLS (Python):**
```python
db = CommitDB('localhost', 3306, ssl=True, ssl_verify=False)
db.connect()
```

**With JWT auth (Python):**
```python
db = CommitDB('localhost', 3306)
db.connect(token="your-jwt-token")
```

---

## Docker

### CLI
```bash
docker run -it -v /path/to/data:/data ghcr.io/nickyhof/commitdb:latest \
    commitdb-cli -baseDir=/data
```

### Server
```bash
docker run -d -p 3306:3306 -v /path/to/data:/data ghcr.io/nickyhof/commitdb:latest \
    commitdb-server -port 3306 -baseDir=/data
```

---

## Persistence Modes

Both CLI and Server support two persistence modes:

### Memory Mode
- Data exists only while the process runs
- Fast for development/testing
- No `-baseDir` flag needed

### File Persistence Mode  
- Data stored in a local Git repository
- Full version history via Git commits
- Survives restarts
- Can sync with remote Git repos

---

## Git Identity

Operations that modify data create Git commits. The identity used for commits:

| Mode | Name | Email |
|------|------|-------|
| CLI (default) | `CommitDB` | `cli@commitdb.local` |
| CLI (custom) | `-name` flag | `-email` flag |
| Server (no auth) | `CommitDB` | `server@commitdb.local` |
| Server (JWT auth) | From JWT token | From JWT token |
