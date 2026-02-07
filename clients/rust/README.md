# CommitDB Rust Client

[![Crates.io](https://img.shields.io/crates/v/commitdb.svg)](https://crates.io/crates/commitdb)
[![Docs.rs](https://docs.rs/commitdb/badge.svg)](https://docs.rs/commitdb)

Rust client for CommitDB — a Git-backed SQL database engine.

**[📚 Full Documentation](https://nickyhof.github.io/CommitDB/rust-client/)**

> ⚠️ **Experimental Project** — This is a hobby project and should not be used in any production environment.

## Installation

```toml
[dependencies]
commitdb = "2"
```

## Quick Start

```rust
use commitdb::CommitDB;

fn main() -> commitdb::Result<()> {
    let mut db = CommitDB::connect("localhost", 3306)?;

    db.execute("CREATE DATABASE mydb")?;
    db.execute("CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)")?;
    db.execute("INSERT INTO mydb.users VALUES (1, 'Alice')")?;

    let result = db.query("SELECT * FROM mydb.users")?;
    for row in &result {
        println!("{:?}", row);
    }
    // [("id", "1"), ("name", "Alice")]

    db.close();
    Ok(())
}
```

## TLS/SSL

```rust
use commitdb::{CommitDB, ConnectOptions};

// Production (verify certificate)
let opts = ConnectOptions::new("localhost", 3306)
    .with_tls(true)
    .with_tls_ca_cert("cert.pem");
let mut db = CommitDB::connect_with(opts)?;

// Development (skip verification)
let opts = ConnectOptions::new("localhost", 3306)
    .with_tls(true)
    .with_tls_verify(false);
let mut db = CommitDB::connect_with(opts)?;
```

## JWT Authentication

```rust
use commitdb::{CommitDB, ConnectOptions};

// Auto-authenticate on connect
let opts = ConnectOptions::new("localhost", 3306)
    .with_jwt_token("eyJhbG...");
let mut db = CommitDB::connect_with(opts)?;
println!("Authenticated as: {:?}", db.identity());

// Or authenticate after connecting
let mut db = CommitDB::connect("localhost", 3306)?;
let auth = db.authenticate_jwt("eyJhbG...")?;
println!("Identity: {:?}", auth.identity);
```

## API Reference

### `CommitDB`

| Method | Description |
|--------|-------------|
| `connect(host, port)` | Connect with defaults |
| `connect_with(opts)` | Connect with full options |
| `execute(sql)` | Execute SQL, returns `ExecuteResult` |
| `query(sql)` | Execute SELECT, returns `QueryResult` |
| `authenticate_jwt(token)` | Authenticate with JWT |
| `close()` | Close connection |

### Result Types

| Type | Description |
|------|-------------|
| `ExecuteResult` | Enum: `Query(QueryResult)` or `Commit(CommitResult)` |
| `QueryResult` | `.columns`, `.data`, `.len()`, `.row(i)`, `.iter()` |
| `CommitResult` | `.records_written`, `.tables_created`, `.affected_rows()` |
| `AuthResult` | `.authenticated`, `.identity`, `.expires_in` |

### Error Handling

```rust
use commitdb::{CommitDB, Error};

match db.execute("SELECT * FROM nonexistent.table") {
    Ok(result) => println!("Success"),
    Err(Error::Server(msg)) => println!("DB error: {msg}"),
    Err(Error::Connection(e)) => println!("Network error: {e}"),
    Err(e) => println!("Other error: {e}"),
}
```
