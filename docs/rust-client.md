# Rust Client

[![Crates.io](https://img.shields.io/crates/v/commitdb.svg)](https://crates.io/crates/commitdb)
[![Docs.rs](https://docs.rs/commitdb/badge.svg)](https://docs.rs/commitdb)

The CommitDB Rust client provides a blocking TCP/TLS client for connecting to CommitDB servers.

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
        // [("id", "1"), ("name", "Alice")]
    }

    db.close();
    Ok(())
}
```

## API Reference

### CommitDB

```rust
// Simple connection
let mut db = CommitDB::connect("localhost", 3306)?;

// With full options
let opts = ConnectOptions::new("localhost", 3306)
    .with_tls(true)
    .with_jwt_token("eyJhbG...");
let mut db = CommitDB::connect_with(opts)?;
```

**Methods:**

| Method | Description |
|--------|-------------|
| `connect(host, port)` | Connect with defaults |
| `connect_with(opts)` | Connect with full options |
| `execute(sql)` | Execute SQL, returns `ExecuteResult` |
| `query(sql)` | Execute SELECT, returns `QueryResult` |
| `authenticate_jwt(token)` | Authenticate with JWT |
| `is_authenticated()` | Check auth status |
| `identity()` | Get authenticated identity |
| `close()` | Close connection (also called on `Drop`) |

### ConnectOptions

```rust
ConnectOptions::new("localhost", 3306)
    .with_tls(true)                      // Enable TLS
    .with_tls_verify(false)              // Skip cert verification (dev only)
    .with_tls_ca_cert("cert.pem")        // CA certificate path
    .with_jwt_token("eyJhbG...")         // Auto-authenticate on connect
    .with_timeout(Duration::from_secs(5)) // Connection timeout
```

### QueryResult

```rust
let result = db.query("SELECT * FROM mydb.users")?;
result.columns;       // Vec<String>: ["id", "name"]
result.data;          // Vec<Vec<String>>: [["1", "Alice"]]
result.len();         // usize: 1
result.is_empty();    // bool: false
result.row(0);        // Option<Vec<(&str, &str)>>

for row in &result {
    // row: Vec<(&str, &str)> = [("id", "1"), ("name", "Alice")]
    println!("{:?}", row);
}
```

### CommitResult

```rust
let result = db.execute("INSERT INTO mydb.users VALUES (2, 'Bob')")?;
if let ExecuteResult::Commit(commit) = result {
    commit.records_written;    // i64: 1
    commit.affected_rows();    // i64: 1
    commit.execution_time_ms;  // f64
}
```

### ExecuteResult

```rust
use commitdb::ExecuteResult;

match db.execute(sql)? {
    ExecuteResult::Query(q)  => println!("Got {} rows", q.len()),
    ExecuteResult::Commit(c) => println!("Affected {} rows", c.affected_rows()),
}
```

## SSL/TLS Encryption

```rust
use commitdb::{CommitDB, ConnectOptions};

// Production (verify certificate)
let opts = ConnectOptions::new("db.example.com", 3306)
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
assert!(db.is_authenticated());
println!("Identity: {:?}", db.identity());

// Or authenticate after connecting
let mut db = CommitDB::connect("localhost", 3306)?;
let auth = db.authenticate_jwt("eyJhbG...")?;
println!("Authenticated: {}", auth.authenticated);
println!("Identity: {:?}", auth.identity);
println!("Expires in: {:?}s", auth.expires_in);
```

## Error Handling

All fallible methods return `commitdb::Result<T>`, which uses the `Error` enum:

```rust
use commitdb::{CommitDB, Error};

let mut db = CommitDB::connect("localhost", 3306)?;

match db.execute("SELECT * FROM nonexistent.table") {
    Ok(result) => { /* handle result */ }
    Err(Error::Server(msg)) => println!("Database error: {msg}"),
    Err(Error::Connection(e)) => println!("Network error: {e}"),
    Err(Error::Tls(e)) => println!("TLS error: {e}"),
    Err(Error::Auth(msg)) => println!("Auth error: {msg}"),
    Err(Error::NotConnected) => println!("Not connected"),
    Err(e) => println!("Other error: {e}"),
}
```

## Branching & Merging

```rust
let mut db = CommitDB::connect("localhost", 3306)?;
db.execute("CREATE DATABASE mydb")?;
db.execute("CREATE TABLE mydb.users (id INT, name STRING)")?;
db.execute("INSERT INTO mydb.users VALUES (1, 'Alice')")?;

// Branch, modify, merge
db.execute("CREATE BRANCH feature")?;
db.execute("CHECKOUT feature")?;
db.execute("INSERT INTO mydb.users VALUES (2, 'Bob')")?;
db.execute("CHECKOUT master")?;
db.execute("MERGE feature")?;

let result = db.query("SELECT * FROM mydb.users")?;
assert_eq!(result.len(), 2);
```
