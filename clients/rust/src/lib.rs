//! # CommitDB Rust Client
//!
//! A blocking TCP/TLS client for [CommitDB](https://github.com/nickyhof/CommitDB).
//!
//! ```no_run
//! use commitdb::CommitDB;
//!
//! let mut db = CommitDB::connect("localhost", 3306)?;
//! db.execute("CREATE DATABASE mydb")?;
//! db.execute("CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)")?;
//! db.execute("INSERT INTO mydb.users VALUES (1, 'Alice')")?;
//!
//! let result = db.query("SELECT * FROM mydb.users")?;
//! for row in &result {
//!     println!("{:?}", row);
//! }
//!
//! db.close();
//! # Ok::<(), commitdb::Error>(())
//! ```

mod error;
mod protocol;
mod tls;

pub use error::{Error, Result};

use protocol::{AuthResponseData, CommitResponseData, QueryResponseData, Response};

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::Duration;

// ---------------------------------------------------------------------------
// Public result types
// ---------------------------------------------------------------------------

/// Result from a SELECT / SHOW / DESCRIBE query.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Column names.
    pub columns: Vec<String>,
    /// Row data — each row is a `Vec<String>` of column values.
    pub data: Vec<Vec<String>>,
    /// Number of records the engine scanned.
    pub records_read: i64,
    /// Server-side execution time in milliseconds.
    pub execution_time_ms: f64,
    /// Number of operations performed.
    pub execution_ops: i64,
}

impl QueryResult {
    /// Number of rows in the result set.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Whether the result set is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Get a single row as a `Vec<(column_name, value)>` pairs.
    pub fn row(&self, index: usize) -> Option<Vec<(&str, &str)>> {
        self.data.get(index).map(|row| {
            self.columns
                .iter()
                .zip(row.iter())
                .map(|(c, v)| (c.as_str(), v.as_str()))
                .collect()
        })
    }

    /// Iterate over rows, each as a `Vec<(&str, &str)>` of (column, value) pairs.
    pub fn iter(&self) -> impl Iterator<Item = Vec<(&str, &str)>> {
        self.data.iter().map(move |row| {
            self.columns
                .iter()
                .zip(row.iter())
                .map(|(c, v)| (c.as_str(), v.as_str()))
                .collect()
        })
    }
}

impl<'a> IntoIterator for &'a QueryResult {
    type Item = Vec<(&'a str, &'a str)>;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.iter())
    }
}

/// Result from a mutation (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
#[derive(Debug, Clone, Default)]
pub struct CommitResult {
    pub databases_created: i64,
    pub databases_deleted: i64,
    pub tables_created: i64,
    pub tables_deleted: i64,
    pub records_written: i64,
    pub records_deleted: i64,
    pub execution_time_ms: f64,
    pub execution_ops: i64,
}

impl CommitResult {
    /// Total number of affected rows/objects.
    pub fn affected_rows(&self) -> i64 {
        self.databases_created
            + self.databases_deleted
            + self.tables_created
            + self.tables_deleted
            + self.records_written
            + self.records_deleted
    }
}

/// Result of executing a SQL statement — either a query or a mutation.
#[derive(Debug, Clone)]
pub enum ExecuteResult {
    Query(QueryResult),
    Commit(CommitResult),
}

/// Authentication result returned by [`CommitDB::authenticate_jwt`].
#[derive(Debug, Clone)]
pub struct AuthResult {
    pub authenticated: bool,
    pub identity: Option<String>,
    pub expires_in: Option<i64>,
}

// ---------------------------------------------------------------------------
// Connection options
// ---------------------------------------------------------------------------

/// Configuration for connecting to a CommitDB server.
///
/// ```no_run
/// use commitdb::ConnectOptions;
///
/// let opts = ConnectOptions::new("localhost", 3306)
///     .with_tls(true)
///     .with_tls_verify(false)
///     .with_jwt_token("eyJhbG...");
/// ```
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    pub host: String,
    pub port: u16,
    pub use_tls: bool,
    pub tls_verify: bool,
    pub tls_ca_cert: Option<PathBuf>,
    pub jwt_token: Option<String>,
    pub timeout: Duration,
}

impl ConnectOptions {
    /// Create options for `host:port` with sensible defaults.
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
            use_tls: false,
            tls_verify: true,
            tls_ca_cert: None,
            jwt_token: None,
            timeout: Duration::from_secs(10),
        }
    }

    pub fn with_tls(mut self, enabled: bool) -> Self {
        self.use_tls = enabled;
        self
    }

    pub fn with_tls_verify(mut self, verify: bool) -> Self {
        self.tls_verify = verify;
        self
    }

    pub fn with_tls_ca_cert(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_ca_cert = Some(path.into());
        self
    }

    pub fn with_jwt_token(mut self, token: impl Into<String>) -> Self {
        self.jwt_token = Some(token.into());
        self
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }
}

// ---------------------------------------------------------------------------
// Stream abstraction (plain TCP vs TLS)
// ---------------------------------------------------------------------------

enum Stream {
    Plain(BufReader<TcpStream>),
    Tls(BufReader<native_tls::TlsStream<TcpStream>>),
}

impl Stream {
    fn read_line(&mut self, buf: &mut String) -> std::io::Result<usize> {
        match self {
            Stream::Plain(r) => r.read_line(buf),
            Stream::Tls(r) => r.read_line(buf),
        }
    }

    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        match self {
            Stream::Plain(r) => r.get_mut().write_all(data),
            Stream::Tls(r) => r.get_mut().write_all(data),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Stream::Plain(r) => r.get_mut().flush(),
            Stream::Tls(r) => r.get_mut().flush(),
        }
    }
}

// ---------------------------------------------------------------------------
// CommitDB client
// ---------------------------------------------------------------------------

/// A blocking client for CommitDB.
pub struct CommitDB {
    stream: Option<Stream>,
    authenticated: bool,
    identity: Option<String>,
}

impl CommitDB {
    /// Connect to a CommitDB server with default options (no TLS, no auth).
    pub fn connect(host: &str, port: u16) -> Result<Self> {
        Self::connect_with(ConnectOptions::new(host, port))
    }

    /// Connect to a CommitDB server with full options.
    pub fn connect_with(opts: ConnectOptions) -> Result<Self> {
        let addr = format!("{}:{}", opts.host, opts.port);
        use std::net::ToSocketAddrs;
        let sock_addr = addr
            .to_socket_addrs()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?
            .next()
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "could not resolve address")
            })?;
        let tcp = TcpStream::connect_timeout(&sock_addr, opts.timeout)?;
        tcp.set_read_timeout(Some(opts.timeout))?;
        tcp.set_write_timeout(Some(opts.timeout))?;

        let stream = if opts.use_tls {
            let connector = tls::build_connector(
                opts.tls_verify,
                opts.tls_ca_cert.as_deref(),
            )?;
            let tls_stream = connector
                .connect(&opts.host, tcp)
                .map_err(|e| Error::TlsHandshake(e.to_string()))?;
            Stream::Tls(BufReader::new(tls_stream))
        } else {
            Stream::Plain(BufReader::new(tcp))
        };

        let mut client = Self {
            stream: Some(stream),
            authenticated: false,
            identity: None,
        };

        // Auto-authenticate if JWT token provided.
        if let Some(token) = &opts.jwt_token {
            let token = token.clone();
            client.authenticate_jwt(&token)?;
        }

        Ok(client)
    }

    /// Whether this connection has been authenticated.
    pub fn is_authenticated(&self) -> bool {
        self.authenticated
    }

    /// The authenticated identity (`"Name <email>"`), if any.
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    /// Authenticate with a JWT token.
    pub fn authenticate_jwt(&mut self, token: &str) -> Result<AuthResult> {
        let raw = self.send_raw(&format!("AUTH JWT {token}"))?;

        if !raw.success {
            return Err(Error::Auth(
                raw.error.unwrap_or_else(|| "unknown error".into()),
            ));
        }

        let data: AuthResponseData = match raw.result {
            Some(v) => serde_json::from_value(v)?,
            None => return Err(Error::Auth("empty auth response".into())),
        };

        self.authenticated = data.authenticated;
        self.identity = data.identity.clone();

        Ok(AuthResult {
            authenticated: data.authenticated,
            identity: data.identity,
            expires_in: data.expires_in,
        })
    }

    /// Execute a SQL statement.
    ///
    /// Returns [`ExecuteResult::Query`] for SELECT-like statements and
    /// [`ExecuteResult::Commit`] for mutations.
    pub fn execute(&mut self, sql: &str) -> Result<ExecuteResult> {
        let raw = self.send_raw(sql)?;

        if !raw.success {
            return Err(Error::Server(
                raw.error.unwrap_or_else(|| "unknown error".into()),
            ));
        }

        let result_value = raw.result.unwrap_or(serde_json::Value::Object(Default::default()));

        match raw.response_type.as_deref() {
            Some("query") => {
                let data: QueryResponseData = serde_json::from_value(result_value)?;
                Ok(ExecuteResult::Query(QueryResult {
                    columns: data.columns,
                    data: data.data,
                    records_read: data.records_read,
                    execution_time_ms: data.execution_time_ms,
                    execution_ops: data.execution_ops,
                }))
            }
            Some("commit") => {
                let data: CommitResponseData = serde_json::from_value(result_value)?;
                Ok(ExecuteResult::Commit(CommitResult {
                    databases_created: data.databases_created,
                    databases_deleted: data.databases_deleted,
                    tables_created: data.tables_created,
                    tables_deleted: data.tables_deleted,
                    records_written: data.records_written,
                    records_deleted: data.records_deleted,
                    execution_time_ms: data.execution_time_ms,
                    execution_ops: data.execution_ops,
                }))
            }
            _ => Ok(ExecuteResult::Commit(CommitResult::default())),
        }
    }

    /// Execute a SELECT query and return a [`QueryResult`].
    ///
    /// Returns [`Error::Server`] if the statement is not a query.
    pub fn query(&mut self, sql: &str) -> Result<QueryResult> {
        match self.execute(sql)? {
            ExecuteResult::Query(q) => Ok(q),
            ExecuteResult::Commit(_) => {
                Err(Error::Server("expected query result, got commit".into()))
            }
        }
    }

    /// Close the connection. Safe to call multiple times.
    pub fn close(&mut self) {
        if let Some(ref mut stream) = self.stream {
            let _ = stream.write_all(b"quit\n");
            let _ = stream.flush();
        }
        self.stream = None;
    }

    // -- internal -----------------------------------------------------------

    fn send_raw(&mut self, query: &str) -> Result<Response> {
        let stream = self.stream.as_mut().ok_or(Error::NotConnected)?;

        // Send query as a line.
        stream.write_all(query.as_bytes())?;
        stream.write_all(b"\n")?;
        stream.flush()?;

        // Read response line.
        let mut line = String::new();
        stream.read_line(&mut line)?;

        if line.is_empty() {
            return Err(Error::Server("connection closed by server".into()));
        }

        let resp: Response = serde_json::from_str(line.trim_end())?;
        Ok(resp)
    }
}

impl Drop for CommitDB {
    fn drop(&mut self) {
        self.close();
    }
}
