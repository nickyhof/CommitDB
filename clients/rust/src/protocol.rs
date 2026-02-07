//! Wire protocol types matching the CommitDB server's JSON format.
//!
//! These are internal serde structs — the public API uses the types in `lib.rs`.

use serde::{Deserialize, Serialize};

/// A SQL query sent to the server (newline-delimited JSON).
#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub(crate) struct Request {
    pub query: String,
}

/// Top-level server response envelope.
#[derive(Debug, Deserialize)]
pub(crate) struct Response {
    pub success: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default, rename = "type")]
    pub response_type: Option<String>,
    #[serde(default)]
    pub result: Option<serde_json::Value>,
}

/// Tabular query results (SELECT, SHOW, DESCRIBE, etc.).
#[derive(Debug, Deserialize)]
pub(crate) struct QueryResponseData {
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub data: Vec<Vec<String>>,
    #[serde(default)]
    pub records_read: i64,
    #[serde(default)]
    pub execution_time_ms: f64,
    #[serde(default)]
    pub execution_ops: i64,
}

/// Mutation results (INSERT, UPDATE, DELETE, CREATE, DROP, etc.).
#[derive(Debug, Deserialize)]
pub(crate) struct CommitResponseData {
    #[serde(default)]
    pub databases_created: i64,
    #[serde(default)]
    pub databases_deleted: i64,
    #[serde(default)]
    pub tables_created: i64,
    #[serde(default)]
    pub tables_deleted: i64,
    #[serde(default)]
    pub records_written: i64,
    #[serde(default)]
    pub records_deleted: i64,
    #[serde(default)]
    pub execution_time_ms: f64,
    #[serde(default)]
    pub execution_ops: i64,
}

/// Authentication result.
#[derive(Debug, Deserialize)]
pub(crate) struct AuthResponseData {
    #[serde(default)]
    pub authenticated: bool,
    #[serde(default)]
    pub identity: Option<String>,
    #[serde(default)]
    pub expires_in: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_query_response() {
        let json = r#"{
            "success": true,
            "type": "query",
            "result": {
                "columns": ["id", "name"],
                "data": [["1", "Alice"], ["2", "Bob"]],
                "records_read": 2,
                "execution_time_ms": 1.5,
                "execution_ops": 1
            }
        }"#;

        let resp: Response = serde_json::from_str(json).unwrap();
        assert!(resp.success);
        assert_eq!(resp.response_type.as_deref(), Some("query"));

        let result: QueryResponseData =
            serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(result.columns, vec!["id", "name"]);
        assert_eq!(result.data.len(), 2);
        assert_eq!(result.records_read, 2);
    }

    #[test]
    fn test_deserialize_commit_response() {
        let json = r#"{
            "success": true,
            "type": "commit",
            "result": {
                "databases_created": 1,
                "execution_time_ms": 0.8,
                "execution_ops": 1
            }
        }"#;

        let resp: Response = serde_json::from_str(json).unwrap();
        assert!(resp.success);
        assert_eq!(resp.response_type.as_deref(), Some("commit"));

        let result: CommitResponseData =
            serde_json::from_value(resp.result.unwrap()).unwrap();
        assert_eq!(result.databases_created, 1);
        assert_eq!(result.tables_created, 0); // default
    }

    #[test]
    fn test_deserialize_error_response() {
        let json = r#"{"success": false, "error": "table not found"}"#;

        let resp: Response = serde_json::from_str(json).unwrap();
        assert!(!resp.success);
        assert_eq!(resp.error.as_deref(), Some("table not found"));
    }

    #[test]
    fn test_deserialize_auth_response() {
        let json = r#"{
            "success": true,
            "type": "auth",
            "result": {
                "authenticated": true,
                "identity": "Alice <alice@example.com>",
                "expires_in": 3600
            }
        }"#;

        let resp: Response = serde_json::from_str(json).unwrap();
        let result: AuthResponseData =
            serde_json::from_value(resp.result.unwrap()).unwrap();
        assert!(result.authenticated);
        assert_eq!(result.identity.as_deref(), Some("Alice <alice@example.com>"));
        assert_eq!(result.expires_in, Some(3600));
    }

    #[test]
    fn test_serialize_request() {
        let req = Request {
            query: "SELECT * FROM mydb.users".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("SELECT * FROM mydb.users"));
    }
}
