//! Integration tests for CommitDB Rust client.
//!
//! These tests require a running CommitDB server on localhost:3306.
//! They are gated behind `#[cfg(feature = "integration")]` so they
//! don't run during regular `cargo test`.
//!
//! Run with:
//!   cargo test --features integration
//!
//! Or via CI, which starts a server automatically.

#![cfg(feature = "integration")]

use commitdb::{CommitDB, ExecuteResult};

fn connect() -> CommitDB {
    let host = std::env::var("COMMITDB_HOST").unwrap_or_else(|_| "localhost".into());
    let port: u16 = std::env::var("COMMITDB_PORT")
        .unwrap_or_else(|_| "3306".into())
        .parse()
        .unwrap();
    CommitDB::connect(&host, port).expect("failed to connect to CommitDB server")
}

// ---------------------------------------------------------------------------
// Database & Table operations
// ---------------------------------------------------------------------------

#[test]
fn test_create_and_drop_database() {
    let mut db = connect();
    let result = db.execute("CREATE DATABASE rust_test_db1").unwrap();
    if let ExecuteResult::Commit(c) = &result {
        assert_eq!(c.databases_created, 1);
    } else {
        panic!("expected commit result");
    }
    db.execute("DROP DATABASE rust_test_db1").unwrap();
}

#[test]
fn test_create_table_and_describe() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db2").unwrap();
    let result = db.execute("CREATE TABLE rust_test_db2.users (id INT PRIMARY KEY, name STRING)").unwrap();
    if let ExecuteResult::Commit(c) = &result {
        assert_eq!(c.tables_created, 1);
    } else {
        panic!("expected commit result");
    }

    let desc = db.query("DESCRIBE rust_test_db2.users").unwrap();
    assert!(desc.len() >= 2);

    db.execute("DROP DATABASE rust_test_db2").unwrap();
}

// ---------------------------------------------------------------------------
// CRUD operations
// ---------------------------------------------------------------------------

#[test]
fn test_insert_and_query() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db3").unwrap();
    db.execute("CREATE TABLE rust_test_db3.items (id INT PRIMARY KEY, value STRING)").unwrap();

    db.execute("INSERT INTO rust_test_db3.items (id, value) VALUES (1, 'hello')").unwrap();
    db.execute("INSERT INTO rust_test_db3.items (id, value) VALUES (2, 'world')").unwrap();

    let result = db.query("SELECT * FROM rust_test_db3.items").unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(result.columns, vec!["id", "value"]);

    let row = result.row(0).unwrap();
    assert_eq!(row[0].1, "1");
    assert_eq!(row[1].1, "hello");

    db.execute("DROP DATABASE rust_test_db3").unwrap();
}

#[test]
fn test_update() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db4").unwrap();
    db.execute("CREATE TABLE rust_test_db4.data (id INT PRIMARY KEY, val STRING)").unwrap();
    db.execute("INSERT INTO rust_test_db4.data (id, val) VALUES (1, 'old')").unwrap();

    db.execute("UPDATE rust_test_db4.data SET val = 'new' WHERE id = 1").unwrap();

    let result = db.query("SELECT * FROM rust_test_db4.data WHERE id = 1").unwrap();
    assert_eq!(result.len(), 1);
    let row = result.row(0).unwrap();
    assert!(row.iter().any(|(k, v)| *k == "val" && *v == "new"));

    db.execute("DROP DATABASE rust_test_db4").unwrap();
}

#[test]
fn test_delete() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db5").unwrap();
    db.execute("CREATE TABLE rust_test_db5.data (id INT PRIMARY KEY)").unwrap();
    db.execute("INSERT INTO rust_test_db5.data (id) VALUES (1)").unwrap();
    db.execute("INSERT INTO rust_test_db5.data (id) VALUES (2)").unwrap();

    db.execute("DELETE FROM rust_test_db5.data WHERE id = 1").unwrap();

    let result = db.query("SELECT * FROM rust_test_db5.data").unwrap();
    assert_eq!(result.len(), 1);

    db.execute("DROP DATABASE rust_test_db5").unwrap();
}

#[test]
fn test_bulk_insert() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db6").unwrap();
    db.execute("CREATE TABLE rust_test_db6.items (id INT PRIMARY KEY, name STRING)").unwrap();

    let result = db.execute(
        "INSERT INTO rust_test_db6.items (id, name) VALUES (1, 'A'), (2, 'B'), (3, 'C')"
    ).unwrap();
    if let ExecuteResult::Commit(c) = &result {
        assert_eq!(c.records_written, 3);
    }

    let result = db.query("SELECT * FROM rust_test_db6.items").unwrap();
    assert_eq!(result.len(), 3);

    db.execute("DROP DATABASE rust_test_db6").unwrap();
}

// ---------------------------------------------------------------------------
// SHOW commands
// ---------------------------------------------------------------------------

#[test]
fn test_show_databases() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db7").unwrap();

    let result = db.query("SHOW DATABASES").unwrap();
    let dbs: Vec<&str> = result.data.iter().map(|r| r[0].as_str()).collect();
    assert!(dbs.contains(&"rust_test_db7"));

    db.execute("DROP DATABASE rust_test_db7").unwrap();
}

#[test]
fn test_show_tables() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db8").unwrap();
    db.execute("CREATE TABLE rust_test_db8.users (id INT PRIMARY KEY)").unwrap();
    db.execute("CREATE TABLE rust_test_db8.orders (id INT PRIMARY KEY)").unwrap();

    let result = db.query("SHOW TABLES IN rust_test_db8").unwrap();
    assert_eq!(result.len(), 2);

    db.execute("DROP DATABASE rust_test_db8").unwrap();
}

// ---------------------------------------------------------------------------
// Branching & Merging
// ---------------------------------------------------------------------------

#[test]
fn test_branch_checkout_merge() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db9").unwrap();
    db.execute("CREATE TABLE rust_test_db9.data (id INT PRIMARY KEY, val STRING)").unwrap();
    db.execute("INSERT INTO rust_test_db9.data (id, val) VALUES (1, 'original')").unwrap();

    // Create and checkout branch
    db.execute("CREATE BRANCH rust_feature").unwrap();
    db.execute("CHECKOUT rust_feature").unwrap();

    // Add data on branch
    db.execute("INSERT INTO rust_test_db9.data (id, val) VALUES (2, 'branch')").unwrap();
    let result = db.query("SELECT * FROM rust_test_db9.data").unwrap();
    assert_eq!(result.len(), 2);

    // Switch back — should only have 1 row
    db.execute("CHECKOUT master").unwrap();
    let result = db.query("SELECT * FROM rust_test_db9.data").unwrap();
    assert_eq!(result.len(), 1);

    // Merge
    db.execute("MERGE rust_feature").unwrap();
    let result = db.query("SELECT * FROM rust_test_db9.data").unwrap();
    assert_eq!(result.len(), 2);

    db.execute("DROP DATABASE rust_test_db9").unwrap();
}

#[test]
fn test_show_branches() {
    let mut db = connect();
    db.execute("CREATE BRANCH rust_show_test").unwrap();

    let result = db.query("SHOW BRANCHES").unwrap();
    assert!(result.len() >= 2); // master + rust_show_test
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

#[test]
fn test_query_nonexistent_table() {
    let mut db = connect();
    let result = db.query("SELECT * FROM nonexistent.table");
    assert!(result.is_err());
}

#[test]
fn test_invalid_sql() {
    let mut db = connect();
    let result = db.execute("INVALID SQL STATEMENT");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Iterator
// ---------------------------------------------------------------------------

#[test]
fn test_query_result_iterator() {
    let mut db = connect();
    db.execute("CREATE DATABASE rust_test_db10").unwrap();
    db.execute("CREATE TABLE rust_test_db10.items (id INT PRIMARY KEY, name STRING)").unwrap();
    db.execute("INSERT INTO rust_test_db10.items (id, name) VALUES (1, 'Alice')").unwrap();
    db.execute("INSERT INTO rust_test_db10.items (id, name) VALUES (2, 'Bob')").unwrap();

    let result = db.query("SELECT * FROM rust_test_db10.items").unwrap();

    let mut count = 0;
    for row in &result {
        assert_eq!(row.len(), 2);
        count += 1;
    }
    assert_eq!(count, 2);

    db.execute("DROP DATABASE rust_test_db10").unwrap();
}
