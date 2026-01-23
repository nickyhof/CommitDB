"""
CommitDB Python Driver

A Python client for connecting to CommitDB SQL Server.

Usage:
    from commitdb import CommitDB

    db = CommitDB('localhost', 3306)
    db.connect()

    # Create database and table
    db.execute('CREATE DATABASE mydb')
    db.execute('CREATE TABLE mydb.users (id INT PRIMARY KEY, name STRING)')

    # Insert data
    db.execute("INSERT INTO mydb.users (id, name) VALUES (1, 'Alice')")

    # Query data
    result = db.query('SELECT * FROM mydb.users')
    for row in result:
        print(row)

    db.close()
"""

from .client import CommitDB, QueryResult, CommitResult, CommitDBError

__version__ = '0.1.0'
__all__ = ['CommitDB', 'QueryResult', 'CommitResult', 'CommitDBError']
