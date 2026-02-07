"""
CommitDB Python Client

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

from importlib.metadata import version, PackageNotFoundError

from .client import CommitDB, QueryResult, CommitResult, CommitDBError

try:
    __version__ = version("commitdb")
except PackageNotFoundError:
    __version__ = "0.0.0"  # Fallback for development/editable installs

__all__ = ['CommitDB', 'QueryResult', 'CommitResult', 'CommitDBError', '__version__']
