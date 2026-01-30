package sql

import (
	"reflect"
	"testing"

	"github.com/nickyhof/CommitDB/core"
)

func TestParser(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected Statement
	}{
		{
			"select wildcard",
			"SELECT * FROM db.test",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
			},
		},
		{
			"select columns",
			"SELECT col_1, col_2 FROM db.test",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col_1", "col_2"},
			},
		},
		{
			"select with where int",
			"SELECT col_1, col_2 FROM db.test WHERE col_1 = 10",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col_1", "col_2"},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col_1", Operator: EqualsOperator, Right: "10"}}},
			},
		},
		{
			"select with where string",
			"SELECT col_1, col_2 FROM db.test WHERE col_2 = 'green'",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col_1", "col_2"},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col_2", Operator: EqualsOperator, Right: "green"}}},
			},
		},
		{
			"select with where string and int",
			"SELECT col_1, col_2 FROM db.test WHERE col_1 = 'green' AND col_2 = 5",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col_1", "col_2"},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col_1", Operator: EqualsOperator, Right: "green"}, {Left: "col_2", Operator: EqualsOperator, Right: "5"}}, LogicalOps: []LogicalOperator{LogicalAnd}},
			},
		},
		{
			"select with limit",
			"SELECT col_1, col_2 FROM db.test WHERE col_1 = 'green' AND col_2 = 5 LIMIT 10",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col_1", "col_2"},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col_1", Operator: EqualsOperator, Right: "green"}, {Left: "col_2", Operator: EqualsOperator, Right: "5"}}, LogicalOps: []LogicalOperator{LogicalAnd}},
				Limit:    10,
			},
		},
		{
			"create table",
			"CREATE TABLE db.test (col_1 STRING, col_2 INT)",
			CreateTableStatement{
				Database: "db",
				Table:    "test",
				Columns: []core.Column{
					{Name: "col_1", Type: core.StringType},
					{Name: "col_2", Type: core.IntType},
				},
			},
		},
		{
			"create table with primary key",
			"CREATE TABLE db.test (col_1 STRING PRIMARY KEY, col_2 INT)",
			CreateTableStatement{
				Database: "db",
				Table:    "test",
				Columns: []core.Column{
					{Name: "col_1", Type: core.StringType, PrimaryKey: true},
					{Name: "col_2", Type: core.IntType},
				},
			},
		},
		{
			"drop table",
			"DROP TABLE db.test",
			DropTableStatement{
				Database: "db",
				Table:    "test",
			},
		},
		{
			"insert table",
			"INSERT INTO db.test (col_1, col_2) VALUES ('value', 1)",
			InsertStatement{
				Database:  "db",
				Table:     "test",
				Columns:   []string{"col_1", "col_2"},
				ValueRows: [][]string{{"value", "1"}},
			},
		},
		{
			"update table",
			"UPDATE db.test SET col_1 = 'value' WHERE col_2 = 5",
			UpdateStatement{
				Database: "db",
				Table:    "test",
				Updates: []SetClause{
					{Column: "col_1", Value: "value"},
				},
				Where: WhereClause{Conditions: []WhereCondition{{Left: "col_2", Operator: EqualsOperator, Right: "5"}}},
			},
		},
		{
			"delete table",
			"DELETE FROM db.test WHERE col_1 = 'value 123'",
			DeleteStatement{
				Database: "db",
				Table:    "test",
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col_1", Operator: EqualsOperator, Right: "value 123"}}},
			},
		},
		{
			"create database",
			"CREATE DATABASE test",
			CreateDatabaseStatement{
				Database: "test",
			},
		},
		{
			"drop database",
			"DROP DATABASE test",
			DropDatabaseStatement{
				Database: "test",
			},
		},
		{
			"show databases",
			"SHOW DATABASES",
			ShowDatabasesStatement{},
		},
		{
			"show tables in database",
			"SHOW TABLES IN test",
			ShowTablesStatement{Database: "test"},
		},
		// New tests for additional features
		{
			"select with not equals",
			"SELECT * FROM db.test WHERE col != 5",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: NotEqualsOperator, Right: "5"}}},
			},
		},
		{
			"select with less than",
			"SELECT * FROM db.test WHERE col < 10",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: LessThanOperator, Right: "10"}}},
			},
		},
		{
			"select with greater than",
			"SELECT * FROM db.test WHERE col > 10",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: GreaterThanOperator, Right: "10"}}},
			},
		},
		{
			"select with less than or equal",
			"SELECT * FROM db.test WHERE col <= 10",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: LessThanOrEqualOperator, Right: "10"}}},
			},
		},
		{
			"select with greater than or equal",
			"SELECT * FROM db.test WHERE col >= 10",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: GreaterThanOrEqualOperator, Right: "10"}}},
			},
		},
		{
			"select with or condition",
			"SELECT * FROM db.test WHERE col = 1 OR col = 2",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: EqualsOperator, Right: "1"}, {Left: "col", Operator: EqualsOperator, Right: "2"}}, LogicalOps: []LogicalOperator{LogicalOr}},
			},
		},
		{
			"select with is null",
			"SELECT * FROM db.test WHERE col IS NULL",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: IsNullOperator, Right: ""}}},
			},
		},
		{
			"select with is not null",
			"SELECT * FROM db.test WHERE col IS NOT NULL",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: IsNotNullOperator, Right: ""}}},
			},
		},
		{
			"select with like",
			"SELECT * FROM db.test WHERE name LIKE '%john%'",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "name", Operator: LikeOperator, Right: "%john%"}}},
			},
		},
		{
			"select with order by",
			"SELECT * FROM db.test ORDER BY col",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				OrderBy:  []OrderByClause{{Column: "col", Descending: false}},
			},
		},
		{
			"select with order by desc",
			"SELECT * FROM db.test ORDER BY col DESC",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				OrderBy:  []OrderByClause{{Column: "col", Descending: true}},
			},
		},
		{
			"select with order by multiple columns",
			"SELECT * FROM db.test ORDER BY col1 ASC, col2 DESC",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				OrderBy:  []OrderByClause{{Column: "col1", Descending: false}, {Column: "col2", Descending: true}},
			},
		},
		{
			"select with limit and offset",
			"SELECT * FROM db.test LIMIT 10 OFFSET 5",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Limit:    10,
				Offset:   5,
			},
		},
		{
			"select distinct",
			"SELECT DISTINCT col FROM db.test",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col"},
				Distinct: true,
			},
		},
		{
			"select count star",
			"SELECT COUNT(*) FROM db.test",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				CountAll: true,
			},
		},
		{
			"select with not condition",
			"SELECT * FROM db.test WHERE NOT col = 5",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{},
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col", Operator: EqualsOperator, Right: "5", Negated: true}}},
			},
		},
		{
			"complex select with all features",
			"SELECT DISTINCT col1, col2 FROM db.test WHERE col1 > 5 AND col2 IS NOT NULL ORDER BY col1 DESC LIMIT 10 OFFSET 20",
			SelectStatement{
				Database: "db",
				Table:    "test",
				Columns:  []string{"col1", "col2"},
				Distinct: true,
				Where:    WhereClause{Conditions: []WhereCondition{{Left: "col1", Operator: GreaterThanOperator, Right: "5"}, {Left: "col2", Operator: IsNotNullOperator, Right: ""}}, LogicalOps: []LogicalOperator{LogicalAnd}},
				OrderBy:  []OrderByClause{{Column: "col1", Descending: true}},
				Limit:    10,
				Offset:   20,
			},
		},
		{
			"select column and count star",
			"SELECT city, COUNT(*) FROM db.test",
			SelectStatement{
				Database:   "db",
				Table:      "test",
				Columns:    []string{"city"},
				Aggregates: []AggregateExpr{{Function: "COUNT", Column: "*"}},
			},
		},
		{
			"select column and multiple aggregates",
			"SELECT name, SUM(amount), AVG(price) FROM db.test",
			SelectStatement{
				Database:   "db",
				Table:      "test",
				Columns:    []string{"name"},
				Aggregates: []AggregateExpr{{Function: "SUM", Column: "amount"}, {Function: "AVG", Column: "price"}},
			},
		},
		{
			"select columns and aggregates with group by",
			"SELECT city, COUNT(*) FROM db.test GROUP BY city",
			SelectStatement{
				Database:   "db",
				Table:      "test",
				Columns:    []string{"city"},
				Aggregates: []AggregateExpr{{Function: "COUNT", Column: "*"}},
				GroupBy:    []string{"city"},
			},
		},
		// View tests
		{
			"create view",
			"CREATE VIEW db.active_users AS SELECT * FROM db.users WHERE active = 1",
			CreateViewStatement{
				Database:     "db",
				ViewName:     "active_users",
				SelectQuery:  "SELECT * FROM db.users WHERE active = 1",
				Materialized: false,
			},
		},
		{
			"create materialized view",
			"CREATE MATERIALIZED VIEW db.user_stats AS SELECT city, COUNT(*) FROM db.users GROUP BY city",
			CreateViewStatement{
				Database:     "db",
				ViewName:     "user_stats",
				SelectQuery:  "SELECT city , COUNT ( * ) FROM db.users GROUP BY city",
				Materialized: true,
			},
		},
		{
			"drop view",
			"DROP VIEW db.my_view",
			DropViewStatement{
				Database: "db",
				ViewName: "my_view",
				IfExists: false,
			},
		},
		{
			"drop view if exists",
			"DROP VIEW IF EXISTS db.my_view",
			DropViewStatement{
				Database: "db",
				ViewName: "my_view",
				IfExists: true,
			},
		},
		{
			"show views",
			"SHOW VIEWS IN mydb",
			ShowViewsStatement{
				Database: "mydb",
			},
		},
		{
			"refresh view",
			"REFRESH VIEW db.cached_data",
			RefreshViewStatement{
				Database: "db",
				ViewName: "cached_data",
			},
		},
		{
			"select with as of",
			"SELECT * FROM db.users AS OF 'abc1234'",
			SelectStatement{
				Database: "db",
				Table:    "users",
				Columns:  []string{},
				AsOf:     "abc1234",
			},
		},
		{
			"select columns with as of",
			"SELECT name, age FROM db.users AS OF 'def5678' WHERE age > 30",
			SelectStatement{
				Database: "db",
				Table:    "users",
				Columns:  []string{"name", "age"},
				AsOf:     "def5678",
				Where: WhereClause{
					Conditions: []WhereCondition{
						{Left: "age", Operator: GreaterThanOperator, Right: "30"},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := parse(test.sql)

			if err != nil {
				t.Errorf("Test Failed: Unexpected error: %v", err)
				return
			}

			if !reflect.DeepEqual(actual, test.expected) {
				t.Errorf("Test Failed: Expected %+v, got %+v", test.expected, actual)
			}
		})
	}
}
