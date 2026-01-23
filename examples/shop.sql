-- Example SQL file for CommitDB
-- This file demonstrates the SQL import feature

-- Create a sample database
CREATE DATABASE shop;

-- Create tables
CREATE TABLE shop.products (
    id INT PRIMARY KEY,
    name STRING,
    price FLOAT,
    category STRING
);

CREATE TABLE shop.customers (
    id INT PRIMARY KEY,
    name STRING,
    email STRING
);

CREATE TABLE shop.orders (
    id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity INT
);

-- Insert sample products
INSERT INTO shop.products (id, name, price, category) VALUES (1, 'Laptop', 999, 'Electronics');
INSERT INTO shop.products (id, name, price, category) VALUES (2, 'Mouse', 29, 'Electronics');
INSERT INTO shop.products (id, name, price, category) VALUES (3, 'Keyboard', 79, 'Electronics');
INSERT INTO shop.products (id, name, price, category) VALUES (4, 'Desk', 299, 'Furniture');
INSERT INTO shop.products (id, name, price, category) VALUES (5, 'Chair', 199, 'Furniture');

-- Insert sample customers
INSERT INTO shop.customers (id, name, email) VALUES (1, 'Alice Smith', 'alice@example.com');
INSERT INTO shop.customers (id, name, email) VALUES (2, 'Bob Johnson', 'bob@example.com');
INSERT INTO shop.customers (id, name, email) VALUES (3, 'Charlie Brown', 'charlie@example.com');

-- Insert sample orders
INSERT INTO shop.orders (id, customer_id, product_id, quantity) VALUES (1, 1, 1, 1);
INSERT INTO shop.orders (id, customer_id, product_id, quantity) VALUES (2, 1, 2, 2);
INSERT INTO shop.orders (id, customer_id, product_id, quantity) VALUES (3, 2, 3, 1);
INSERT INTO shop.orders (id, customer_id, product_id, quantity) VALUES (4, 3, 4, 1);
INSERT INTO shop.orders (id, customer_id, product_id, quantity) VALUES (5, 3, 5, 2);
