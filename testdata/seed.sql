INSERT INTO Users (UserID, Name, Email, Status, CreatedAt) VALUES
('user-001', 'Alice Johnson', 'alice@example.com', 1, CURRENT_TIMESTAMP()),
('user-002', 'Bob Smith', 'bob@example.com', 2, CURRENT_TIMESTAMP()),
('user-003', 'Charlie Brown', 'charlie@example.com', 1, CURRENT_TIMESTAMP());

INSERT INTO Products (ProductID, Name, Price, IsActive, CategoryID, CreatedAt) VALUES
('prod-001', 'Laptop Computer', 150000, true, 'cat-electronics', CURRENT_TIMESTAMP()),
('prod-002', 'Wireless Mouse', 3000, true, 'cat-electronics', CURRENT_TIMESTAMP()),
('prod-003', 'Coffee Mug', 1200, false, 'cat-kitchen', CURRENT_TIMESTAMP());

INSERT INTO Orders (OrderID, UserID, ProductID, Quantity, OrderDate) VALUES
('order-001', 'user-001', 'prod-001', 1, CURRENT_TIMESTAMP()),
('order-002', 'user-002', 'prod-002', 2, CURRENT_TIMESTAMP()),
('order-003', 'user-001', 'prod-003', 1, CURRENT_TIMESTAMP());

-- Test data for DataTypeTest table
INSERT INTO DataTypeTest (
    ID, StringCol, Int64Col, NumericCol, Float64Col, BoolCol, 
    BytesCol, TimestampCol, DateCol, ArrayStringCol, ArrayInt64Col, JSONCol
) VALUES
-- Row with all non-null values
('dtype-001', 'Hello World', 42, NUMERIC '123.456', 3.14159, true, 
 b'binary data', TIMESTAMP '2024-01-15T10:30:00Z', DATE '2024-01-15', 
 ['apple', 'banana', 'cherry'], [1, 2, 3], JSON '{"key": "value", "number": 123}'),
-- Row with some null values
('dtype-002', 'Test String', NULL, NUMERIC '999.999', 2.71828, false,
 NULL, TIMESTAMP '2024-02-20T15:45:00Z', DATE '2024-02-20',
 ['single'], [], JSON '{"empty": null}'),
-- Row with Unicode and special characters
('dtype-003', '日本語テスト 🚀', -9999, NUMERIC '-0.001', -1.0, true,
 b'\x00\xFF\xFE', TIMESTAMP '2024-03-25T23:59:59.999999Z', DATE '2024-03-25',
 ['multi', 'line\nstring', 'tab\there'], [-1, 0, 1], JSON '{"unicode": "こんにちは"}');

-- EmptyTable remains empty for testing

-- LargeTable will be populated dynamically in tests for performance testing