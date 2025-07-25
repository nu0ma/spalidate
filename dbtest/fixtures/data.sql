-- Clear all tables before inserting test data
DELETE FROM Orders WHERE true;
DELETE FROM Products WHERE true;
DELETE FROM Users WHERE true;
DELETE FROM json WHERE true;
DELETE FROM NullTypes WHERE true;
DELETE FROM ArrayTypes WHERE true;
DELETE FROM ComplexTypes WHERE true;

-- Users test data
INSERT INTO Users (UserID, Name, Email, Status, CreatedAt) VALUES
('user-001', 'Alice Johnson', 'alice@example.com', 1, '2024-01-01T00:00:00Z'),
('user-002', 'Bob Smith', 'bob@example.com', 2, '2024-01-01T00:00:00Z'),
('user-003', 'Charlie Brown', 'charlie@example.com', 1, '2024-01-01T00:00:00Z');

-- Products test data
INSERT INTO Products (ProductID, Name, Price, IsActive, CategoryID, CreatedAt) VALUES
('prod-001', 'Laptop Computer', 150000, true, 'cat-electronics', '2024-01-01T00:00:00Z'),
('prod-002', 'Wireless Mouse', 3000, true, 'cat-electronics', '2024-01-01T00:00:00Z'),
('prod-003', 'Coffee Mug', 1200, false, 'cat-kitchen', '2024-01-01T00:00:00Z');

-- Orders test data (INTERLEAVED with Users)
INSERT INTO Orders (OrderID, UserID, ProductID, Quantity, OrderDate) VALUES
('order-001', 'user-001', 'prod-001', 1, '2024-01-01T00:00:00Z'),
('order-002', 'user-002', 'prod-002', 2, '2024-01-01T00:00:00Z'),
('order-003', 'user-001', 'prod-003', 1, '2024-01-01T00:00:00Z');

-- JSON test data
INSERT INTO json (ID, Data, Metadata) VALUES
('json-001', '{"name": "John Doe", "age": 30, "address": {"city": "Tokyo", "country": "Japan"}}', '{"tags": ["admin", "developer"], "active": true, "score": 95.5}'),
('json-002', '{"name": "Jane Smith", "age": 25, "preferences": {"theme": "dark", "notifications": true}}', '{"tags": ["user"], "active": false, "score": 87}'),
('json-003', 'null', '{}'),
('json-004', '{"products": [{"id": 1, "name": "Laptop"}, {"id": 2, "name": "Mouse"}]}', '{"created_at": "2024-01-01T00:00:00Z", "version": 1.0}'),
('json-005', '{"empty_object": {}, "empty_array": [], "boolean_values": [true, false, null]}', '{"test": true}');

-- NullTypes test data
INSERT INTO NullTypes (ID, NullString, NullInt64, NullBool, NullFloat64, NullDate, NullTimestamp, NullNumeric, NullJson) VALUES
('null-001', 'test string', 42, true, 3.14159, '2024-01-15', '2024-01-15T10:30:00Z', 123.456, JSON '{"key": "value"}'),
('null-002', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
('null-003', 'another string', 100, false, 2.71828, '2024-12-31', '2024-12-31T23:59:59Z', 999.999, JSON '{"active": true, "count": 0}'),
('null-004', '', 0, true, 0.0, '1970-01-01', '1970-01-01T00:00:00Z', 0, NULL);

-- ArrayTypes test data
INSERT INTO ArrayTypes (ID, StringArray, Int64Array, BoolArray, NullStringArray) VALUES
('array-001', ['hello', 'world', 'test'], [1, 2, 3, 4, 5], [true, false, true], ['first', 'second', 'third']),
('array-002', [], [], [], []),
('array-003', ['single'], [42], [false], ['only one']),
('array-004', ['a', 'b', 'c', 'd'], [10, 20, 30], [true, true, false, true], ['mixed', 'content', 'here']);

-- ComplexTypes test data
INSERT INTO ComplexTypes (ID, BytesData, NumericData, DateData, JsonData) VALUES
('complex-001', B'dGVzdCBieXRlcyBkYXRh', 123.456789, '2024-01-01', JSON '{"type": "test", "value": 42}'),
('complex-002', B'', 0, '1970-01-01', JSON '{}'),
('complex-003', B'YW5vdGhlciB0ZXN0', 999999.123456, '2024-12-31', JSON '{"array": [1, 2, 3], "nested": {"key": "value"}}'),
('complex-004', B'c3BlY2lhbCBjaGFyYWN0ZXJz', -123.456, '2000-02-29', JSON '{"boolean": true, "null_value": null, "number": 3.14}');