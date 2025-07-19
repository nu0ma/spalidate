-- Clear all tables before inserting test data
DELETE FROM Orders WHERE true;
DELETE FROM Products WHERE true;
DELETE FROM Users WHERE true;
DELETE FROM json WHERE true;

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