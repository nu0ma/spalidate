-- Test data for integration tests
-- This data will be used to validate the spalidate tool

-- Insert test users (ordered by UserID for consistent testing)
INSERT INTO Users (UserID, Name, Email, Status, CreatedAt) VALUES
('user-001', 'Alice Johnson', 'alice@example.com', 1, CURRENT_TIMESTAMP()),
('user-002', 'Bob Smith', 'bob@example.com', 2, CURRENT_TIMESTAMP()),
('user-003', 'Charlie Brown', 'charlie@example.com', 1, CURRENT_TIMESTAMP());

-- Insert test products (ordered by ProductID for consistent testing)
INSERT INTO Products (ProductID, Name, Price, IsActive, CategoryID, CreatedAt) VALUES
('prod-001', 'Laptop Computer', 150000, true, 'cat-electronics', CURRENT_TIMESTAMP()),
('prod-002', 'Wireless Mouse', 3000, true, 'cat-electronics', CURRENT_TIMESTAMP()),
('prod-003', 'Coffee Mug', 1200, false, 'cat-kitchen', CURRENT_TIMESTAMP());

-- Insert test orders (ordered by OrderID for consistent testing)
INSERT INTO Orders (OrderID, UserID, ProductID, Quantity, OrderDate) VALUES
('order-001', 'user-001', 'prod-001', 1, CURRENT_TIMESTAMP()),
('order-002', 'user-002', 'prod-002', 2, CURRENT_TIMESTAMP()),
('order-003', 'user-001', 'prod-003', 1, CURRENT_TIMESTAMP());