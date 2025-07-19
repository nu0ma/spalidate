-- Orders test data (INTERLEAVED with Users)
INSERT INTO Orders (OrderID, UserID, ProductID, Quantity, OrderDate) VALUES
('order-001', 'user-001', 'prod-001', 1, '2024-01-01T00:00:00Z'),
('order-002', 'user-002', 'prod-002', 2, '2024-01-01T00:00:00Z'),
('order-003', 'user-001', 'prod-003', 1, '2024-01-01T00:00:00Z');