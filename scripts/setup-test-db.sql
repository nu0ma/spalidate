-- Sample SQL script to set up test database schema
-- This can be used for manual testing with Spanner emulator

-- Create Users table
CREATE TABLE Users (
    UserID STRING(36) NOT NULL,
    Name STRING(100),
    Email STRING(100),
    Status INT64,
) PRIMARY KEY (UserID);

-- Create Products table
CREATE TABLE Products (
    ProductID STRING(36) NOT NULL,
    Name STRING(100),
    Price INT64,
    IsActive BOOL,
) PRIMARY KEY (ProductID);

-- Create Orders table
CREATE TABLE Orders (
    OrderID STRING(36) NOT NULL,
    UserID STRING(36),
    Total FLOAT64,
    CreatedAt TIMESTAMP,
) PRIMARY KEY (OrderID);

-- Sample data insertions
INSERT INTO Users (UserID, Name, Email, Status) VALUES
    ('user-001', 'Test User', 'test@example.com', 1),
    ('user-002', 'Another User', 'another@example.com', 1);

INSERT INTO Products (ProductID, Name, Price, IsActive) VALUES
    ('prod-001', 'Test Product', 1000, true),
    ('prod-002', 'Another Product', 2000, false);

INSERT INTO Orders (OrderID, UserID, Total, CreatedAt) VALUES
    ('order-001', 'user-001', 1500.50, CURRENT_TIMESTAMP());