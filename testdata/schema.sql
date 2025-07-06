-- Test schema for integration tests

CREATE TABLE Users (
  UserID STRING(36) NOT NULL,
  Name STRING(100) NOT NULL,
  Email STRING(255) NOT NULL,
  Status INT64 NOT NULL,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID);

CREATE TABLE Products (
  ProductID STRING(36) NOT NULL,
  Name STRING(200) NOT NULL,
  Price INT64 NOT NULL,
  IsActive BOOL NOT NULL,
  CategoryID STRING(36),
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ProductID);

CREATE TABLE Orders (
  OrderID STRING(36) NOT NULL,
  UserID STRING(36) NOT NULL,
  ProductID STRING(36) NOT NULL,
  Quantity INT64 NOT NULL,
  OrderDate TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (UserID, ProductID),
INTERLEAVE IN PARENT Users ON DELETE CASCADE;