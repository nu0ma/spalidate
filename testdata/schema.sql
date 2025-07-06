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
) PRIMARY KEY (UserID, OrderID),
INTERLEAVE IN PARENT Users ON DELETE CASCADE;

-- Table for testing all Spanner data types
CREATE TABLE DataTypeTest (
  ID STRING(36) NOT NULL,
  StringCol STRING(MAX),
  Int64Col INT64,
  NumericCol NUMERIC,
  Float64Col FLOAT64,
  BoolCol BOOL,
  BytesCol BYTES(MAX),
  TimestampCol TIMESTAMP,
  DateCol DATE,
  ArrayStringCol ARRAY<STRING(MAX)>,
  ArrayInt64Col ARRAY<INT64>,
  JSONCol JSON,
) PRIMARY KEY (ID);

-- Table for performance testing with large datasets
CREATE TABLE LargeTable (
  ID INT64 NOT NULL,
  Data STRING(1000),
  Category STRING(50),
  Value NUMERIC,
  CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY (ID);

-- Table for testing empty table scenarios
CREATE TABLE EmptyTable (
  ID STRING(36) NOT NULL,
  Name STRING(100),
  Value INT64,
) PRIMARY KEY (ID);