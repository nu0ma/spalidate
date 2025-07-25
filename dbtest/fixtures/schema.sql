-- Database schema for spalidate integration tests

CREATE TABLE Users (
    UserID STRING(36) NOT NULL,
    Name STRING(100) NOT NULL,
    Email STRING(255) NOT NULL,
    Status INT64 NOT NULL,
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (UserID);

CREATE TABLE Products (
    ProductID STRING(36) NOT NULL,
    Name STRING(200) NOT NULL,
    Price INT64 NOT NULL,
    IsActive BOOL NOT NULL,
    CategoryID STRING(36),
    CreatedAt TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (ProductID);

CREATE TABLE Orders (
    OrderID STRING(36) NOT NULL,
    UserID STRING(36) NOT NULL,
    ProductID STRING(36) NOT NULL,
    Quantity INT64 NOT NULL,
    OrderDate TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true)
) PRIMARY KEY (UserID, ProductID), INTERLEAVE IN PARENT Users ON DELETE CASCADE;

CREATE TABLE json (
    ID STRING(36) NOT NULL,
    Data STRING(MAX),
    Metadata STRING(MAX)
) PRIMARY KEY (ID);

CREATE TABLE NullTypes (
    ID STRING(36) NOT NULL,
    NullString STRING(100),
    NullInt64 INT64,
    NullBool BOOL,
    NullFloat64 FLOAT64,
    NullDate DATE,
    NullTimestamp TIMESTAMP,
    NullNumeric NUMERIC,
    NullJson JSON
) PRIMARY KEY (ID);

CREATE TABLE ArrayTypes (
    ID STRING(36) NOT NULL,
    StringArray ARRAY<STRING(50)>,
    Int64Array ARRAY<INT64>,
    BoolArray ARRAY<BOOL>,
    NullStringArray ARRAY<STRING(50)>
) PRIMARY KEY (ID);

CREATE TABLE ComplexTypes (
    ID STRING(36) NOT NULL,
    BytesData BYTES(MAX),
    NumericData NUMERIC,
    DateData DATE,
    JsonData JSON
) PRIMARY KEY (ID);