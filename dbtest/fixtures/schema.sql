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

CREATE TABLE Books (
		BookID STRING(36) NOT NULL,
		Title STRING(200) NOT NULL,
		Author STRING(100) NOT NULL,
		PublishedYear INT64 NOT NULL,
		JSONData STRING(MAX)
) PRIMARY KEY (BookID)