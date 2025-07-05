#!/bin/bash
# Example script showing how to use spemu to seed data for spalidate validation

# Install spemu if not already installed
if ! command -v spemu &> /dev/null; then
    echo "Installing spemu..."
    go install github.com/nu0ma/spemu@latest
fi

# Start Spanner emulator if not running
docker-compose up -d spanner-emulator

# Wait for emulator to be ready
echo "Waiting for Spanner emulator..."
sleep 5

# Set environment variables
export SPANNER_EMULATOR_HOST=localhost:9010
export GOOGLE_CLOUD_PROJECT=test-project

# Create instance and database using spemu
echo "Creating Spanner instance and database..."
spemu create-instance --project=test-project --instance=test-instance
spemu create-database --project=test-project --instance=test-instance --database=test-database

# Create schema
echo "Creating schema..."
cat <<EOF > /tmp/schema.sql
CREATE TABLE users (
    id INT64 NOT NULL,
    name STRING(100),
    email STRING(100),
    active BOOL,
    score FLOAT64,
    created_at TIMESTAMP
) PRIMARY KEY (id);

CREATE TABLE products (
    id INT64 NOT NULL,
    name STRING(100),
    price FLOAT64,
    stock INT64
) PRIMARY KEY (id);
EOF

spemu execute-sql --project=test-project --instance=test-instance --database=test-database --file=/tmp/schema.sql

# Seed data
echo "Seeding data..."
cat <<EOF > /tmp/seed.sql
INSERT INTO users (id, name, email, active, score, created_at) VALUES
(1, 'Alice', 'alice@example.com', true, 95.5, CURRENT_TIMESTAMP()),
(2, 'Bob', 'bob@example.com', false, 87.3, CURRENT_TIMESTAMP());

INSERT INTO products (id, name, price, stock) VALUES
(1, 'Product A', 29.99, 100),
(2, 'Product B', 49.99, 50);
EOF

spemu execute-sql --project=test-project --instance=test-instance --database=test-database --file=/tmp/seed.sql

# Create validation config
echo "Creating validation config..."
cat <<EOF > /tmp/validation.yaml
tables:
  - name: users
    expected_count: 2
    expected_values:
      - column: name
        row: 0
        value: "Alice"
      - column: email
        row: 0
        value: "alice@example.com"
      - column: active
        row: 0
        value: true
  - name: products
    expected_count: 2
    expected_values:
      - column: name
        row: 0
        value: "Product A"
      - column: price
        row: 0
        value: 29.99
EOF

# Run spalidate validation
echo "Running spalidate validation..."
./spalidate --project=test-project --instance=test-instance --database=test-database /tmp/validation.yaml

# Cleanup
rm -f /tmp/schema.sql /tmp/seed.sql /tmp/validation.yaml