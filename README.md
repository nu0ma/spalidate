# Spalidate

[![Tests](https://github.com/nu0ma/spalidate/actions/workflows/test.yml/badge.svg)](https://github.com/nu0ma/spalidate/actions/workflows/test.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/nu0ma/spalidate)](https://goreportcard.com/report/github.com/nu0ma/spalidate)
[![GoDoc](https://pkg.go.dev/badge/github.com/nu0ma/spalidate)](https://pkg.go.dev/github.com/nu0ma/spalidate)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Validate Google Cloud Spanner data (emulator supported) against expectations written in YAML. Simple CLI, fast feedback.

## Install

```bash
go install github.com/nu0ma/spalidate@latest
```

## Quick Start

1) Start the Spanner emulator and set `SPANNER_EMULATOR_HOST`.

2) Create expected YAML (example):

```yaml
# validation.yaml
tables:
  Users:
    columns:
      - UserID: "user-001"
        Name: "Alice Johnson"
        Email: "alice@example.com"
        Status: 1
        CreatedAt: "2024-01-01T00:00:00Z"
  Products:
    columns:
      - ProductID: "prod-001"
        Name: "Laptop Computer"
        Price: 150000
        IsActive: true
        CategoryID: "cat-electronics"
        CreatedAt: "2024-01-01T00:00:00Z"
  Books:
    columns:
      - BookID: "book-001"
        Title: "The Great Gatsby"
        Author: "F. Scott Fitzgerald"
        PublishedYear: 1925
        JSONData: '{"genre": "Fiction", "rating": 4.5}'
```

3) Run

```bash
spalidate --project <your-project> --instance <your-instance> --database <your-database> ./validation.yaml
```
On success: `Validation passed for all tables`


### If not successful:

```bash
2025/09/13 19:09:25 ERRO ✖️ table Books: expected row does not match
                column mismatch: 1
              1)  column: JSONData
                 ▸ expected: {"genre":"Fiction","ratifeawfng":4.5}
                 ▸   actual: {"genre": "invalid", "rating": 4.5}

            2025/09/13 19:09:25 ERRO ✖️ table Products: expected row does not match
                column mismatch: 1

              1)  column: CategoryID
                 ▸ expected: cat-electronieeecs
                 ▸   actual: cat-electronics

            2025/09/13 19:09:25 ERRO ✖️ table Users: expected row does not match
                column mismatch: 2

              1)  column: Email
                 ▸ expected: alice@example.comfeawfa
                 ▸   actual: alice@example.com

              2)  column: Status
                 ▸ expected: 999
                 ▸   actual: 1
```

You will see logs like the ones shown above.

## License

MIT
