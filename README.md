# Spalidate

Validate Google Cloud Spanner data (emulator supported) against expectations written in YAML. Simple CLI, fast feedback.

## Install

```bash
go install github.com/nu0ma/spalidate@latest
```

## Quick Start

1) Start the Spanner emulator and set `SPANNER_EMULATOR_HOST`.

2) Create a minimal YAML (example):

```yaml
tables:
  Users:
    count: 1
    columns:
      - UserID: "user-001"
        Name: "Test User"
```

3) Run

```bash
spalidate \
  --project proj --instance inst --database db \
  ./validation.yaml
```

On success: `Validation passed for all tables`

## Flags

- `-p, --project` (required): Project ID
- `-i, --instance` (required): Instance ID
- `-d, --database` (required): Database ID
- `--port` (default 9010): Emulator port
- `-v, --verbose`: Verbose logs (same as `--log-level=debug`)
- `--log-level` (`debug|info|warn|error`, default `info`)
- `--log-format` (`console|json`, default `console`)
- `--color` (`auto|always|never`, default `auto`)
- `--version`: Show version

Tip: in non-TTY contexts (CI or `go test`) force color with
`SPALIDATE_COLOR=always` or `--color=always`.

## Logging

- Uses Charmbracelet/log with unified output; standard `log` is captured.
- `--log-format=json` for structured logs; `console` for colored logs (`--color` controls coloring).

## What It Validates

- Table existence
- Row count (`count`)
- Specified column values (`columns`)

See `test_validation.yaml` for a fuller example.

## License

MIT
