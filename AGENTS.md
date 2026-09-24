# AGENTS.md

Guidance for AI coding agents working in this repository.

## Overview

`github.com/smallstep/nosql` is a small Go library (Apache-2.0) that provides a
key/value persistence abstraction with pluggable backends: Badger v1, Badger v2,
bbolt (BoltDB), MySQL/MariaDB, and PostgreSQL. It is consumed by
`github.com/smallstep/certificates` (step-ca) as its database layer, so the
`database.DB` interface is effectively a public API: changing method signatures
or error semantics is a breaking change for downstream users. The README states
the API is not yet stable, but treat it as stable in practice.

There is no binary, no `go generate`, no mocks, and no migrations. Each SQL
backend creates its own database and tables on the fly (`CREATE DATABASE IF NOT
EXISTS`, `CREATE TABLE IF NOT EXISTS`) inside `Open` and `CreateTable`.

## Commands

```bash
make test        # go test -short -coverprofile=coverage.out ./...  (no external services)
make ci-test     # same, with CI=1 so the MySQL and PostgreSQL tests actually run
make lint        # golangci-lint (config fetched from smallstep/workflows) + govulncheck
make fmt         # goimports -w on all .go files
make bootstrap   # install golangci-lint, govulncheck, gotestsum
make build       # no-op; this is a library. Use: go build ./...
```

Run a single test:

```bash
go test -short -run TestBolt ./...
go test -short -run Test_toBadgerKey ./badger/v1/
```

Verify all backend stubs still compile with every driver disabled:

```bash
go build -tags nobadger,nobbolt,nomysql,nopgx ./...
```

`go build ./...`, `go vet ./...`, and `make test` all complete in a few seconds
with no network access, environment variables, or Docker.

## Tests and live databases

`nosql_test.go` runs one shared conformance suite (`run(t, db)`) against each
backend via `New(driver, dsn, ...)`:

| Test | Backend | Needs |
|------|---------|-------|
| `TestBadger` | Badger v1 | nothing; writes to `./tmp/badgerdb` |
| `TestBolt` | bbolt | nothing; writes to `./tmp/boltdb` |
| `TestMySQL` | MySQL | `CI=1` and a MySQL server at `127.0.0.1:3306`, user `user`, password `password`, database `test` |
| `TestPostgreSQL` | PostgreSQL | `CI=1` and a PostgreSQL server at `127.0.0.1:5432`, user `user`, password `password`, database `test` |

Without `CI` set, `TestMySQL` and `TestPostgreSQL` print "Not running ...
integration tests" and return early as passing, so a green `make test` says
nothing about the SQL backends. `TestMain` creates `./tmp` in the repo root and
removes it afterwards; `tmp/` is not in `.gitignore`, so a test that is killed
mid-run can leave it behind.

`badger/v1` and `badger/v2` also have unit tests for key encoding
(`Test_badgerEncode`, `Test_toBadgerKey`, etc.) that need no database.

The GitHub Actions `test` job (`.github/workflows/ci.yml`) starts `postgres:17`
and `mysql:5.7` service containers with exactly those credentials and runs
`V=1 make ci-test` on Go `stable` and `oldstable`. To reproduce locally, start
the two containers with the same env and run `make ci-test`. The separate `ci`
job calls the shared `goCI.yml` workflow for lint, govulncheck, build, and
CodeQL (`run-test: false`, tests live in the explicit job above).

## Build tags

Every backend has a `no<driver>` build tag that swaps the real implementation
for `database.NotSupportedDB`, whose methods all return
`database.ErrOpNotSupported`. This lets downstream binaries drop heavy
dependencies. Keep both files in sync when changing a backend's exported
surface.

| Tag | Disables | Files |
|-----|----------|-------|
| `nobadger` | Badger v1 and v2 | `badger/v1/{badger,nobadger}.go`, `badger/v2/{badger,nobadger}.go` |
| `nobadgerv1` | Badger v1 only | `badger/v1/` |
| `nobadgerv2` | Badger v2 only | `badger/v2/` |
| `nobbolt` | bbolt | `bolt/{bbolt,nobbolt}.go` |
| `nomysql` | MySQL | `mysql/{mysql,nomysql}.go` |
| `nopgx` | PostgreSQL (pgx) | `postgresql/{postgresql,nopostgresql}.go` |

## Architecture

```
nosql/
├── nosql.go              # New(driver, dsn, opts...) factory; driver name constants;
│                         # type aliases re-exporting database.DB / Option / With*
├── nosql_test.go         # shared conformance suite + per-backend TestXxx entry points
├── database/
│   ├── database.go       # DB interface, Options/Option, Tx/TxEntry/TxCmd, Entry,
│   │                     # ErrNotFound / ErrOpNotSupported and IsErr* helpers
│   └── notsupported.go   # NotSupportedDB stub used behind the no* build tags
├── badger/v1/            # dgraph-io/badger v1 (default for driver "badger")
├── badger/v2/            # dgraph-io/badger/v2 (driver "badgerv2")
├── bolt/                 # go.etcd.io/bbolt (driver "bbolt")
├── mysql/                # go-sql-driver/mysql (driver "mysql")
└── postgresql/           # jackc/pgx/v5 via database/sql (driver "postgresql")
```

Driver names accepted by `New` (case-insensitive): `badger`, `badgerv1`,
`badgerv2`, `bbolt`, `mysql`, `postgresql`. `badger` currently means v1.

Data model: every backend exposes buckets (tables) of `[]byte` key to `[]byte`
value. SQL backends map a bucket to a table with `nkey` (max 255 bytes, primary
key) and `nvalue` columns. Badger encodes bucket and key into a single
length-prefixed key (see `toBadgerKey` / `fromBadgerKey`). `Update(tx)` runs a
`database.Tx` list of operations atomically; `CmpAndSwap` is the primitive the
conformance suite leans on most, so keep its return contract
(`(current, swapped, err)`) identical across backends. Badger backends also
implement `nosql.Compactor` (`Compact(discardRatio)`) for value-log GC.

## Conventions

- Errors are wrapped with `github.com/pkg/errors` (`errors.Wrap`,
  `errors.Wrapf`, `errors.WithStack`) in every backend. `database/database.go`
  uses the standard library `errors` and `IsErrNotFound` matches with
  `errors.Is`, so wrapping must preserve the chain. A missing key or table must
  wrap `database.ErrNotFound` (SQL backends may also surface `sql.ErrNoRows`,
  which `IsErrNotFound` accepts).
- Tests use `github.com/smallstep/assert`, not testify. New backend behavior
  belongs in the shared `run` suite in `nosql_test.go` so every backend is
  checked the same way.
- `CreateTable` and `DeleteTable` must be idempotent; `DeleteTable` on a
  missing table returns `ErrNotFound`. Bucket names may contain characters like
  `-`, so SQL backends must quote identifiers (`quoteIdentifier` in
  postgresql, backticks in mysql).
- Options: `WithDatabase` (SQL backends; overrides the database in the DSN),
  `WithValueDir` and `WithBadgerFileLoadingMode` (Badger only). Ignore options
  that do not apply to a backend rather than erroring.
- Lint config is not vendored; `make lint` curls `.golangci.yml` from
  `smallstep/workflows` at run time, so it needs network access.
- Keep the repository free of internal or customer-specific references; it is
  public.
