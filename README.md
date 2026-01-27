# bend-archiver
Archive data from common databases into Databend with parallel sync (by key or time range).

## Supported sources
| Data source | Supported |
|:-----------|:---------:|
| MySQL      |    Yes    |
| PostgreSQL |    Yes    |
| TiDB       |    Yes    |
| SQL Server |    Yes    |
| Oracle     | Coming soon |
| CSV        | Coming soon |
| NDJSON     | Coming soon |

## Install
Download the binary from the [release page](https://github.com/databendcloud/bend-archiver/releases).

## Configure
Create `config/conf.json`.

Parameters (defaults are from code):
| Key | Required | Default | Notes |
|:----|:--------:|:--------|:------|
| `databaseType` | No | `mysql` | `mysql`, `tidb`, `pg`, `mssql`, `oracle` |
| `sourceHost` | Yes | - | Source host |
| `sourcePort` | Yes | - | Source port |
| `sourceUser` | Yes | - | Source user |
| `sourcePass` | Yes | - | Source password |
| `sourceDB` | If no `sourceDbTables` | - | Source database |
| `sourceTable` | If no `sourceDbTables` | - | Source table |
| `sourceDbTables` | No | `[]` | Multi-table: `["dbRegex@tableRegex"]` |
| `sourceQuery` | No | - | Currently ignored |
| `sourceWhereCondition` | Yes | - | WHERE clause without `WHERE` |
| `sourceSplitKey` | If key split | - | Integer primary key |
| `sourceSplitTimeKey` | If time split | - | Time column |
| `timeSplitUnit` | If time split | `hour` | `minute`, `quarter`, `hour`, `day` |
| `sslMode` | No | `disable` | Postgres only |
| `databendDSN` | Yes | `localhost:8000` | Databend DSN |
| `databendTable` | Yes | - | Target table |
| `batchSize` | Yes | `1000` | Rows per batch |
| `batchMaxInterval` | No | `3` | Seconds between batches |
| `copyPurge` | No | `true` | Databend COPY option |
| `copyForce` | No | `false` | Databend COPY option |
| `disableVariantCheck` | No | `true` | Databend COPY option |
| `userStage` | No | `~` | Databend stage |
| `deleteAfterSync` | No | `false` | Deletes source rows |
| `maxThread` | No | `1` | Max concurrency |
| `oracleSID` | No | - | Oracle SID |

Rules:
- `sourceWhereCondition` is always required; for time split use `t >= '...' and t < '...'` with `YYYY-MM-DD HH:MM:SS`.
- `sourceSplitKey` and `sourceSplitTimeKey` are mutually exclusive.
- For time split, `timeSplitUnit` is required.

Example (key split):
```json
{
  "databaseType": "mysql",
  "sourceHost": "127.0.0.1",
  "sourcePort": 3306,
  "sourceUser": "root",
  "sourcePass": "123456",
  "sourceDB": "mydb",
  "sourceTable": "test_table",
  "sourceWhereCondition": "id > 0",
  "sourceSplitKey": "id",
  "databendDSN": "databend://username:password@host:port?sslmode=disable",
  "databendTable": "mydb.test_table",
  "batchSize": 40000,
  "maxThread": 5
}
```

Example (time split keys):
```json
{
  "sourceWhereCondition": "t1 >= '2024-06-01 00:00:00' and t1 < '2024-07-01 00:00:00'",
  "sourceSplitTimeKey": "t1",
  "timeSplitUnit": "hour"
}
```

## Run
```bash
./bend-archiver -f config/conf.json
```
If `-f` is omitted, it loads `config/conf.json`.

## Development
### Build
```bash
go build -o bend-archiver ./cmd
```

### Tests
```bash
go test ./...
```
Tests in `cmd` and `source` expect local databases (Databend plus the source DBs in the tests).

### Run from source
```bash
go run ./cmd -f config/conf.json
```

## Notes
- Multi-table sync uses regex in `sourceDbTables` (example: `["^mydb$@^test_table_.*$"]`).
- The MySQL driver reports BOOL as `TINYINT(1)`, so use `TINYINT` in Databend for boolean columns.
- COPY options reference: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table#copy-options
