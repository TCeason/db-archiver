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

Required:
- `sourceHost`, `sourcePort`, `sourceUser`, `sourcePass`
- `databendDSN`, `databendTable`
- `sourceWhereCondition`
- `batchSize`
- exactly one of `sourceSplitKey` or `sourceSplitTimeKey` (time split also needs `timeSplitUnit`)
- either `sourceDbTables` or (`sourceDB` + `sourceTable`)

Common optional:
- `databaseType`: `mysql` (default), `tidb`, `pg`, `mssql`, `oracle`
- `sourceDbTables`: `["dbRegex@tableRegex"]` for multi-table sync
- `timeSplitUnit`: `minute`, `quarter`, `hour`, `day`
- `copyPurge`, `copyForce`, `disableVariantCheck`: Databend COPY options
- `maxThread`, `batchMaxInterval`
- `deleteAfterSync` (uses `sourceWhereCondition`)
- `userStage`, `sslMode`, `oracleSID`
- `sourceQuery` is currently ignored

Notes:
- `sourceWhereCondition` is always required; for time split use `t >= '...' and t < '...'` with `YYYY-MM-DD HH:MM:SS`.
- `sourceSplitKey` and `sourceSplitTimeKey` are mutually exclusive.

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

## Build
```bash
go build -o bend-archiver ./cmd
```

## Tests
```bash
go test ./...
```
Tests in `cmd` and `source` expect local databases (Databend plus the source DBs in the tests).

## Development
Run from source:
```bash
go run ./cmd -f config/conf.json
```

## Notes
- Multi-table sync uses regex in `sourceDbTables` (example: `["^mydb$@^test_table_.*$"]`).
- The MySQL driver reports BOOL as `TINYINT(1)`, so use `TINYINT` in Databend for boolean columns.
- COPY options reference: https://docs.databend.com/sql/sql-commands/dml/dml-copy-into-table#copy-options
