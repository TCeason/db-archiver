package source

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/databendcloud/db-archiver/config"
)

type MysqlSource struct {
	db            *sql.DB
	cfg           *config.Config
	statsRecorder *DatabendSourceStatsRecorder
}

func NewMysqlSource(cfg *config.Config) (*MysqlSource, error) {
	stats := NewDatabendIntesterStatsRecorder()
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql",
		cfg.SourceUser,
		cfg.SourcePass,
		cfg.SourceHost,
		cfg.SourcePort))
	if err != nil {
		logrus.Errorf("failed to open db: %v", err)
		return nil, err
	}
	//fmt.Printf("connected to mysql successfully %v", cfg)
	return &MysqlSource{
		db:            db,
		cfg:           cfg,
		statsRecorder: stats,
	}, nil
}

// AdjustBatchSizeAccordingToSourceDbTable has a concept called s,  s = (maxKey - minKey) / sourceTableRowCount
// if s == 1 it means the data is uniform in the table, if s is much bigger than 1, it means the data is not uniform in the table
func (s *MysqlSource) AdjustBatchSizeAccordingToSourceDbTable() uint64 {
	minSplitKey, maxSplitKey, err := s.GetMinMaxSplitKey()
	if err != nil {
		return uint64(s.cfg.BatchSize)
	}
	sourceTableRowCount, err := s.GetSourceReadRowsCount()
	if err != nil {
		return uint64(s.cfg.BatchSize)
	}
	rangeSize := maxSplitKey - minSplitKey + 1
	switch {
	case int64(sourceTableRowCount) <= s.cfg.BatchSize:
		return rangeSize
	case rangeSize/uint64(sourceTableRowCount) >= 10:
		return uint64(s.cfg.BatchSize * 5)
	case rangeSize/uint64(sourceTableRowCount) >= 100:
		return uint64(s.cfg.BatchSize * 20)
	default:
		return uint64(s.cfg.BatchSize)
	}
}

func (s *MysqlSource) GetSourceReadRowsCount() (int, error) {
	row := s.db.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s.%s WHERE %s", s.cfg.SourceDB,
		s.cfg.SourceTable, s.cfg.SourceWhereCondition))
	var rowCount int
	err := row.Scan(&rowCount)
	if err != nil {
		return 0, err
	}

	return rowCount, nil
}

func (s *MysqlSource) GetMinMaxSplitKey() (uint64, uint64, error) {
	query := fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s.%s WHERE %s",
		s.cfg.SourceSplitKey, s.cfg.SourceSplitKey,
		s.cfg.SourceDB, s.cfg.SourceTable, s.cfg.SourceWhereCondition)

	rows, err := s.db.Query(query)
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()

	var minSplitKey, maxSplitKey interface{}
	for rows.Next() {
		err = rows.Scan(&minSplitKey, &maxSplitKey)
		if err != nil {
			return 0, 0, err
		}
	}

	// 处理 NULL 值
	if minSplitKey == nil || maxSplitKey == nil {
		return 0, 0, nil
	}

	min64, err := toUint64(minSplitKey)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to convert min value: %w", err)
	}

	max64, err := toUint64(maxSplitKey)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to convert max value: %w", err)
	}

	return min64, max64, nil
}

func (s *MysqlSource) GetMinMaxTimeSplitKey() (string, string, error) {
	rows, err := s.db.Query(fmt.Sprintf("select min(%s), max(%s) from %s.%s WHERE %s", s.cfg.SourceSplitTimeKey,
		s.cfg.SourceSplitTimeKey, s.cfg.SourceDB, s.cfg.SourceTable, s.cfg.SourceWhereCondition))
	if err != nil {
		return "", "", err
	}
	defer rows.Close()

	var minSplitKey, maxSplitKey string
	for rows.Next() {
		err = rows.Scan(&minSplitKey, &maxSplitKey)
		if err != nil {
			return "", "", err
		}
	}
	return minSplitKey, maxSplitKey, nil
}

func (s *MysqlSource) DeleteAfterSync() error {
	logrus.Infof("DeleteAfterSync: %v", s.cfg.DeleteAfterSync)
	if !s.cfg.DeleteAfterSync {
		return nil
	}

	dbTables, err := s.GetDbTablesAccordingToSourceDbTables()
	if err != nil {
		return err
	}

	logrus.Infof("dbTables: %v", dbTables)

	for db, tables := range dbTables {
		for _, table := range tables {
			count, err := s.GetSourceReadRowsCount()
			if err != nil {
				log.Printf("Error getting row count for table %s.%s: %v", db, table, err)
				continue
			}

			// Delete in batches
			for count > 0 {
				limit := min(int(s.cfg.BatchSize), count)
				query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s LIMIT %d", db, table, s.cfg.SourceWhereCondition, limit)
				_, err := s.db.Exec(query)
				if err != nil {
					log.Printf("Error deleting rows from table %s.%s: %v", db, table, err)
					break
				}
				count -= limit
				log.Printf("Deleted %d rows from table %s.%s\n", limit, db, table)
				time.Sleep(time.Duration(s.cfg.BatchMaxInterval) * time.Second)
			}
		}
	}

	return nil
}

// Utility function to get the smaller of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (s *MysqlSource) QueryTableData(threadNum int, conditionSql string) ([][]interface{}, []string, error) {
	startTime := time.Now()
	execSql := fmt.Sprintf("SELECT * FROM %s.%s WHERE %s", s.cfg.SourceDB,
		s.cfg.SourceTable, conditionSql)
	if s.cfg.SourceWhereCondition != "" && s.cfg.SourceSplitKey != "" {
		execSql = fmt.Sprintf("%s AND %s", execSql, s.cfg.SourceWhereCondition)
	}
	rows, err := s.db.Query(execSql)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	scanArgs := make([]interface{}, len(columns))
	for i, columnType := range columnTypes {
		switch columnType.DatabaseTypeName() {
		case "INT", "SMALLINT", "TINYINT", "MEDIUMINT", "BIGINT":
			scanArgs[i] = new(sql.NullInt64)
		case "UNSIGNED BIGINT":
			scanArgs[i] = new(NullUint64)
		case "UNSIGNED INT", "UNSIGNED TINYINT", "UNSIGNED MEDIUMINT":
			scanArgs[i] = new(sql.NullInt64)
		case "FLOAT", "DOUBLE":
			scanArgs[i] = new(sql.NullFloat64)
		case "DECIMAL":
			scanArgs[i] = new(sql.NullFloat64)
		case "CHAR", "VARCHAR", "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
			scanArgs[i] = new(sql.NullString)
		case "DATE", "TIME", "DATETIME", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString) // or use time.Time
		case "BOOL", "BOOLEAN":
			scanArgs[i] = new(sql.NullBool)
		default:
			scanArgs[i] = new(sql.RawBytes)
		}
	}

	var result [][]interface{}
	//rowCount, err := s.GetRowsCountByConditionSql(conditionSql)
	//if err != nil {
	//	return nil, nil, err
	//}
	//result := make([][]interface{}, rowCount)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return nil, nil, err
		}

		row := make([]interface{}, len(columns))
		for i, v := range scanArgs {
			switch v := v.(type) {
			case *int:
				row[i] = *v
			case *string:
				row[i] = *v
			case *sql.NullString:
				if v.Valid {
					row[i] = v.String
				} else {
					row[i] = nil
				}
			case *bool:
				row[i] = *v
			case *sql.NullInt64:
				if v.Valid {
					row[i] = v.Int64
				} else {
					row[i] = nil
				}
			case *sql.NullFloat64:
				if v.Valid {
					row[i] = v.Float64
				} else {
					row[i] = nil
				}
			case *NullUint64:
				if v.Valid {
					row[i] = v.Uint64
				} else {
					row[i] = nil
				}
			case *sql.NullBool:
				if v.Valid {
					row[i] = v.Bool
				} else {
					row[i] = nil
				}
			case *float64:
				row[i] = *v
			case *sql.RawBytes:
				row[i] = string(*v)
			}
		}
		result = append(result, row)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, err
	}
	s.statsRecorder.RecordMetric(len(result))
	stats := s.statsRecorder.Stats(time.Since(startTime))
	log.Printf("thread-%d: extract %d rows (%f rows/s)", threadNum, len(result)+1, stats.RowsPerSecondd)

	return result, columns, nil
}

func (s *MysqlSource) GetDatabasesAccordingToSourceDbRegex(sourceDatabasePattern string) ([]string, error) {
	rows, err := s.db.Query("SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var database string
		err = rows.Scan(&database)
		if err != nil {
			return nil, err
		}
		match, err := regexp.MatchString(sourceDatabasePattern, database)
		if err != nil {
			return nil, err
		}
		if match {
			databases = append(databases, database)
		}

	}
	return databases, nil
}

func (s *MysqlSource) GetTablesAccordingToSourceTableRegex(sourceTablePattern string, databases []string) (map[string][]string, error) {
	dbTables := make(map[string][]string)
	for _, database := range databases {
		rows, err := s.db.Query(fmt.Sprintf("SHOW TABLES FROM %s", database))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var tables []string
		for rows.Next() {
			var table string
			err = rows.Scan(&table)
			if err != nil {
				return nil, err
			}
			match, err := regexp.MatchString(sourceTablePattern, table)
			if err != nil {
				return nil, err
			}
			if match {
				tables = append(tables, table)
			}
		}
		dbTables[database] = tables
	}
	return dbTables, nil
}

func (s *MysqlSource) GetAllSourceReadRowsCount() (int, error) {
	allCount := 0

	dbTables, err := s.GetDbTablesAccordingToSourceDbTables()
	if err != nil {
		return 0, err
	}
	for db, tables := range dbTables {
		s.cfg.SourceDB = db
		for _, table := range tables {
			s.cfg.SourceTable = table
			count, err := s.GetSourceReadRowsCount()
			if err != nil {
				return 0, err
			}
			allCount += count
		}
	}
	if allCount != 0 {
		return allCount, nil
	}
	if len(dbTables) == 0 && s.cfg.SourceTable != "" {
		count, err := s.GetSourceReadRowsCount()
		if err != nil {
			return 0, err
		}
		allCount += count
	}

	return allCount, nil
}

func (s *MysqlSource) GetDbTablesAccordingToSourceDbTables() (map[string][]string, error) {
	allDbTables := make(map[string][]string)
	for _, sourceDbTable := range s.cfg.SourceDbTables {
		dbTable := strings.Split(sourceDbTable, "@") // because `.` in regex is a special character, so use `@` to split
		if len(dbTable) != 2 {
			return nil, fmt.Errorf("invalid sourceDbTable: %s, should be a.b format", sourceDbTable)
		}
		dbs, err := s.GetDatabasesAccordingToSourceDbRegex(dbTable[0])
		if err != nil {
			return nil, fmt.Errorf("get databases according to sourceDbRegex failed: %v", err)
		}
		dbTables, err := s.GetTablesAccordingToSourceTableRegex(dbTable[1], dbs)
		if err != nil {
			return nil, fmt.Errorf("get tables according to sourceTableRegex failed: %v", err)
		}
		for db, tables := range dbTables {
			allDbTables[db] = append(allDbTables[db], tables...)
		}
	}
	if s.cfg.SourceDB != "" && s.cfg.SourceTable != "" {
		allDbTables[s.cfg.SourceDB] = append(allDbTables[s.cfg.SourceDB], s.cfg.SourceTable)
	}
	return allDbTables, nil
}

// NullUint64 represents a uint64 that may be null.
type NullUint64 struct {
	Uint64 uint64
	Valid  bool // Valid is true if Uint64 is not NULL
}

// Scan implements the Scanner interface.
func (n *NullUint64) Scan(value interface{}) error {
	if value == nil {
		n.Uint64, n.Valid = 0, false
		return nil
	}

	n.Valid = true
	switch v := value.(type) {
	case uint64:
		n.Uint64 = v
	case int64:
		if v < 0 {
			// 处理溢出的情况
			n.Uint64 = uint64(v)
		} else {
			n.Uint64 = uint64(v)
		}
	case []byte:
		var err error
		n.Uint64, err = strconv.ParseUint(string(v), 10, 64)
		if err != nil {
			return err
		}
	case string:
		var err error
		n.Uint64, err = strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("cannot scan type %T into NullUint64", value)
	}
	return nil
}

// Value implements the driver Valuer interface.
func (n NullUint64) Value() (driver.Value, error) {
	if !n.Valid {
		return nil, nil
	}
	return n.Uint64, nil
}
