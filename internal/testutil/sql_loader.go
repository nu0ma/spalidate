package testutil

import (
	"bytes"
	"database/sql"
	"os"
	"testing"
)

// LoadTestDataFromSQL loads test data from a single SQL file
func LoadTestDataFromSQL(t *testing.T, db *sql.DB, sqlFilePath string) {
	t.Helper()
	
	sqlContent, err := os.ReadFile(sqlFilePath)
	if err != nil {
		t.Fatalf("cannot read SQL file: %v", err)
	}
	
	if _, err := db.Exec(string(sqlContent)); err != nil {
		t.Fatalf("cannot execute SQL: %v", err)
	}
}

// LoadSQLInBatchesBySplitter splits SQL file by delimiter and executes in batches
func LoadSQLInBatchesBySplitter(t *testing.T, db *sql.DB, sqlFilePath string, splitter []byte) {
	t.Helper()
	
	sqlContent, err := os.ReadFile(sqlFilePath)
	if err != nil {
		t.Errorf("cannot read SQL file: %v", err)
		return
	}
	
	batches := bytes.Split(sqlContent, splitter)
	LoadSQLInBatches(t, db, batches)
}

// LoadSQLInBatches executes SQL statements in batches
func LoadSQLInBatches(t *testing.T, db *sql.DB, batches [][]byte) {
	t.Helper()
	
	for _, batch := range batches {
		if len(batch) == 0 {
			continue
		}
		
		if _, err := db.Exec(string(batch)); err != nil {
			t.Errorf("cannot execute SQL batch: %v", err)
			return
		}
	}
}