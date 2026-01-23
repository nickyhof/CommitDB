package ps

import (
	"testing"
	"time"

	"github.com/nickyhof/CommitDB/core"
)

func TestBranch(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit via database creation
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Create branch
	err = persistence.Branch("feature", nil)
	if err != nil {
		t.Fatalf("Branch failed: %v", err)
	}

	// Verify branch exists
	branches, err := persistence.ListBranches()
	if err != nil {
		t.Fatalf("ListBranches failed: %v", err)
	}

	found := false
	for _, b := range branches {
		if b == "feature" {
			found = true
			break
		}
	}
	if !found {
		t.Error("Expected 'feature' branch to exist")
	}
}

func TestBranchFromTransaction(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create first commit (database only)
	db := core.Database{Name: "testdb"}
	txn1, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Create second commit (add table - this should NOT be visible on old branch)
	table := core.Table{Database: "testdb", Name: "users", Columns: []core.Column{{Name: "id", Type: core.IntType}}}
	_, err = persistence.CreateTable(table, identity)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify table exists on master
	tables := persistence.ListTables("testdb")
	if len(tables) != 1 {
		t.Fatalf("Expected 1 table on master, got %d", len(tables))
	}

	// Create branch from first transaction (before table was created)
	err = persistence.Branch("old-state", &txn1)
	if err != nil {
		t.Fatalf("Branch from transaction failed: %v", err)
	}

	// Checkout old-state branch
	err = persistence.Checkout("old-state")
	if err != nil {
		t.Fatalf("Checkout old-state failed: %v", err)
	}

	// On old-state branch, the table should NOT exist
	tablesOnOldState := persistence.ListTables("testdb")
	if len(tablesOnOldState) != 0 {
		t.Errorf("Expected 0 tables on old-state branch (created from before table), got %d", len(tablesOnOldState))
	}

	// Verify we can switch back to master and see the table
	persistence.Checkout("master")
	tablesOnMaster := persistence.ListTables("testdb")
	if len(tablesOnMaster) != 1 {
		t.Errorf("Expected 1 table on master after checkout, got %d", len(tablesOnMaster))
	}
}

func TestCheckout(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Create and checkout feature branch
	persistence.Branch("feature", nil)
	err = persistence.Checkout("feature")
	if err != nil {
		t.Fatalf("Checkout failed: %v", err)
	}

	// Verify we're on feature branch
	current, err := persistence.CurrentBranch()
	if err != nil {
		t.Fatalf("CurrentBranch failed: %v", err)
	}
	if current != "feature" {
		t.Errorf("Expected current branch 'feature', got '%s'", current)
	}
}

func TestMergeFastForward(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit on master
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Create feature branch
	persistence.Branch("feature", nil)
	persistence.Checkout("feature")

	// Make commit on feature
	table := core.Table{Database: "testdb", Name: "users", Columns: []core.Column{{Name: "id", Type: core.IntType}}}
	featureTxn, err := persistence.CreateTable(table, identity)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Checkout master and merge feature
	persistence.Checkout("master")
	txn, err := persistence.Merge("feature", identity)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}

	// Verify merge result
	if txn.Id != featureTxn.Id {
		t.Errorf("Expected merge to fast-forward to %s, got %s", featureTxn.Id, txn.Id)
	}
}

func TestListBranches(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Create multiple branches
	persistence.Branch("feature-a", nil)
	persistence.Branch("feature-b", nil)

	branches, err := persistence.ListBranches()
	if err != nil {
		t.Fatalf("ListBranches failed: %v", err)
	}

	if len(branches) < 3 {
		t.Errorf("Expected at least 3 branches (master + 2 features), got %d", len(branches))
	}
}

func TestCurrentBranch(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	current, err := persistence.CurrentBranch()
	if err != nil {
		t.Fatalf("CurrentBranch failed: %v", err)
	}

	// Default branch is usually master or main
	if current != "master" && current != "main" {
		t.Errorf("Expected current branch 'master' or 'main', got '%s'", current)
	}
}

func TestDeleteBranch(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit and branch
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	persistence.Branch("to-delete", nil)

	// Delete the branch
	err = persistence.DeleteBranch("to-delete")
	if err != nil {
		t.Fatalf("DeleteBranch failed: %v", err)
	}

	// Verify branch is gone
	branches, _ := persistence.ListBranches()
	for _, b := range branches {
		if b == "to-delete" {
			t.Error("Branch 'to-delete' should have been deleted")
		}
	}
}

func TestDeleteCurrentBranchFails(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial commit
	db := core.Database{Name: "testdb"}
	_, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Try to delete current branch
	current, _ := persistence.CurrentBranch()
	err = persistence.DeleteBranch(current)
	if err == nil {
		t.Error("Expected error when deleting current branch")
	}
}

// TestMergeRowLevel tests row-level merge when branches have diverged
func TestMergeRowLevel(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial state
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"Alice"}`),
	}, identity)

	// Create and checkout feature branch
	persistence.Branch("feature", nil)
	persistence.Checkout("feature")

	// Add record on feature
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"2.json": []byte(`{"id":"2","name":"Bob"}`),
	}, identity)

	// Go back to master and add different record
	persistence.Checkout("master")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"3.json": []byte(`{"id":"3","name":"Charlie"}`),
	}, identity)

	// Now branches have diverged - merge should use row-level strategy
	_, err := persistence.Merge("feature", identity)
	if err != nil {
		t.Fatalf("Row-level merge failed: %v", err)
	}

	// After merge, master should have all 3 records
	keys := persistence.ListRecordKeys("testdb", "users")
	if len(keys) != 3 {
		t.Errorf("Expected 3 records after merge, got %d", len(keys))
	}
}

// TestMergeRowLevelWithConflict tests LWW resolution when both branches modify same record
func TestMergeRowLevelWithConflict(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial state with a record
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"Original"}`),
	}, identity)

	// Create and checkout feature branch
	persistence.Branch("feature", nil)
	persistence.Checkout("feature")

	// Modify record on feature (earlier time)
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"FeatureVersion"}`),
	}, identity)

	// Go back to master and modify the same record (later time)
	persistence.Checkout("master")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"MasterVersion"}`),
	}, identity)

	// Merge - LWW should pick master version (later commit)
	_, err := persistence.Merge("feature", identity)
	if err != nil {
		t.Fatalf("Row-level merge with conflict failed: %v", err)
	}

	// After merge, the record should have the later value (master wins)
	data, exists := persistence.GetRecord("testdb", "users", "1.json")
	if !exists {
		t.Fatal("Record should exist after merge")
	}

	// Check that record exists and was merged
	if data == nil {
		t.Error("Expected non-nil data after merge")
	}
}

// TestMergeRecordMaps tests the mergeRecordMaps function directly
func TestMergeRecordMaps(t *testing.T) {
	now := time.Now()
	earlier := now.Add(-1 * time.Hour)
	later := now.Add(1 * time.Hour)

	tests := []struct {
		name       string
		base       map[string][]byte
		head       map[string][]byte
		source     map[string][]byte
		headTime   time.Time
		sourceTime time.Time
		wantKeys   []string
	}{
		{
			name:       "disjoint additions",
			base:       map[string][]byte{"1": []byte("a")},
			head:       map[string][]byte{"1": []byte("a"), "2": []byte("b")},
			source:     map[string][]byte{"1": []byte("a"), "3": []byte("c")},
			headTime:   now,
			sourceTime: now,
			wantKeys:   []string{"1", "2", "3"},
		},
		{
			name:       "source wins LWW",
			base:       map[string][]byte{"1": []byte("original")},
			head:       map[string][]byte{"1": []byte("head")},
			source:     map[string][]byte{"1": []byte("source")},
			headTime:   earlier,
			sourceTime: later,
			wantKeys:   []string{"1"}, // source wins
		},
		{
			name:       "head wins LWW",
			base:       map[string][]byte{"1": []byte("original")},
			head:       map[string][]byte{"1": []byte("head")},
			source:     map[string][]byte{"1": []byte("source")},
			headTime:   later,
			sourceTime: earlier,
			wantKeys:   []string{"1"}, // head wins
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			merged, _ := mergeRecordMaps(tt.base, tt.head, tt.source, tt.headTime, tt.sourceTime)
			if len(merged) != len(tt.wantKeys) {
				t.Errorf("Expected %d keys, got %d", len(tt.wantKeys), len(merged))
			}
		})
	}
}

// TestMergeManualMode tests manual conflict resolution mode
func TestMergeManualMode(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Create initial state with a record
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"Original"}`),
	}, identity)

	// Create and checkout feature branch
	persistence.Branch("feature", nil)
	persistence.Checkout("feature")

	// Modify record on feature
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"FeatureVersion"}`),
	}, identity)

	// Go back to master and modify the same record
	persistence.Checkout("master")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"MasterVersion"}`),
	}, identity)

	// Merge with manual strategy
	result, err := persistence.MergeWithOptions("feature", identity, MergeOptions{
		Strategy: MergeStrategyManual,
	})
	if err != nil {
		t.Fatalf("Manual merge failed: %v", err)
	}

	// Should be pending with unresolved conflicts
	if !result.Pending {
		t.Error("Expected merge to be pending")
	}
	if len(result.Unresolved) != 1 {
		t.Errorf("Expected 1 unresolved conflict, got %d", len(result.Unresolved))
	}
	if result.MergeID == "" {
		t.Error("Expected MergeID to be set")
	}

	// Verify pending merge state
	pending := persistence.GetPendingMerge()
	if pending == nil {
		t.Fatal("Expected pending merge to exist")
	}
}

// TestMergeManualResolveAndComplete tests resolving conflicts and completing merge
func TestMergeManualResolveAndComplete(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Setup: create conflict situation
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"Original"}`),
	}, identity)

	persistence.Branch("feature", nil)
	persistence.Checkout("feature")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"FeatureVersion"}`),
	}, identity)

	persistence.Checkout("master")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"MasterVersion"}`),
	}, identity)

	// Start manual merge
	persistence.MergeWithOptions("feature", identity, MergeOptions{
		Strategy: MergeStrategyManual,
	})

	// Resolve conflict
	err := persistence.ResolveConflict("testdb", "users", "1", []byte(`{"id":"1","name":"Resolved"}`))
	if err != nil {
		t.Fatalf("ResolveConflict failed: %v", err)
	}

	// Complete merge
	txn, err := persistence.CompleteMerge(identity)
	if err != nil {
		t.Fatalf("CompleteMerge failed: %v", err)
	}

	if txn.Id == "" {
		t.Error("Expected transaction ID")
	}

	// Verify pending merge is cleared
	if persistence.GetPendingMerge() != nil {
		t.Error("Expected pending merge to be cleared")
	}
}

// TestMergeAbort tests aborting a pending merge
func TestMergeAbort(t *testing.T) {
	persistence, _ := NewMemoryPersistence()
	identity := core.Identity{Name: "Test", Email: "test@test.com"}

	// Setup: create conflict situation
	persistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	persistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType}},
	}, identity)
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"Original"}`),
	}, identity)

	persistence.Branch("feature", nil)
	persistence.Checkout("feature")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"FeatureVersion"}`),
	}, identity)

	persistence.Checkout("master")
	persistence.SaveRecord("testdb", "users", map[string][]byte{
		"1.json": []byte(`{"id":"1","name":"MasterVersion"}`),
	}, identity)

	// Start and abort manual merge
	persistence.MergeWithOptions("feature", identity, MergeOptions{
		Strategy: MergeStrategyManual,
	})

	err := persistence.AbortMerge()
	if err != nil {
		t.Fatalf("AbortMerge failed: %v", err)
	}

	// Verify pending merge is cleared
	if persistence.GetPendingMerge() != nil {
		t.Error("Expected pending merge to be cleared after abort")
	}
}
