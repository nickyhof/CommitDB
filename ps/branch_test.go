package ps

import (
	"testing"

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

	// Create first commit
	db := core.Database{Name: "testdb"}
	txn1, err := persistence.CreateDatabase(db, identity)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Create second commit
	table := core.Table{Database: "testdb", Name: "users", Columns: []core.Column{{Name: "id", Type: core.IntType}}}
	_, err = persistence.CreateTable(table, identity)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Create branch from first transaction
	err = persistence.Branch("old-state", &txn1)
	if err != nil {
		t.Fatalf("Branch from transaction failed: %v", err)
	}

	branches, _ := persistence.ListBranches()
	found := false
	for _, b := range branches {
		if b == "old-state" {
			found = true
		}
	}
	if !found {
		t.Error("Expected 'old-state' branch to exist")
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
