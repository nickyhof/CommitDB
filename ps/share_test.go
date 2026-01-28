package ps

import (
	"os"
	"testing"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/nickyhof/CommitDB/core"
)

// setupShareTestEnv creates a source repo, pushes to bare repo, returns paths
func setupShareTestEnv(t *testing.T) (sourceDir, bareDir string, cleanup func()) {
	t.Helper()

	// Create source CommitDB repo
	sourceDir, err := os.MkdirTemp("", "share-test-source-*")
	if err != nil {
		t.Fatalf("Failed to create source dir: %v", err)
	}

	sourcePersistence, err := NewFilePersistence(sourceDir, nil)
	if err != nil {
		os.RemoveAll(sourceDir)
		t.Fatalf("Failed to create source persistence: %v", err)
	}

	// Create test data
	identity := core.Identity{Name: "test", Email: "test@test.com"}
	_, _ = sourcePersistence.CreateDatabase(core.Database{Name: "testdb"}, identity)
	_, _ = sourcePersistence.CreateTable(core.Table{
		Database: "testdb",
		Name:     "users",
		Columns:  []core.Column{{Name: "id", Type: core.IntType, PrimaryKey: true}},
	}, identity)

	// Create bare repo
	bareDir, err = os.MkdirTemp("", "share-test-bare-*")
	if err != nil {
		os.RemoveAll(sourceDir)
		t.Fatalf("Failed to create bare dir: %v", err)
	}

	bareFS := osfs.New(bareDir)
	bareStorer := filesystem.NewStorage(bareFS, cache.NewObjectLRUDefault())
	_, err = git.Init(bareStorer)
	if err != nil {
		os.RemoveAll(sourceDir)
		os.RemoveAll(bareDir)
		t.Fatalf("Failed to init bare repo: %v", err)
	}

	// Push source to bare
	err = sourcePersistence.AddRemote("origin", bareDir)
	if err != nil {
		os.RemoveAll(sourceDir)
		os.RemoveAll(bareDir)
		t.Fatalf("Failed to add remote: %v", err)
	}

	err = sourcePersistence.Push("origin", "", nil)
	if err != nil {
		os.RemoveAll(sourceDir)
		os.RemoveAll(bareDir)
		t.Fatalf("Failed to push: %v", err)
	}

	cleanup = func() {
		os.RemoveAll(sourceDir)
		os.RemoveAll(bareDir)
	}

	return sourceDir, bareDir, cleanup
}

func TestCreateShare(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	// Create target repo
	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Test CreateShare
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Verify share exists in metadata
	shares, err := persistence.ListShares()
	if err != nil {
		t.Fatalf("ListShares failed: %v", err)
	}
	if len(shares) != 1 {
		t.Errorf("Expected 1 share, got %d", len(shares))
	}
	if shares[0].Name != "myshare" {
		t.Errorf("Expected share name 'myshare', got '%s'", shares[0].Name)
	}
}

func TestCreateShareDuplicate(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create first share
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Try to create duplicate
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err == nil {
		t.Error("Expected error for duplicate share")
	}
}

func TestCreateShareMemoryMode(t *testing.T) {
	persistence, err := NewMemoryPersistence()
	if err != nil {
		t.Fatalf("Failed to create memory persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}
	err = persistence.CreateShare("test", "http://example.com", nil, testIdentity)
	if err == nil {
		t.Error("Expected error for memory mode")
	}
}

func TestListShares(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	// Initially empty
	shares, err := persistence.ListShares()
	if err != nil {
		t.Fatalf("ListShares failed: %v", err)
	}
	if len(shares) != 0 {
		t.Errorf("Expected 0 shares initially, got %d", len(shares))
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create share
	err = persistence.CreateShare("share1", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Should have 1 share
	shares, err = persistence.ListShares()
	if err != nil {
		t.Fatalf("ListShares failed: %v", err)
	}
	if len(shares) != 1 {
		t.Errorf("Expected 1 share, got %d", len(shares))
	}
}

func TestDropShare(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create share
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Get share path before dropping
	sharePath, err := persistence.GetSharePath("myshare")
	if err != nil {
		t.Fatalf("GetSharePath failed: %v", err)
	}

	// Drop share
	err = persistence.DropShare("myshare", testIdentity)
	if err != nil {
		t.Fatalf("DropShare failed: %v", err)
	}

	// Verify share is removed from metadata
	shares, err := persistence.ListShares()
	if err != nil {
		t.Fatalf("ListShares failed: %v", err)
	}
	if len(shares) != 0 {
		t.Errorf("Expected 0 shares after drop, got %d", len(shares))
	}

	// Verify directory is removed
	if _, err := os.Stat(sharePath); !os.IsNotExist(err) {
		t.Error("Expected share directory to be removed")
	}
}

func TestSyncShare(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create share
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Sync share (should succeed even if nothing new)
	err = persistence.SyncShare("myshare", nil)
	if err != nil {
		t.Fatalf("SyncShare failed: %v", err)
	}
}

func TestSyncShareNotFound(t *testing.T) {
	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	err = persistence.SyncShare("nonexistent", nil)
	if err == nil {
		t.Error("Expected error for non-existent share")
	}
}

func TestGetSharePath(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create share
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Get path
	path, err := persistence.GetSharePath("myshare")
	if err != nil {
		t.Fatalf("GetSharePath failed: %v", err)
	}

	expectedPath := targetDir + "/.shares/myshare"
	if path != expectedPath {
		t.Errorf("Expected path '%s', got '%s'", expectedPath, path)
	}

	// Verify directory exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("Expected share directory to exist")
	}
}

func TestGetSharePathNotFound(t *testing.T) {
	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	_, err = persistence.GetSharePath("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent share")
	}
}

func TestOpenSharePersistence(t *testing.T) {
	_, bareDir, cleanup := setupShareTestEnv(t)
	defer cleanup()

	targetDir, err := os.MkdirTemp("", "share-test-target-*")
	if err != nil {
		t.Fatalf("Failed to create target dir: %v", err)
	}
	defer os.RemoveAll(targetDir)

	persistence, err := NewFilePersistence(targetDir, nil)
	if err != nil {
		t.Fatalf("Failed to create persistence: %v", err)
	}

	testIdentity := core.Identity{Name: "test", Email: "test@test.com"}

	// Create share
	err = persistence.CreateShare("myshare", bareDir, nil, testIdentity)
	if err != nil {
		t.Fatalf("CreateShare failed: %v", err)
	}

	// Open share persistence
	sharePersistence, err := persistence.OpenSharePersistence("myshare")
	if err != nil {
		t.Fatalf("OpenSharePersistence failed: %v", err)
	}

	if !sharePersistence.IsInitialized() {
		t.Error("Expected share persistence to be initialized")
	}

	// Should be able to list databases from share
	databases := sharePersistence.ListDatabases()
	found := false
	for _, db := range databases {
		if db == "testdb" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected to find 'testdb' in share, got: %v", databases)
	}
}
