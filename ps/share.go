package ps

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/nickyhof/CommitDB/core"
)

// Share represents an external database reference
type Share struct {
	Name      string `json:"name"`
	URL       string `json:"url"`
	CommitRef string `json:"commit_ref,omitempty"` // Last synced commit
}

// SharesConfig holds all shares metadata
type SharesConfig struct {
	Shares []Share `json:"shares"`
}

// CreateShare creates a new share by cloning an external repository
func (p *Persistence) CreateShare(name, url string, auth *RemoteAuth, identity core.Identity) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	// Memory mode doesn't support shares
	if p.isMemoryMode {
		return fmt.Errorf("shares are not supported in memory mode")
	}

	// Get the base directory
	wt, err := p.repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}
	baseDir := wt.Filesystem.Root()

	// Check if share already exists
	shares, err := p.ListShares()
	if err != nil {
		return err
	}
	for _, s := range shares {
		if s.Name == name {
			return fmt.Errorf("share '%s' already exists", name)
		}
	}

	// Create shares directory
	sharesDir := filepath.Join(baseDir, ".shares")
	if err := os.MkdirAll(sharesDir, 0755); err != nil {
		return fmt.Errorf("failed to create shares directory: %w", err)
	}

	// Clone the external repository into .shares/<name>/
	shareDir := filepath.Join(sharesDir, name)

	authMethod, err := auth.getAuthMethod()
	if err != nil {
		return fmt.Errorf("failed to configure auth: %w", err)
	}

	// Use PlainClone for simpler handling of both local and remote repos
	_, err = git.PlainClone(shareDir, &git.CloneOptions{
		URL:  url,
		Auth: authMethod,
	})
	if err != nil {
		return fmt.Errorf("failed to clone share '%s': %w", name, err)
	}

	// Save share metadata
	share := Share{Name: name, URL: url}
	return p.saveShare(share, identity)
}

// SyncShare pulls latest changes from the share's remote
func (p *Persistence) SyncShare(name string, auth *RemoteAuth) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	if p.isMemoryMode {
		return fmt.Errorf("shares are not supported in memory mode")
	}

	// Get share info
	shares, err := p.ListShares()
	if err != nil {
		return err
	}
	var found bool
	for _, s := range shares {
		if s.Name == name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("share '%s' not found", name)
	}

	// Open the share repository
	sharePath, err := p.GetSharePath(name)
	if err != nil {
		return err
	}

	shareWt := osfs.New(sharePath)
	shareGitDir, err := shareWt.Chroot(".git")
	if err != nil {
		return fmt.Errorf("failed to open share git directory: %w", err)
	}

	storer := filesystem.NewStorageWithOptions(
		shareGitDir,
		cache.NewObjectLRUDefault(),
		filesystem.Options{ExclusiveAccess: true})

	shareRepo, err := git.Open(storer, shareWt)
	if err != nil {
		return fmt.Errorf("failed to open share repository: %w", err)
	}

	wt, err := shareRepo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get share worktree: %w", err)
	}

	authMethod, err := auth.getAuthMethod()
	if err != nil {
		return fmt.Errorf("failed to configure auth: %w", err)
	}

	err = wt.Pull(&git.PullOptions{
		Auth: authMethod,
	})
	if err == git.NoErrAlreadyUpToDate {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to sync share '%s': %w", name, err)
	}

	return nil
}

// DropShare removes a share
func (p *Persistence) DropShare(name string, identity core.Identity) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	if p.isMemoryMode {
		return fmt.Errorf("shares are not supported in memory mode")
	}

	// Get share path and remove the directory
	sharePath, err := p.GetSharePath(name)
	if err != nil {
		return err
	}

	if err := os.RemoveAll(sharePath); err != nil {
		return fmt.Errorf("failed to remove share directory: %w", err)
	}

	// Remove from metadata
	return p.removeShare(name, identity)
}

// ListShares returns all configured shares
func (p *Persistence) ListShares() ([]Share, error) {
	if err := p.ensureInitialized(); err != nil {
		return nil, err
	}

	if p.isMemoryMode {
		return []Share{}, nil
	}

	config, err := p.loadSharesConfig()
	if err != nil {
		return nil, err
	}

	return config.Shares, nil
}

// GetSharePath returns the filesystem path to a share
func (p *Persistence) GetSharePath(name string) (string, error) {
	if err := p.ensureInitialized(); err != nil {
		return "", err
	}

	if p.isMemoryMode {
		return "", fmt.Errorf("shares are not supported in memory mode")
	}

	wt, err := p.repo.Worktree()
	if err != nil {
		return "", fmt.Errorf("failed to get worktree: %w", err)
	}

	sharePath := filepath.Join(wt.Filesystem.Root(), ".shares", name)
	if _, err := os.Stat(sharePath); os.IsNotExist(err) {
		return "", fmt.Errorf("share '%s' not found", name)
	}

	return sharePath, nil
}

// IsShare checks if a database name refers to a share
func (p *Persistence) IsShare(name string) bool {
	shares, err := p.ListShares()
	if err != nil {
		return false
	}
	for _, s := range shares {
		if s.Name == name {
			return true
		}
	}
	return false
}

// Helper functions

func (p *Persistence) sharesConfigPath() (string, error) {
	wt, err := p.repo.Worktree()
	if err != nil {
		return "", err
	}
	return filepath.Join(wt.Filesystem.Root(), ".commitdb", "shares.json"), nil
}

func (p *Persistence) loadSharesConfig() (SharesConfig, error) {
	// Read from git tree using ReadFileDirect to survive syncWorktree
	data, err := p.ReadFileDirect(".commitdb/shares.json")
	if err != nil {
		// File doesn't exist yet = empty config
		return SharesConfig{Shares: []Share{}}, nil
	}

	var config SharesConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return SharesConfig{}, fmt.Errorf("failed to parse shares config: %w", err)
	}

	return config, nil
}

func (p *Persistence) saveSharesConfig(config SharesConfig, identity core.Identity) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize shares config: %w", err)
	}

	// Write to git using WriteFileDirect to ensure it survives syncWorktree
	_, err = p.WriteFileDirect(".commitdb/shares.json", data, identity, "Update shares config")
	return err
}

func (p *Persistence) saveShare(share Share, identity core.Identity) error {
	config, err := p.loadSharesConfig()
	if err != nil {
		return err
	}

	config.Shares = append(config.Shares, share)
	return p.saveSharesConfig(config, identity)
}

func (p *Persistence) removeShare(name string, identity core.Identity) error {
	config, err := p.loadSharesConfig()
	if err != nil {
		return err
	}

	newShares := make([]Share, 0, len(config.Shares))
	for _, s := range config.Shares {
		if s.Name != name {
			newShares = append(newShares, s)
		}
	}
	config.Shares = newShares

	return p.saveSharesConfig(config, identity)
}

// OpenSharePersistence opens a read-only persistence for a share's repository
func (p *Persistence) OpenSharePersistence(shareName string) (*Persistence, error) {
	sharePath, err := p.GetSharePath(shareName)
	if err != nil {
		return nil, err
	}

	shareWt := osfs.New(sharePath)
	shareGitDir, err := shareWt.Chroot(".git")
	if err != nil {
		return nil, fmt.Errorf("failed to open share git directory: %w", err)
	}

	storer := filesystem.NewStorageWithOptions(
		shareGitDir,
		cache.NewObjectLRUDefault(),
		filesystem.Options{ExclusiveAccess: false}) // Read-only access

	shareRepo, err := git.Open(storer, shareWt)
	if err != nil {
		return nil, fmt.Errorf("failed to open share repository: %w", err)
	}

	return &Persistence{
		repo:         shareRepo,
		isMemoryMode: false,
	}, nil
}
