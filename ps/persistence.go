package ps

import (
	"errors"
	"os"
	"sync"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-billy/v6/osfs"
	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/plumbing/cache"
	"github.com/go-git/go-git/v6/storage/filesystem"
	"github.com/go-git/go-git/v6/storage/memory"
)

var (
	ErrNotInitialized = errors.New("persistence layer not initialized")
	ErrRepoNotFound   = errors.New("repository not found")
)

type Persistence struct {
	repo         *git.Repository
	mu           sync.RWMutex
	pendingMerge *PendingMerge // For manual conflict resolution
}

// IsInitialized returns true if the persistence layer has a valid repository
func (p *Persistence) IsInitialized() bool {
	return p != nil && p.repo != nil
}

// ensureInitialized checks if the persistence layer is initialized and returns an error if not
func (p *Persistence) ensureInitialized() error {
	if !p.IsInitialized() {
		return ErrNotInitialized
	}
	return nil
}

// RLock acquires a read lock for concurrent read operations
func (p *Persistence) RLock() {
	p.mu.RLock()
}

// RUnlock releases the read lock
func (p *Persistence) RUnlock() {
	p.mu.RUnlock()
}

// Lock acquires a write lock for exclusive write operations
func (p *Persistence) Lock() {
	p.mu.Lock()
}

// Unlock releases the write lock
func (p *Persistence) Unlock() {
	p.mu.Unlock()
}

func NewMemoryPersistence() (Persistence, error) {
	wt := memfs.New()
	storer := memory.NewStorage()

	repo, err := git.Init(storer, git.WithWorkTree(wt))
	if err != nil {
		return Persistence{}, err
	}

	return Persistence{
		repo: repo,
	}, nil
}

func NewFilePersistence(baseDir string, gitUrl *string) (Persistence, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return Persistence{}, err
	}

	wt := osfs.New(baseDir)
	fs, err := wt.Chroot(".git")
	if err != nil {
		return Persistence{}, err
	}

	storer := filesystem.NewStorageWithOptions(
		fs,
		cache.NewObjectLRUDefault(),
		filesystem.Options{ExclusiveAccess: true})

	var repo *git.Repository

	if gitUrl != nil {
		repo, err = git.Clone(storer, wt, &git.CloneOptions{
			URL: *gitUrl,
		})
		if err != nil {
			return Persistence{}, err
		}
	} else {
		_, statErr := os.Stat(fs.Root())
		if statErr != nil {
			// Directory doesn't exist, initialize new repo
			repo, err = git.Init(storer, git.WithWorkTree(wt))
			if err != nil {
				return Persistence{}, err
			}
		} else {
			// Directory exists, open existing repo
			repo, err = git.Open(storer, wt)
			if err != nil {
				return Persistence{}, err
			}
		}
	}

	return Persistence{
		repo: repo,
	}, nil
}
