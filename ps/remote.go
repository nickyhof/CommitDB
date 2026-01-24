package ps

import (
	"fmt"
	"os"

	"github.com/go-git/go-git/v6"
	"github.com/go-git/go-git/v6/config"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/plumbing/transport/http"
	"github.com/go-git/go-git/v6/plumbing/transport/ssh"
)

// AuthType defines the type of authentication
type AuthType string

const (
	AuthTypeNone  AuthType = "none"
	AuthTypeToken AuthType = "token"
	AuthTypeSSH   AuthType = "ssh"
	AuthTypeBasic AuthType = "basic"
)

// RemoteAuth holds authentication configuration for remote operations
type RemoteAuth struct {
	Type       AuthType
	Token      string // For token auth
	KeyPath    string // For SSH key auth
	Passphrase string // For SSH key with passphrase
	Username   string // For basic auth
	Password   string // For basic auth
}

// Remote represents a Git remote
type Remote struct {
	Name string
	URLs []string
}

// getAuthMethod converts RemoteAuth to go-git's AuthMethod
func (auth *RemoteAuth) getAuthMethod() (transport.AuthMethod, error) {
	if auth == nil {
		return nil, nil
	}

	switch auth.Type {
	case AuthTypeNone:
		return nil, nil

	case AuthTypeToken:
		// Token auth uses username "git" or any non-empty string
		return &http.BasicAuth{
			Username: "git",
			Password: auth.Token,
		}, nil

	case AuthTypeSSH:
		keyPath := auth.KeyPath
		if keyPath == "" {
			// Default to ~/.ssh/id_rsa
			home, _ := os.UserHomeDir()
			keyPath = home + "/.ssh/id_rsa"
		}

		if auth.Passphrase != "" {
			return ssh.NewPublicKeysFromFile("git", keyPath, auth.Passphrase)
		}
		return ssh.NewPublicKeysFromFile("git", keyPath, "")

	case AuthTypeBasic:
		return &http.BasicAuth{
			Username: auth.Username,
			Password: auth.Password,
		}, nil

	default:
		return nil, fmt.Errorf("unknown auth type: %s", auth.Type)
	}
}

// AddRemote adds a named remote to the repository
func (p *Persistence) AddRemote(name, url string) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	_, err := p.repo.CreateRemote(&config.RemoteConfig{
		Name: name,
		URLs: []string{url},
	})
	if err != nil {
		return fmt.Errorf("failed to add remote '%s': %w", name, err)
	}
	return nil
}

// ListRemotes returns all configured remotes
func (p *Persistence) ListRemotes() ([]Remote, error) {
	if err := p.ensureInitialized(); err != nil {
		return nil, err
	}

	remotes, err := p.repo.Remotes()
	if err != nil {
		return nil, fmt.Errorf("failed to list remotes: %w", err)
	}

	result := make([]Remote, len(remotes))
	for i, r := range remotes {
		cfg := r.Config()
		result[i] = Remote{
			Name: cfg.Name,
			URLs: cfg.URLs,
		}
	}
	return result, nil
}

// RemoveRemote removes a remote from the repository
func (p *Persistence) RemoveRemote(name string) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	err := p.repo.DeleteRemote(name)
	if err != nil {
		return fmt.Errorf("failed to remove remote '%s': %w", name, err)
	}
	return nil
}

// Push pushes a branch to a remote
func (p *Persistence) Push(remoteName, branch string, auth *RemoteAuth) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	// Default to origin if not specified
	if remoteName == "" {
		remoteName = "origin"
	}

	// Default to current branch if not specified
	if branch == "" {
		currentBranch, err := p.CurrentBranch()
		if err != nil {
			return fmt.Errorf("failed to get current branch: %w", err)
		}
		branch = currentBranch
	}

	authMethod, err := auth.getAuthMethod()
	if err != nil {
		return fmt.Errorf("failed to configure auth: %w", err)
	}

	refSpec := config.RefSpec(fmt.Sprintf("refs/heads/%s:refs/heads/%s", branch, branch))

	err = p.repo.Push(&git.PushOptions{
		RemoteName: remoteName,
		RefSpecs:   []config.RefSpec{refSpec},
		Auth:       authMethod,
	})

	if err == git.NoErrAlreadyUpToDate {
		return nil // Not an error
	}
	if err != nil {
		return fmt.Errorf("failed to push to '%s': %w", remoteName, err)
	}
	return nil
}

// Pull pulls changes from a remote and merges into current branch
func (p *Persistence) Pull(remoteName, branch string, auth *RemoteAuth) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	// Default to origin if not specified
	if remoteName == "" {
		remoteName = "origin"
	}

	wt, err := p.repo.Worktree()
	if err != nil {
		return fmt.Errorf("failed to get worktree: %w", err)
	}

	authMethod, err := auth.getAuthMethod()
	if err != nil {
		return fmt.Errorf("failed to configure auth: %w", err)
	}

	pullOpts := &git.PullOptions{
		RemoteName: remoteName,
		Auth:       authMethod,
	}

	// If branch specified, set the reference
	if branch != "" {
		pullOpts.ReferenceName = plumbing.ReferenceName(fmt.Sprintf("refs/heads/%s", branch))
	}

	err = wt.Pull(pullOpts)
	if err == git.NoErrAlreadyUpToDate {
		return nil // Not an error
	}
	if err != nil {
		return fmt.Errorf("failed to pull from '%s': %w", remoteName, err)
	}
	return nil
}

// Fetch fetches refs from a remote without merging
func (p *Persistence) Fetch(remoteName string, auth *RemoteAuth) error {
	if err := p.ensureInitialized(); err != nil {
		return err
	}

	// Default to origin if not specified
	if remoteName == "" {
		remoteName = "origin"
	}

	authMethod, err := auth.getAuthMethod()
	if err != nil {
		return fmt.Errorf("failed to configure auth: %w", err)
	}

	err = p.repo.Fetch(&git.FetchOptions{
		RemoteName: remoteName,
		Auth:       authMethod,
	})

	if err == git.NoErrAlreadyUpToDate {
		return nil // Not an error
	}
	if err != nil {
		return fmt.Errorf("failed to fetch from '%s': %w", remoteName, err)
	}
	return nil
}
