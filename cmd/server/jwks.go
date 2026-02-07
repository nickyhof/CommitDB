// JWKS (JSON Web Key Set) client for RS256/ES256 JWT validation.
package main

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// JWKSClient fetches and caches public keys from a JWKS endpoint.
type JWKSClient struct {
	url       string
	keys      map[string]crypto.PublicKey // kid -> key
	mutex     sync.RWMutex
	lastFetch time.Time
	cacheTTL  time.Duration
	client    *http.Client
}

// jwksResponse represents the JSON response from a JWKS endpoint.
type jwksResponse struct {
	Keys []jwk `json:"keys"`
}

// jwk represents a single JSON Web Key.
type jwk struct {
	Kty string `json:"kty"` // Key type: RSA or EC
	Kid string `json:"kid"` // Key ID
	Use string `json:"use"` // Usage: sig or enc
	Alg string `json:"alg"` // Algorithm: RS256, ES256, etc.

	// RSA parameters
	N string `json:"n"` // Modulus
	E string `json:"e"` // Exponent

	// EC parameters
	Crv string `json:"crv"` // Curve: P-256, P-384, P-521
	X   string `json:"x"`   // X coordinate
	Y   string `json:"y"`   // Y coordinate
}

// NewJWKSClient creates a new JWKS client with the given endpoint URL.
func NewJWKSClient(url string) *JWKSClient {
	return &JWKSClient{
		url:      url,
		keys:     make(map[string]crypto.PublicKey),
		cacheTTL: 1 * time.Hour,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// GetKey retrieves a public key by key ID (kid).
// If the key is not cached or cache is stale, it fetches from the JWKS endpoint.
func (c *JWKSClient) GetKey(kid string) (crypto.PublicKey, error) {
	// Try cached key first
	c.mutex.RLock()
	key, exists := c.keys[kid]
	cacheValid := time.Since(c.lastFetch) < c.cacheTTL
	c.mutex.RUnlock()

	if exists && cacheValid {
		return key, nil
	}

	// Refresh keys
	if err := c.refreshKeys(); err != nil {
		return nil, err
	}

	// Try again after refresh
	c.mutex.RLock()
	key, exists = c.keys[kid]
	c.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("key not found: %s", kid)
	}

	return key, nil
}

// refreshKeys fetches the JWKS from the endpoint and updates the cache.
func (c *JWKSClient) refreshKeys() error {
	resp, err := c.client.Get(c.url)
	if err != nil {
		return fmt.Errorf("failed to fetch JWKS: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned status %d", resp.StatusCode)
	}

	var jwks jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return fmt.Errorf("failed to decode JWKS: %w", err)
	}

	newKeys := make(map[string]crypto.PublicKey)
	for _, key := range jwks.Keys {
		// Only process signing keys
		if key.Use != "" && key.Use != "sig" {
			continue
		}

		pubKey, err := parseJWK(key)
		if err != nil {
			continue // Skip invalid keys
		}

		newKeys[key.Kid] = pubKey
	}

	c.mutex.Lock()
	c.keys = newKeys
	c.lastFetch = time.Now()
	c.mutex.Unlock()

	return nil
}

// parseJWK converts a JWK to a crypto.PublicKey.
func parseJWK(key jwk) (crypto.PublicKey, error) {
	switch key.Kty {
	case "RSA":
		return parseRSAKey(key)
	case "EC":
		return parseECKey(key)
	default:
		return nil, fmt.Errorf("unsupported key type: %s", key.Kty)
	}
}

// parseRSAKey parses an RSA public key from JWK.
func parseRSAKey(key jwk) (*rsa.PublicKey, error) {
	if key.N == "" || key.E == "" {
		return nil, errors.New("missing RSA key parameters")
	}

	nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
	if err != nil {
		return nil, fmt.Errorf("invalid modulus: %w", err)
	}

	eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
	if err != nil {
		return nil, fmt.Errorf("invalid exponent: %w", err)
	}

	n := new(big.Int).SetBytes(nBytes)
	e := 0
	for _, b := range eBytes {
		e = e<<8 + int(b)
	}

	return &rsa.PublicKey{N: n, E: e}, nil
}

// parseECKey parses an ECDSA public key from JWK.
func parseECKey(key jwk) (*ecdsa.PublicKey, error) {
	if key.X == "" || key.Y == "" || key.Crv == "" {
		return nil, errors.New("missing EC key parameters")
	}

	xBytes, err := base64.RawURLEncoding.DecodeString(key.X)
	if err != nil {
		return nil, fmt.Errorf("invalid X coordinate: %w", err)
	}

	yBytes, err := base64.RawURLEncoding.DecodeString(key.Y)
	if err != nil {
		return nil, fmt.Errorf("invalid Y coordinate: %w", err)
	}

	curve, err := getCurve(key.Crv)
	if err != nil {
		return nil, err
	}

	return &ecdsa.PublicKey{
		Curve: curve,
		X:     new(big.Int).SetBytes(xBytes),
		Y:     new(big.Int).SetBytes(yBytes),
	}, nil
}

// getCurve returns the elliptic curve for the given curve name.
func getCurve(crv string) (elliptic.Curve, error) {
	switch crv {
	case "P-256":
		return elliptic.P256(), nil
	case "P-384":
		return elliptic.P384(), nil
	case "P-521":
		return elliptic.P521(), nil
	default:
		return nil, fmt.Errorf("unsupported curve: %s", crv)
	}
}
