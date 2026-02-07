package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestJWKSClientGetKey(t *testing.T) {
	// Generate a test RSA key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	// Create mock JWKS endpoint
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"kid": "test-key-1",
				"use": "sig",
				"n":   base64.RawURLEncoding.EncodeToString(privateKey.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString([]byte{0x01, 0x00, 0x01}),
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	// Create JWKS client
	client := NewJWKSClient(server.URL)

	// Get the key
	key, err := client.GetKey("test-key-1")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}

	rsaKey, ok := key.(*rsa.PublicKey)
	if !ok {
		t.Fatalf("expected *rsa.PublicKey, got %T", key)
	}

	if rsaKey.N.Cmp(privateKey.N) != 0 {
		t.Error("RSA modulus mismatch")
	}
}

func TestJWKSClientCaching(t *testing.T) {
	fetchCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetchCount++
		jwks := map[string]interface{}{
			"keys": []map[string]interface{}{
				{
					"kty": "RSA",
					"kid": "cached-key",
					"use": "sig",
					"n":   "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
					"e":   "AQAB",
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	client := NewJWKSClient(server.URL)
	client.cacheTTL = 1 * time.Hour

	// First fetch
	_, err := client.GetKey("cached-key")
	if err != nil {
		t.Fatalf("first GetKey failed: %v", err)
	}

	// Second fetch should use cache
	_, err = client.GetKey("cached-key")
	if err != nil {
		t.Fatalf("second GetKey failed: %v", err)
	}

	// Should only have fetched once
	if fetchCount != 1 {
		t.Errorf("expected 1 fetch, got %d", fetchCount)
	}
}

func TestJWKSClientKeyNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		jwks := map[string]interface{}{
			"keys": []map[string]interface{}{},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	client := NewJWKSClient(server.URL)

	_, err := client.GetKey("nonexistent-key")
	if err == nil {
		t.Error("expected error for nonexistent key")
	}
}

func TestJWKSClientECKey(t *testing.T) {
	// Generate a test EC key
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate EC key: %v", err)
	}

	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "EC",
				"kid": "ec-key-1",
				"use": "sig",
				"crv": "P-256",
				"x":   base64.RawURLEncoding.EncodeToString(privateKey.X.Bytes()),
				"y":   base64.RawURLEncoding.EncodeToString(privateKey.Y.Bytes()),
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	client := NewJWKSClient(server.URL)

	key, err := client.GetKey("ec-key-1")
	if err != nil {
		t.Fatalf("GetKey failed: %v", err)
	}

	ecKey, ok := key.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("expected *ecdsa.PublicKey, got %T", key)
	}

	if ecKey.X.Cmp(privateKey.X) != 0 || ecKey.Y.Cmp(privateKey.Y) != 0 {
		t.Error("EC key coordinates mismatch")
	}
}

func TestValidateJWTWithJWKS(t *testing.T) {
	// Generate a test RSA key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	// Create mock JWKS endpoint
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{
			{
				"kty": "RSA",
				"kid": "jwt-test-key",
				"use": "sig",
				"n":   base64.RawURLEncoding.EncodeToString(privateKey.N.Bytes()),
				"e":   base64.RawURLEncoding.EncodeToString([]byte{0x01, 0x00, 0x01}),
			},
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	// Create a signed JWT
	claims := jwt.MapClaims{
		"name":  "Test User",
		"email": "test@example.com",
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["kid"] = "jwt-test-key"

	tokenString, err := token.SignedString(privateKey)
	if err != nil {
		t.Fatalf("failed to sign token: %v", err)
	}

	// Create server with JWKS client
	s := &Server{
		authConfig: &AuthConfig{
			Enabled: true,
			JWKSUrl: server.URL,
		},
		jwksClient: NewJWKSClient(server.URL),
	}

	// Validate the token
	result := s.validateJWT(tokenString)
	if result.err != nil {
		t.Fatalf("validateJWT failed: %v", result.err)
	}

	if result.identity.Name != "Test User" {
		t.Errorf("expected name 'Test User', got '%s'", result.identity.Name)
	}
	if result.identity.Email != "test@example.com" {
		t.Errorf("expected email 'test@example.com', got '%s'", result.identity.Email)
	}
}
