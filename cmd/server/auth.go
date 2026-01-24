// Package main provides authentication for the CommitDB TCP server.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/nickyhof/CommitDB/core"
)

// AuthConfig configures server authentication.
type AuthConfig struct {
	// Enabled enables authentication. If false, server uses default identity.
	Enabled bool

	// JWTSecret is the shared secret for HS256 JWT validation.
	// Either JWTSecret or JWKSUrl must be set if Enabled is true.
	JWTSecret string

	// JWKSUrl is the URL to fetch JSON Web Key Set for RS256/ES256 validation.
	JWKSUrl string

	// Issuer is the expected "iss" claim in JWTs.
	Issuer string

	// Audience is the expected "aud" claim in JWTs (optional).
	Audience string

	// NameClaim is the JWT claim for user's name (default: "name").
	NameClaim string

	// EmailClaim is the JWT claim for user's email (default: "email").
	EmailClaim string
}

// ConnectionState tracks per-connection authentication state.
type ConnectionState struct {
	identity      *core.Identity
	authenticated bool
	tokenExpiry   time.Time
}

// IsAuthenticated returns true if the connection has been authenticated.
func (cs *ConnectionState) IsAuthenticated() bool {
	return cs.authenticated
}

// Identity returns the connection's identity, or nil if not authenticated.
func (cs *ConnectionState) Identity() *core.Identity {
	return cs.identity
}

// authResult represents the result of an authentication attempt.
type authResult struct {
	identity  core.Identity
	expiresAt time.Time
	err       error
}

// validateJWT validates a JWT token and extracts identity claims.
func (s *Server) validateJWT(tokenString string) authResult {
	if s.authConfig == nil {
		return authResult{err: errors.New("authentication not configured")}
	}

	// Determine name and email claims
	nameClaim := s.authConfig.NameClaim
	if nameClaim == "" {
		nameClaim = "name"
	}
	emailClaim := s.authConfig.EmailClaim
	if emailClaim == "" {
		emailClaim = "email"
	}

	// Parse and validate the token
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		// Validate signing method
		if s.authConfig.JWTSecret != "" {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return []byte(s.authConfig.JWTSecret), nil
		}

		// TODO: JWKS-based validation for RS256/ES256
		return nil, errors.New("no JWT secret configured")
	}, jwt.WithValidMethods([]string{"HS256", "HS384", "HS512"}))

	if err != nil {
		return authResult{err: fmt.Errorf("invalid token: %w", err)}
	}

	if !token.Valid {
		return authResult{err: errors.New("invalid token")}
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return authResult{err: errors.New("invalid token claims")}
	}

	// Validate issuer if configured
	if s.authConfig.Issuer != "" {
		issuer, _ := claims.GetIssuer()
		if issuer != s.authConfig.Issuer {
			return authResult{err: fmt.Errorf("invalid issuer: expected %s, got %s", s.authConfig.Issuer, issuer)}
		}
	}

	// Validate audience if configured
	if s.authConfig.Audience != "" {
		audiences, _ := claims.GetAudience()
		found := false
		for _, aud := range audiences {
			if aud == s.authConfig.Audience {
				found = true
				break
			}
		}
		if !found {
			return authResult{err: fmt.Errorf("invalid audience: expected %s", s.authConfig.Audience)}
		}
	}

	// Extract identity claims
	name, _ := claims[nameClaim].(string)
	email, _ := claims[emailClaim].(string)

	if name == "" && email == "" {
		return authResult{err: fmt.Errorf("token missing identity claims (%s or %s)", nameClaim, emailClaim)}
	}

	// Get expiration
	var expiresAt time.Time
	if exp, err := claims.GetExpirationTime(); err == nil && exp != nil {
		expiresAt = exp.Time
	}

	return authResult{
		identity: core.Identity{
			Name:  name,
			Email: email,
		},
		expiresAt: expiresAt,
	}
}

// parseAuthCommand parses an AUTH command and returns the auth type and token.
// Supported formats:
//   - AUTH JWT <token>
func parseAuthCommand(line string) (authType, token string, err error) {
	line = strings.TrimSpace(line)

	// Check for AUTH prefix (case-insensitive)
	if !strings.HasPrefix(strings.ToUpper(line), "AUTH ") {
		return "", "", errors.New("not an AUTH command")
	}

	parts := strings.Fields(line)
	if len(parts) < 3 {
		return "", "", errors.New("invalid AUTH command: expected AUTH <type> <credentials>")
	}

	authType = strings.ToUpper(parts[1])
	token = parts[2]

	switch authType {
	case "JWT":
		return authType, token, nil
	default:
		return "", "", fmt.Errorf("unsupported auth type: %s", authType)
	}
}

// handleAuth processes an AUTH command and returns the response.
func (s *Server) handleAuth(line string, state *ConnectionState) Response {
	authType, token, err := parseAuthCommand(line)
	if err != nil {
		return Response{
			Success: false,
			Type:    "auth",
			Error:   err.Error(),
		}
	}

	switch authType {
	case "JWT":
		result := s.validateJWT(token)
		if result.err != nil {
			return Response{
				Success: false,
				Type:    "auth",
				Error:   result.err.Error(),
			}
		}

		state.identity = &result.identity
		state.authenticated = true
		state.tokenExpiry = result.expiresAt

		ar := AuthResponse{
			Authenticated: true,
			Identity:      fmt.Sprintf("%s <%s>", result.identity.Name, result.identity.Email),
		}
		if !result.expiresAt.IsZero() {
			ar.ExpiresIn = int(time.Until(result.expiresAt).Seconds())
		}

		data, _ := json.Marshal(ar)
		return Response{
			Success: true,
			Type:    "auth",
			Result:  data,
		}

	default:
		return Response{
			Success: false,
			Type:    "auth",
			Error:   fmt.Sprintf("unsupported auth type: %s", authType),
		}
	}
}
