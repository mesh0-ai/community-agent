package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// The golden fixtures are signed with a TEST-ONLY keypair committed under
// testdata/keys. Their tokens carry fixed iat/exp, and every fixture run
// verifies them against fixtureNow, so the files stay static.
const (
	fixtureIssuer   = "xano-community"
	fixtureAudience = "xano-community-agent"
	fixtureInstance = "test-instance"
)

var (
	fixtureIat = time.Date(2026, 10, 13, 9, 45, 0, 0, time.UTC)
	fixtureExp = fixtureIat.Add(time.Hour)
	// fixtureNow is the clock every fixture run uses.
	fixtureNow = time.Date(2026, 10, 13, 9, 50, 30, 0, time.UTC)
)

func testdataDir() string { return filepath.Join("..", "..", "testdata") }

func loadTestPrivateKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testdataDir(), "keys", "TEST_ONLY_ed25519_private.pem"))
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(b)
	k, err := x509.ParsePKCS8PrivateKey(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return k.(ed25519.PrivateKey)
}

func loadTestPublicKeyPEM(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(testdataDir(), "keys", "TEST_ONLY_ed25519_public.pem"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// signToken builds a compact JWS. The header is signed as given, so a test
// can sign a header with any alg.
func signToken(priv ed25519.PrivateKey, header, claims map[string]any) string {
	h, _ := json.Marshal(header)
	c, _ := json.Marshal(claims)
	input := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	sig := ed25519.Sign(priv, []byte(input))
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// validClaims are claims that verify for project at fixtureNow.
func validClaims(project string) map[string]any {
	return map[string]any{
		"iss":          fixtureIssuer,
		"aud":          fixtureAudience,
		"sub":          fixtureInstance,
		"m0/workspace": project,
		"iat":          fixtureIat.Unix(),
		"exp":          fixtureExp.Unix(),
	}
}

func edHeader() map[string]any { return map[string]any{"alg": "EdDSA", "typ": "JWT"} }

// fixtureVerifierOptions verifies fixture tokens at clock().
func fixtureVerifierOptions(t *testing.T, clock func() time.Time) VerifierOptions {
	t.Helper()
	pub, err := parsePublicKeyPEM(loadTestPublicKeyPEM(t))
	if err != nil {
		t.Fatal(err)
	}
	return VerifierOptions{
		PublicKey:    pub,
		Issuer:       fixtureIssuer,
		Audience:     fixtureAudience,
		InstanceName: fixtureInstance,
		ClockSkew:    60 * time.Second,
		Now:          clock,
	}
}
