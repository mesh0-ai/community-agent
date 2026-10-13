package main

// verify.go checks the `_token` on each datagram (contract section 3).
//
// Ported from mesh0 core's backend/internal/auth/instancetoken.go
// (VerifyInstanceToken). Hand-rolled on crypto/ed25519 for the same reason:
// one token shape, one pinned algorithm, and the security properties
// readable top to bottom. The order of operations is the security property:
//
//  1. exactly three segments
//  2. decode the header; `alg` must be EdDSA. `none` and everything else
//     is rejected BEFORE any signature work
//  3. Ed25519 over the raw `header.claims` bytes (constant-time)
//  4. only now decode the claims: iss, aud, exp/iat within skew,
//     exp - iat <= 24h, then the two community pins:
//     sub == instance name, m0/workspace == the datagram's `_project`
//
// Reordering 2/3/4 is a vulnerability, not a refactor.
//
// Community-specific additions over the core verifier: the `sub` pin (every
// community instance shares one signing key, so a valid signature alone does
// not say which instance minted it), the `_project` pin, and a cache of
// verified tokens keyed by sha256(token) so a token is signature-checked once
// per lifetime rather than once per datagram.

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

// maxTokenTTL bounds exp - iat. A security control, not a tunable.
const maxTokenTTL = 24 * time.Hour

// maxCachedTokens bounds the verified-token cache. Expired entries are swept
// when it fills; if it is still full the cache is cleared (the next datagram
// per token pays one signature check again).
const maxCachedTokens = 4096

// VerifierOptions is the verifier's configuration.
type VerifierOptions struct {
	PublicKey    ed25519.PublicKey
	Issuer       string
	Audience     string
	InstanceName string
	ClockSkew    time.Duration
	// Now is the clock; nil means time.Now. Injected so fixture tokens with
	// fixed iat/exp verify deterministically.
	Now func() time.Time
}

// Verifier verifies tokens and caches the verified ones. Not safe for
// concurrent Verify calls: the listener's single read goroutine owns it.
// The counters are atomics so /stats can read them.
type Verifier struct {
	opts  VerifierOptions
	cache map[[32]byte]cachedToken

	CacheHits   atomic.Uint64
	CacheMisses atomic.Uint64

	// signatureChecks counts ed25519.Verify calls (tests only).
	signatureChecks int
}

type cachedToken struct {
	exp       time.Time
	workspace string
}

// NewVerifier validates the options.
func NewVerifier(o VerifierOptions) (*Verifier, error) {
	if len(o.PublicKey) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("community public key must be a %d-byte Ed25519 key", ed25519.PublicKeySize)
	}
	if o.Issuer == "" || o.Audience == "" || o.InstanceName == "" {
		return nil, errors.New("issuer, audience and instance name are all required")
	}
	if o.ClockSkew < 0 {
		return nil, errors.New("clock skew must not be negative")
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return &Verifier{opts: o, cache: map[[32]byte]cachedToken{}}, nil
}

// TokenError is a rejected token. Detail never echoes token content.
type TokenError struct{ Detail string }

func (e *TokenError) Error() string { return "bad token: " + e.Detail }

func reject(format string, args ...any) error {
	return &TokenError{Detail: fmt.Sprintf(format, args...)}
}

// Verify reports whether token authorizes a datagram whose `_project` is
// project. A cache hit skips steps 1-4 but still re-checks exp and the
// `_project` pin.
func (v *Verifier) Verify(token, project string) error {
	now := v.opts.Now()
	key := sha256.Sum256([]byte(token))
	if c, ok := v.cache[key]; ok {
		v.CacheHits.Add(1)
		if now.After(c.exp.Add(v.opts.ClockSkew)) {
			delete(v.cache, key)
			return reject("token expired")
		}
		if c.workspace != project {
			return reject("m0/workspace does not match _project")
		}
		return nil
	}
	v.CacheMisses.Add(1)
	exp, workspace, err := v.verifyFull(token, now)
	if err != nil {
		return err
	}
	// Cached even when this datagram's `_project` disagrees: the token
	// itself verified, and the pin is re-checked on every hit.
	v.store(key, cachedToken{exp: exp, workspace: workspace}, now)
	if workspace != project {
		return reject("m0/workspace does not match _project")
	}
	return nil
}

func (v *Verifier) store(key [32]byte, c cachedToken, now time.Time) {
	if len(v.cache) >= maxCachedTokens {
		for k, e := range v.cache {
			if now.After(e.exp.Add(v.opts.ClockSkew)) {
				delete(v.cache, k)
			}
		}
		if len(v.cache) >= maxCachedTokens {
			v.cache = map[[32]byte]cachedToken{}
		}
	}
	v.cache[key] = c
}

type tokenHeader struct {
	Alg string `json:"alg"`
}

type tokenClaims struct {
	Issuer    string   `json:"iss"`
	Subject   string   `json:"sub"`
	Audience  audience `json:"aud"`
	Workspace *string  `json:"m0/workspace"`
	IssuedAt  *float64 `json:"iat"`
	ExpiresAt *float64 `json:"exp"`
}

// verifyFull runs steps 1-4 and returns the token's exp and m0/workspace.
func (v *Verifier) verifyFull(token string, now time.Time) (time.Time, string, error) {
	// 1. Exactly three segments (Split, not SplitN: a JWE-shaped string
	//    must not be truncated into something that parses).
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, "", reject("expected three segments, got %d", len(parts))
	}

	// 2. Pin the algorithm before any signature work. `alg` is compared
	//    against EdDSA; it never selects a verifier.
	var h tokenHeader
	if err := decodeSegment(parts[0], &h); err != nil {
		return time.Time{}, "", reject("header: %v", err)
	}
	if strings.EqualFold(h.Alg, "none") {
		return time.Time{}, "", reject("unsigned token (alg=none)")
	}
	if h.Alg != "EdDSA" {
		return time.Time{}, "", reject("alg %q is not EdDSA", h.Alg)
	}

	// 3. Signature over the raw "<header>.<claims>" bytes as received.
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return time.Time{}, "", reject("signature is not base64url")
	}
	v.signatureChecks++
	if !ed25519.Verify(v.opts.PublicKey, []byte(token[:len(parts[0])+1+len(parts[1])]), sig) {
		return time.Time{}, "", reject("signature does not verify")
	}

	// 4. Claims are trusted only from here on.
	var c tokenClaims
	if err := decodeSegment(parts[1], &c); err != nil {
		return time.Time{}, "", reject("claims: %v", err)
	}
	if c.Issuer != v.opts.Issuer {
		return time.Time{}, "", reject("iss mismatch")
	}
	if !c.Audience.contains(v.opts.Audience) {
		return time.Time{}, "", reject("aud mismatch")
	}
	if c.ExpiresAt == nil || c.IssuedAt == nil {
		return time.Time{}, "", reject("iat and exp are required")
	}
	exp := time.Unix(int64(*c.ExpiresAt), 0).UTC()
	iat := time.Unix(int64(*c.IssuedAt), 0).UTC()
	skew := v.opts.ClockSkew
	if now.After(exp.Add(skew)) {
		return time.Time{}, "", reject("expired")
	}
	if now.Before(iat.Add(-skew)) {
		return time.Time{}, "", reject("iat is in the future")
	}
	if exp.Sub(iat) > maxTokenTTL {
		return time.Time{}, "", reject("exp - iat exceeds 24h")
	}
	if c.Subject != v.opts.InstanceName {
		return time.Time{}, "", reject("sub is not this instance")
	}
	if c.Workspace == nil {
		return time.Time{}, "", reject("m0/workspace missing")
	}
	return exp, *c.Workspace, nil
}

// audience decodes RFC 7519 `aud`: a string or an array of strings.
type audience []string

func (a *audience) UnmarshalJSON(b []byte) error {
	var one string
	if err := json.Unmarshal(b, &one); err == nil {
		*a = audience{one}
		return nil
	}
	var many []string
	if err := json.Unmarshal(b, &many); err != nil {
		return errors.New("aud must be a string or an array of strings")
	}
	*a = many
	return nil
}

func (a audience) contains(want string) bool {
	for _, got := range a {
		if got == want {
			return true
		}
	}
	return false
}

// decodeSegment base64url-decodes (unpadded only) a JSON object segment.
func decodeSegment(seg string, dst any) error {
	raw, err := base64.RawURLEncoding.DecodeString(seg)
	if err != nil {
		return errors.New("not unpadded base64url")
	}
	if t := bytes.TrimLeft(raw, " \t\r\n"); len(t) == 0 || t[0] != '{' {
		return errors.New("not a JSON object")
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return errors.New("unexpected field types")
	}
	return nil
}

// parsePublicKeyPEM reads an Ed25519 public key from PEM (SPKI).
func parsePublicKeyPEM(s string) (ed25519.PublicKey, error) {
	blk, _ := pem.Decode([]byte(s))
	if blk == nil {
		return nil, errors.New("MESH0_COMMUNITY_PUBLIC_KEY is not PEM")
	}
	k, err := x509.ParsePKIXPublicKey(blk.Bytes)
	if err != nil {
		return nil, fmt.Errorf("MESH0_COMMUNITY_PUBLIC_KEY: %w", err)
	}
	pub, ok := k.(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("MESH0_COMMUNITY_PUBLIC_KEY is not an Ed25519 key")
	}
	return pub, nil
}
