package main

import (
	"encoding/base64"
	"strings"
	"testing"
	"time"
)

func newFixtureVerifier(t *testing.T, now *time.Time) *Verifier {
	t.Helper()
	v, err := NewVerifier(fixtureVerifierOptions(t, func() time.Time { return *now }))
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestVerify_AcceptsMatchingSubAndProject(t *testing.T) {
	now := fixtureNow
	v := newFixtureVerifier(t, &now)
	tok := signToken(loadTestPrivateKey(t), edHeader(), validClaims("workspace-7"))
	if err := v.Verify(tok, "workspace-7"); err != nil {
		t.Fatalf("valid token rejected: %v", err)
	}
}

func TestVerify_Rejections(t *testing.T) {
	priv := loadTestPrivateKey(t)
	with := func(k string, val any) map[string]any {
		c := validClaims("workspace-7")
		c[k] = val
		return c
	}
	tampered := func() string {
		tok := signToken(priv, edHeader(), validClaims("workspace-7"))
		parts := strings.Split(tok, ".")
		sig, _ := base64.RawURLEncoding.DecodeString(parts[2])
		sig[0] ^= 0xff
		return parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(sig)
	}
	algNone := func() string {
		tok := signToken(priv, map[string]any{"alg": "none"}, validClaims("workspace-7"))
		parts := strings.Split(tok, ".")
		return parts[0] + "." + parts[1] + "."
	}
	cases := map[string]string{
		"workspace differs from _project": signToken(priv, edHeader(), with("m0/workspace", "workspace-8")),
		"sub is another instance":         signToken(priv, edHeader(), with("sub", "other-instance")),
		"expired beyond skew":             signToken(priv, edHeader(), with("exp", fixtureNow.Add(-61*time.Second).Unix())),
		"wrong aud":                       signToken(priv, edHeader(), with("aud", "xano-community-other")),
		"wrong iss":                       signToken(priv, edHeader(), with("iss", "someone-else")),
		"lifetime over 24h":               signToken(priv, edHeader(), with("exp", fixtureIat.Add(24*time.Hour+time.Second).Unix())),
		"alg none":                        algNone(),
		"alg HS256":                       signToken(priv, map[string]any{"alg": "HS256"}, validClaims("workspace-7")),
		"tampered signature":              tampered(),
		"two segments":                    "abc.def",
	}
	for name, tok := range cases {
		t.Run(name, func(t *testing.T) {
			now := fixtureNow
			v := newFixtureVerifier(t, &now)
			if err := v.Verify(tok, "workspace-7"); err == nil {
				t.Fatal("token accepted")
			}
		})
	}
}

func TestVerify_AlgNoneRejectedBeforeSignatureWork(t *testing.T) {
	now := fixtureNow
	v := newFixtureVerifier(t, &now)
	tok := signToken(loadTestPrivateKey(t), map[string]any{"alg": "none"}, validClaims("workspace-7"))
	parts := strings.Split(tok, ".")
	_ = v.Verify(parts[0]+"."+parts[1]+".", "workspace-7")
	if got := v.signatureChecks; got != 0 {
		t.Fatalf("signature checks = %d, want 0", got)
	}
}

func TestVerify_CacheHitSkipsSignatureWork(t *testing.T) {
	now := fixtureNow
	v := newFixtureVerifier(t, &now)
	tok := signToken(loadTestPrivateKey(t), edHeader(), validClaims("workspace-7"))
	for i := 0; i < 2; i++ {
		if err := v.Verify(tok, "workspace-7"); err != nil {
			t.Fatalf("verify %d: %v", i, err)
		}
	}
	if v.CacheMisses.Load() != 1 || v.CacheHits.Load() != 1 {
		t.Fatalf("misses=%d hits=%d, want 1/1", v.CacheMisses.Load(), v.CacheHits.Load())
	}
	if v.signatureChecks != 1 {
		t.Fatalf("signature checks = %d, want 1", v.signatureChecks)
	}
}

func TestVerify_CacheHitStillChecksProjectAndExpiry(t *testing.T) {
	now := fixtureNow
	v := newFixtureVerifier(t, &now)
	tok := signToken(loadTestPrivateKey(t), edHeader(), validClaims("workspace-7"))
	if err := v.Verify(tok, "workspace-7"); err != nil {
		t.Fatal(err)
	}
	if err := v.Verify(tok, "workspace-8"); err == nil {
		t.Fatal("cached token accepted for another _project")
	}
	now = fixtureExp.Add(61 * time.Second)
	if err := v.Verify(tok, "workspace-7"); err == nil {
		t.Fatal("cached token accepted after exp + skew")
	}
	if v.signatureChecks != 1 {
		t.Fatalf("signature checks = %d, want 1", v.signatureChecks)
	}
}
