package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"strings"
)

// Token is v1.<base64url(userID)>.<base64url(hmac-sha256(secret, userID))>.
// Identity is derived only from a verified token, never from a request body.

func Issue(secret, userID string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(userID))
	sum := mac.Sum(nil)
	return "v1." + base64.RawURLEncoding.EncodeToString([]byte(userID)) + "." + base64.RawURLEncoding.EncodeToString(sum)
}

func Verify(secret, token string) (string, bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] != "v1" {
		return "", false
	}
	uid, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(uid) == 0 || len(uid) > 128 {
		return "", false
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(uid)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return "", false
	}
	return string(uid), true
}

// Admin reports whether token is the configured admin bearer.
// Comparison is constant-time when lengths match; mismatched lengths are not admin.
func Admin(adminToken, token string) bool {
	if adminToken == "" || token == "" {
		return false
	}
	return hmac.Equal([]byte(adminToken), []byte(token))
}
