package main

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackoff(t *testing.T) {
	tests := []struct {
		name   string
		exp    int
		expect float64
	}{
		{
			name:   "test backoff exp 0",
			exp:    0,
			expect: 1,
		},
		{
			name:   "test backoff exp 1",
			exp:    1,
			expect: 2,
		},
		{
			name:   "test backoff exp 2",
			exp:    2,
			expect: 4,
		},
	}

	for _, test := range tests {
		start := time.Now()
		backoff(test.exp, "unit-test")
		duration := time.Since(start).Seconds()

		cond := duration >= test.expect && duration <= test.expect*1.05
		assert.Truef(t, cond, "%d failed", test.name)
	}
}

func TestGetRoleArn(t *testing.T) {
	tests := []struct {
		name   string
		env    string
		key    string
		expect string
	}{
		{
			name:   "test dev",
			env:    "development",
			key:    "DEVELOPMENT_ROLE",
			expect: "arn:aws:iam::123456789:role/development-dynamodb-role",
		},
	}

	for _, test := range tests {
		err := os.Setenv(test.key, test.expect)
		require.NoErrorf(t, err, "%s: %v", test.name, err)

		actual := getRoleArn(test.env)
		assert.Equalf(t, test.expect, actual, "%s failed", test.name)
	}
}

func TestGetSession(t *testing.T) {
	tests := []struct {
		name       string
		region     string
		endpoint   string
		httpClient *http.Client
	}{
		{
			name:       "session with http client",
			region:     "us-west-2",
			endpoint:   "",
			httpClient: &http.Client{},
		},
		{
			name:       "session without http client",
			region:     "us-west-2",
			endpoint:   "http://localhost:8000",
			httpClient: nil,
		},
	}

	for _, test := range tests {
		sess := getSession(test.region, test.endpoint, test.httpClient)
		assert.Equalf(t, test.region, *sess.Config.Region, "%s differ in region", test.name)
		assert.Equalf(t, test.endpoint, *sess.Config.Endpoint, "%s differ in endpoint", test.name)
		assert.NotNil(t, sess.Config.HTTPClient, "%s httpClient is nil", test.name)
	}
}
