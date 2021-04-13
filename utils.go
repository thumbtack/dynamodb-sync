package main

import (
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
)

func backoff(exp int, caller string) {
	time.Sleep(time.Duration(1<<exp) * time.Second)
}

func getRoleArn(env string) string {
	roleKey := strings.ToUpper(env) + "_ROLE"
	logger.Debugf("role key: %s", roleKey)

	roleArn := os.Getenv(roleKey)
	if roleArn == "" {
		logger.Fatalf("failed to get role arn. please check the config")
	}
	return roleArn
}

func getSession(region, endpoint string, httpClient *http.Client) *session.Session {
	config := aws.NewConfig().
		WithRegion(region).
		WithEndpoint(endpoint).
		WithMaxRetries(maxRetries)

	if httpClient != nil {
		config = config.WithHTTPClient(httpClient)
	}

	return session.Must(session.NewSession(config))
}
