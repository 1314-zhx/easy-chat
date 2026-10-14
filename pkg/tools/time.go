package tools

import (
	"context"
	"time"
)

func handleGetTime(_ context.Context, _ map[string]string, _ string) (string, error) {
	return time.Now().Format("2006-01-02 15:04:05"), nil
}
