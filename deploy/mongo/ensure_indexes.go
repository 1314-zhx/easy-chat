package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"easy-chat/apps/im/immodels"
)

func main() {
	uri := os.Getenv("MONGO_URL")
	if uri == "" {
		uri = "mongodb://root:easy-chat@127.0.0.1:27017/?authSource=admin"
	}
	db := os.Getenv("MONGO_DB")
	if db == "" {
		db = "easy-chat"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if err := immodels.EnsureIndexes(ctx, uri, db); err != nil {
		fmt.Fprintf(os.Stderr, "ensure indexes: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("mongo indexes ok")
}
