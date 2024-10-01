package main

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/go-redis/redis/v8"
)

var (
	ctx = context.Background()

	// Redis client for KeyDB
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // KeyDB server address
	})
)

func main() {
	// 1. Write data to KeyDB
	key := "user:1001"
	value := "John Doe"

	err := rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Fatalf("Error setting key in KeyDB: %v\n", err)
	}
	fmt.Printf("Data written to KeyDB: %s -> %s\n", key, value)

	// 2. Example SQL-like query to get data from KeyDB
	sqlQuery := "SELECT value FROM keydb WHERE key='user:1001'"

	// 3. Parse SQL-like query and retrieve data from KeyDB
	result, err := handleSQLQuery(sqlQuery)
	if err != nil {
		log.Fatalf("Error handling SQL query: %v\n", err)
	}
	fmt.Printf("Data retrieved: %s\n", result)
}

// handleSQLQuery parses a basic SQL query and retrieves data from KeyDB
func handleSQLQuery(query string) (string, error) {
	// Example: "SELECT value FROM keydb WHERE key='user:1001'"
	query = strings.TrimSpace(query)
	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		// Extract the key from the query
		start := strings.Index(query, "key='") + len("key='")
		end := strings.Index(query[start:], "'") + start

		if start == -1 || end == -1 || end <= start {
			return "", fmt.Errorf("invalid SQL query")
		}

		// Extracted key
		key := query[start:end]

		// Retrieve value from KeyDB using the key
		val, err := rdb.Get(ctx, key).Result()
		if err != nil {
			if err == redis.Nil {
				return "", fmt.Errorf("key not found")
			}
			return "", err
		}

		return val, nil
	}

	return "", fmt.Errorf("unsupported query")
}
