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
	// Complex data write: Store user profile in KeyDB (as a Redis HASH)
	userKey := "user:1001"
	userData := map[string]interface{}{
		"name":    "John Doe",
		"email":   "john.doe@example.com",
		"age":     "30",
		"country": "USA",
	}

	err := rdb.HMSet(ctx, userKey, userData).Err()
	if err != nil {
		log.Fatalf("Error writing complex data to KeyDB: %v\n", err)
	}
	fmt.Printf("Complex data written to KeyDB: %s -> %v\n", userKey, userData)

	// Complex SQL-like query to retrieve data
	//sqlQuery := "SELECT name, email FROM keydb WHERE key='user:1001' AND country='USA'"
	sqlQuery := "SELECT name, email FROM keydb WHERE country='USA'"

	// Parse SQL-like query and retrieve data from KeyDB
	result, err := handleSQLQuery(sqlQuery)
	if err != nil {
		log.Fatalf("Error handling SQL query: %v\n", err)
	}
	fmt.Printf("Data retrieved: %s\n", result)
}

func handleSQLQuery(query string) (string, error) {
	query = strings.TrimSpace(query)

	fmt.Printf("Received SQL query: %s\n", query)

	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		// You already know the key (user:1001), no need to extract it from the query

		key := "user:1001"

		// Debugging: Check if the key exists
		exists, err := rdb.Exists(ctx, key).Result()
		if err != nil {
			return "", fmt.Errorf("error checking if key exists: %v", err)
		}
		if exists == 0 {
			return "", fmt.Errorf("key '%s' not found", key)
		}
		fmt.Printf("Key '%s' exists\n", key)

		// Extract the fields to select
		selectStart := len("SELECT ")
		selectEnd := strings.Index(query[selectStart:], " FROM ") + selectStart
		if selectEnd == -1 {
			return "", fmt.Errorf("invalid SQL query")
		}
		fields := strings.Split(strings.TrimSpace(query[selectStart:selectEnd]), ",")
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}

		// Extract any additional conditions (e.g., WHERE clauses)
		whereStart := strings.Index(query, "WHERE")
		var conditions map[string]string
		if whereStart != -1 {
			conditions = parseWhereClause(query[whereStart:])
		}

		// Check conditions (e.g., `country='USA'`)
		if len(conditions) > 0 {
			for field, expectedVal := range conditions {
				actualVal, err := rdb.HGet(ctx, key, field).Result()
				if err != nil {
					if err == redis.Nil {
						return "", fmt.Errorf("field '%s' not found for key '%s'", field, key)
					}
					return "", err
				}
				if actualVal != expectedVal {
					return "", fmt.Errorf("condition '%s=%s' not met", field, expectedVal)
				}
			}
		}

		// Retrieve requested fields from KeyDB
		result := make(map[string]string)
		for _, field := range fields {
			val, err := rdb.HGet(ctx, key, field).Result()
			if err != nil {
				if err == redis.Nil {
					return "", fmt.Errorf("field '%s' not found for key '%s'", field, key)
				}
				return "", err
			}
			result[field] = val
		}

		// Log the result from KeyDB
		fmt.Printf("Executed KeyDB HGET/HGETALL command: key=%s -> result=%v\n", key, result)

		return fmt.Sprintf("%v", result), nil
	}

	return "", fmt.Errorf("unsupported query")
}


// parseWhereClause parses the WHERE clause from the SQL-like query
func parseWhereClause(whereClause string) map[string]string {
	// Example WHERE clause: "WHERE key='user:1001' AND country='USA'"
	conditions := make(map[string]string)

	// Trim and split conditions by 'AND'
	whereClause = strings.TrimSpace(strings.TrimPrefix(whereClause, "WHERE"))
	conditionsList := strings.Split(whereClause, "AND")

	for _, condition := range conditionsList {
		condition = strings.TrimSpace(condition)
		parts := strings.Split(condition, "=")
		if len(parts) == 2 {
			field := strings.TrimSpace(strings.Trim(parts[0], "'"))
			value := strings.TrimSpace(strings.Trim(parts[1], "'"))
			conditions[field] = value
		}
	}

	return conditions
}
