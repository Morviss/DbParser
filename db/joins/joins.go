package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/vg"
	"image/color"
)

var (
	ctx = context.Background()

	// Redis client for KeyDB
	rdb = redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // KeyDB server address
	})

	countries = []string{"India", "USA", "Canada"} // List of countries to choose from
)

func main() {
	rand.Seed(time.Now().UnixNano()) // Seed the random number generator

	// Variables to store times for plotting
	var insertionTimes []float64
	var queryTimes []float64
	var numUsersList []float64

	// Benchmark data insertion and query for different numbers of users
	for numUsers := 100; numUsers <= 500; numUsers += 100 {
		startInsert := time.Now()

		// Insert data into KeyDB
		for i := 1; i <= numUsers; i++ {
			userKey := fmt.Sprintf("user:%d", i)
			userData := map[string]interface{}{
				"name":    fmt.Sprintf("User %d", i),
				"email":   fmt.Sprintf("user%d@example.com", i),
				"age":     strconv.Itoa(rand.Intn(40) + 20), // Random age between 20 and 60
				"country": countries[rand.Intn(len(countries))], // Random country from the list
			}

			err := rdb.HMSet(ctx, userKey, userData).Err()
			if err != nil {
				log.Fatalf("Error writing user data to KeyDB: %v\n", err)
			}

			// Inserting additional user profile data
			profileKey := fmt.Sprintf("user_profile:%d", i)
			profileData := map[string]interface{}{
				"bio":  fmt.Sprintf("This is user %d", i),
				"city": fmt.Sprintf("City%d", rand.Intn(10)), // Random city
			}

			err = rdb.HMSet(ctx, profileKey, profileData).Err()
			if err != nil {
				log.Fatalf("Error writing user profile data to KeyDB: %v\n", err)
			}
		}

		durationInsert := time.Since(startInsert).Seconds() // Convert to seconds
		insertionTimes = append(insertionTimes, durationInsert)
		numUsersList = append(numUsersList, float64(numUsers))
		fmt.Printf("Inserted %d user profiles in %v\n", numUsers, durationInsert)

		// Example SQL-like query to retrieve data with age > 25 and country='India'
		sqlQuery := "SELECT email, bio FROM keydb WHERE age > 25 AND country='India' JOIN user_profile ON user.id = user_profile.id"

		// Measure query time
		startQuery := time.Now()
		result, err := handleSQLQuery(sqlQuery, numUsers)
		if err != nil {
			log.Fatalf("Error handling SQL query: %v\n", err)
		}
		durationQuery := time.Since(startQuery).Seconds() // Convert to seconds
		queryTimes = append(queryTimes, durationQuery)
		fmt.Printf("Query time for %d users: %v\n", numUsers, durationQuery)
		fmt.Printf("result: %v\n", result)
	}

	// Plot the graph
	err := plotGraph(numUsersList, insertionTimes, queryTimes)
	if err != nil {
		log.Fatalf("Error plotting graph: %v\n", err)
	}
}

func extractFields(query string) []string {
	selectStart := len("SELECT ")
	selectEnd := strings.Index(query, " FROM ")
	fields := strings.TrimSpace(query[selectStart:selectEnd])

	if fields == "*" {
		return []string{"name", "email", "age", "country"} // Return all fields you expect
	}

	return strings.Split(fields, ",")
}

func handleSQLQuery(query string, numUsers int) (string, error) {
	query = strings.TrimSpace(query)
	fmt.Printf("Received SQL query: %s\n", query)

	if strings.HasPrefix(strings.ToUpper(query), "SELECT") {
		parts := strings.Split(query, " JOIN ")
		if len(parts) > 1 {
			return handleJoinQuery(parts, numUsers)
		}

		whereStart := strings.Index(query, "WHERE")
		var conditions map[string]interface{}
		if whereStart != -1 {
			conditions = parseWhereClause(query[whereStart:])
		}

		var results []map[string]string
		keys := make([]string, numUsers)
		for i := 1; i <= numUsers; i++ {
			keys[i-1] = fmt.Sprintf("user:%d", i)
		}

		for _, key := range keys {
			if len(conditions) > 0 && !checkConditions(ctx, key, conditions) {
				continue
			}

			fields := extractFields(query)
			result := make(map[string]string)
			for _, field := range fields {
				val, err := rdb.HGet(ctx, key, field).Result()
				if err != nil && err != redis.Nil {
					return "", err
				}
				if err == redis.Nil {
					continue
				}
				result[field] = val
			}
			results = append(results, result)
		}
		return fmt.Sprintf("%v", results), nil
	}

	return "", fmt.Errorf("unsupported query")
}

func handleJoinQuery(parts []string, numUsers int) (string, error) {
	mainQuery := parts[0] // The main table (e.g., user)
	joinQuery := parts[1] // The joined table (e.g., user_profile)

	mainFields := extractFields(mainQuery)
	joinFields := extractFields(joinQuery)

	var results []map[string]string
	keys := make([]string, numUsers)
	for i := 1; i <= numUsers; i++ {
		keys[i-1] = fmt.Sprintf("user:%d", i)
	}

	for _, key := range keys {
		mainResult := make(map[string]string)
		for _, field := range mainFields {
			val, err := rdb.HGet(ctx, key, field).Result()
			if err != nil && err != redis.Nil {
				return "", err
			}
			if err == redis.Nil {
				continue
			}
			mainResult[field] = val
		}

		profileKey := fmt.Sprintf("user_profile:%d", strings.TrimPrefix(key, "user:"))
		joinResult := make(map[string]string)
		for _, field := range joinFields {
			val, err := rdb.HGet(ctx, profileKey, field).Result()
			if err != nil && err != redis.Nil {
				return "", err
			}
			if err == redis.Nil {
				continue
			}
			joinResult[field] = val
		}

		combinedResult := make(map[string]string)
		for k, v := range mainResult {
			combinedResult[k] = v
		}
		for k, v := range joinResult {
			combinedResult[k] = v
		}
		results = append(results, combinedResult)
	}

	return fmt.Sprintf("%v", results), nil
}

func plotGraph(numUsers, insertTimes, queryTimes []float64) error {
	p := plot.New()

	p.Title.Text = "Insertion and Query Time vs Number of Users"
	p.X.Label.Text = "Number of Users"
	p.Y.Label.Text = "Time (seconds)"

	// Create insertion time points
	insertionPoints := make(plotter.XYs, len(numUsers))
	for i := range numUsers {
		insertionPoints[i].X = numUsers[i]
		insertionPoints[i].Y = insertTimes[i]
	}

	// Create query time points
	queryPoints := make(plotter.XYs, len(numUsers))
	for i := range numUsers {
		queryPoints[i].X = numUsers[i]
		queryPoints[i].Y = queryTimes[i]
	}

	// Create line plot for insertion time
	lineInsert, err := plotter.NewLine(insertionPoints)
	if err != nil {
		return err
	}
	lineInsert.Color = color.RGBA{R: 255, G: 0, B: 0, A: 255} // Set the color to red

	// Create line plot for query time
	lineQuery, err := plotter.NewLine(queryPoints)
	if err != nil {
		return err
	}
	lineQuery.Color = color.RGBA{R: 0, G: 0, B: 255, A: 255} // Set the color to blue

	// Add legend
	p.Legend.Add("Insertion Time", lineInsert)
	p.Legend.Add("Query Time", lineQuery)

	// Add lines to the plot
	p.Add(lineInsert, lineQuery)

	// Save the plot to a PNG file
	if err := p.Save(6*vg.Inch, 4*vg.Inch, "times_vs_users.png"); err != nil {
		return err
	}

	fmt.Println("Graph saved as times_vs_users.png")
	return nil
}

func checkConditions(ctx context.Context, key string, conditions map[string]interface{}) bool {
	for field, expectedVal := range conditions {
		actualVal, err := rdb.HGet(ctx, key, field).Result()
		if err != nil {
			return false // Field not found or error occurred
		}

		// Special handling for numeric comparisons (e.g., age > 25)
		switch expectedVal := expectedVal.(type) {
		case string:
			if actualVal != expectedVal {
				return false
			}
		case int:
			actualInt, err := strconv.Atoi(actualVal)
			if err != nil {
				return false
			}
			if actualInt <= expectedVal { // If the age is less than or equal to the condition, skip this entry
				return false
			}
		}
	}
	return true // All conditions met
}

func parseWhereClause(whereClause string) map[string]interface{} {
	conditions := make(map[string]interface{})

	whereClause = strings.TrimSpace(strings.TrimPrefix(whereClause, "WHERE"))
	conditionsList := strings.Split(whereClause, "AND")

	for _, condition := range conditionsList {
		condition = strings.TrimSpace(condition)

		// Handle greater-than conditions (e.g., age > 25)
		if strings.Contains(condition, ">") {
			parts := strings.Split(condition, ">")
			if len(parts) == 2 {
				field := strings.TrimSpace(parts[0])
				value, err := strconv.Atoi(strings.TrimSpace(parts[1]))
				if err == nil {
					conditions[field] = value
				}
			}
		} else {
			// Handle equality conditions (e.g., country='USA')
			parts := strings.Split(condition, "=")
			if len(parts) == 2 {
				field := strings.TrimSpace(strings.Trim(parts[0], "'"))
				value := strings.TrimSpace(strings.Trim(parts[1], "'"))
				conditions[field] = value
			}
		}
	}

	return conditions
}
