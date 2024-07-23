package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/joho/godotenv"
)

var (
	authHash          string
	table             string
	endpointUrl       string
	allProcessedItems = 0
)

func main() {
	godotenv.Load()

	authHash = os.Getenv("AUTH_HASH")
	table = os.Getenv("DYNAMODB_TABLE")
	endpointUrl = os.Getenv("ENDPOINT_URL")

	cfg, err := config.LoadDefaultConfig(context.TODO(), func(o *config.LoadOptions) error {
		o.Region = "eu-west-1"
		return nil
	})
	if err != nil {
		panic(err)
	}

	client := dynamodb.NewFromConfig(cfg)

	scan(client)
}

func scan(client *dynamodb.Client) {
	var scannedItems int64
	var lastKey map[string]types.AttributeValue

	for {
		result, err := client.Scan(context.Background(), &dynamodb.ScanInput{
			TableName:         aws.String(table),
			ExclusiveStartKey: lastKey,
		})
		if err != nil {
			log.Fatal("Scan failed", err)
		}

		scannedItems += int64(len(result.Items))
		fmt.Println("All scanned items: ", scannedItems)

		start := time.Now()
		generatePresets(result.Items)
		fmt.Println("Time taken to generate presets: ", time.Since(start).Milliseconds(), "ms")

		if result.LastEvaluatedKey == nil {
			fmt.Println("Ending. Scanned items: ", scannedItems, "Processed items: ", allProcessedItems)
			break
		}

		lastKey = result.LastEvaluatedKey
	}
}

func generatePresets(items []map[string]types.AttributeValue) {
	var wg sync.WaitGroup
	var processedItems int
	batches := splitItems(items)

	for _, batch := range batches {
		wg.Add(1)

		go func(batch []map[string]types.AttributeValue) {
			defer wg.Done()

			pictures, err := extractPictures(batch)
			if err != nil {
				log.Println(err)
				return
			}

			retryLimit := 3

			for i := 0; i < retryLimit; i++ {
				res, err := fetch(pictures)
				if err != nil {
					log.Println("fetch failed", err)
					return
				}
				defer res.Body.Close()

				if res.StatusCode == 200 {
					fmt.Println("Batch processed successfully")
					break
				}

				if i == retryLimit-1 {
					log.Println("Batch failed to process after 3 retries")
					return
				}

				log.Println("Response status: ", res.Status)
				fmt.Println("Retrying...")

				time.Sleep(500 * time.Millisecond)
			}

			processedItems += len(batch)
		}(batch)
	}

	wg.Wait()

	fmt.Println("All batches processed")
	fmt.Println("Processed items: ", processedItems)

	allProcessedItems += processedItems
}
