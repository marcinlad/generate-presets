package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/joho/godotenv"
)

var authHash string

func main() {
	godotenv.Load()

	authHash = os.Getenv("AUTH_HASH")

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
	table := "plat-dam-metadata-dev"
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
		fmt.Println("Scanned items: ", scannedItems)

		generatePresets(result.Items)

		if result.LastEvaluatedKey == nil {
			fmt.Println("Ending. Scanned items: ", scannedItems)
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

			res, err := fetch(pictures)
			if err != nil {
				log.Println("fetch failed", err)
				return
			}
			defer res.Body.Close()

			log.Println("Response status: ", res.Status)

			processedItems += len(batch)
		}(batch)
	}

	wg.Wait()

	fmt.Println("All batches processed")
	fmt.Println("Processed items: ", processedItems)
}
