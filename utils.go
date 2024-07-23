package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func splitItems(items []map[string]types.AttributeValue) [][]map[string]types.AttributeValue {
	var batches [][]map[string]types.AttributeValue
	batchSize := 50

	for i := 0; i < len(items); i += batchSize {
		end := i + batchSize
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}

	return batches
}

func extractPictures(batch []map[string]types.AttributeValue) ([]string, error) {
	var pictures []string

	for _, item := range batch {
		var picture map[string]interface{}

		if err := attributevalue.UnmarshalMap(item, &picture); err != nil {
			return nil, fmt.Errorf("unmarshal failed: %w", err)
		}

		id, ok := picture["id"].(string)
		if !ok {
			return nil, fmt.Errorf("id not found or not a string")
		}

		pictures = append(pictures, id)
	}

	return pictures, nil
}
