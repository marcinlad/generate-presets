package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

var (
	client = http.Client{}
	url    = "http://localhost:3000/management/picture/generate-presets"
)

func fetch(pictures []string) (*http.Response, error) {
	body := map[string][]string{
		"ids": pictures,
	}

	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("error marshalling body: %w", err)
	}

	req, err := http.NewRequest("PATCH", url, bytes.NewBuffer(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %w", err)
	}

	req.Header = http.Header{
		"auth-hash":    []string{authHash},
		"Content-Type": []string{"application/json"},
	}

	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error sending request: %w", err)
	}

	return res, nil
}
