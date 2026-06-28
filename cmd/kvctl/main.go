package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

type StatusResponse struct {
	NodeID      string `json:"node_id"`
	Role        string `json:"role"`
	Term        int64  `json:"term"`
	CommitIndex int64  `json:"commit_index"`
	LogLength   int64  `json:"log_length"`
	Leader      string `json:"leader"`
}

type RedirectResponse struct {
	Error  string `json:"error"`
	Leader string `json:"leader"`
}

func main() {
	addr := flag.String("addr", "localhost:8081", "Address of the node to contact (e.g. localhost:8081)")
	stale := flag.Bool("stale", false, "Allow stale reads from followers (GET only)")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	cmd := strings.ToUpper(args[0])
	client := &http.Client{
		Timeout: 5 * time.Second,
		// Prevent automatic redirect following so we can handle redirect status codes manually
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	url := fmt.Sprintf("http://%s", *addr)
	if !strings.HasPrefix(*addr, "http://") && !strings.HasPrefix(*addr, "https://") {
		url = fmt.Sprintf("http://%s", *addr)
	}

	switch cmd {
	case "GET":
		if len(args) < 2 {
			fmt.Println("Usage: kvctl GET <key>")
			os.Exit(1)
		}
		key := args[1]
		reqURL := fmt.Sprintf("%s/kv/%s", url, key)
		if *stale {
			reqURL += "?stale=true"
		}
		doRequest(client, http.MethodGet, reqURL, nil)

	case "PUT":
		if len(args) < 3 {
			fmt.Println("Usage: kvctl PUT <key> <value>")
			os.Exit(1)
		}
		key := args[1]
		val := args[2]
		reqURL := fmt.Sprintf("%s/kv/%s", url, key)
		doRequest(client, http.MethodPut, reqURL, []byte(val))

	case "DELETE":
		if len(args) < 2 {
			fmt.Println("Usage: kvctl DELETE <key>")
			os.Exit(1)
		}
		key := args[1]
		reqURL := fmt.Sprintf("%s/kv/%s", url, key)
		doRequest(client, http.MethodDelete, reqURL, nil)

	case "STATUS":
		reqURL := fmt.Sprintf("%s/status", url)
		doRequest(client, http.MethodGet, reqURL, nil)

	default:
		printUsage()
		os.Exit(1)
	}
}

func doRequest(client *http.Client, method, url string, body []byte) {
	for {
		var bodyReader io.Reader
		if body != nil {
			bodyReader = bytes.NewReader(body)
		}

		req, err := http.NewRequest(method, url, bodyReader)
		if err != nil {
			fmt.Printf("Error creating request: %v\n", err)
			os.Exit(1)
		}

		resp, err := client.Do(req)
		if err != nil {
			fmt.Printf("HTTP request failed: %v\n", err)
			os.Exit(1)
		}

		respBody, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			fmt.Printf("Failed to read response body: %v\n", err)
			os.Exit(1)
		}

		// Handle redirects manually
		if resp.StatusCode == http.StatusTemporaryRedirect || resp.StatusCode == http.StatusMovedPermanently {
			// Extract redirect URL from header or response body
			var redirect RedirectResponse
			if err := json.Unmarshal(respBody, &redirect); err == nil && redirect.Leader != "" {
				url = redirect.Leader
				fmt.Printf("Redirecting to leader: %s\n", url)
				continue
			}
			loc := resp.Header.Get("Location")
			if loc != "" {
				url = loc
				fmt.Printf("Redirecting to leader (Location header): %s\n", url)
				continue
			}
			fmt.Printf("Redirect status received, but leader address could not be resolved. Body: %s\n", string(respBody))
			os.Exit(1)
		}

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("Error [Status %d]: %s\n", resp.StatusCode, string(respBody))
			os.Exit(1)
		}

		fmt.Println(string(respBody))
		return
	}
}

func printUsage() {
	fmt.Println("Usage:")
	fmt.Println("  kvctl [options] GET <key>          - Get value for key")
	fmt.Println("  kvctl [options] PUT <key> <value>  - Put key-value pair")
	fmt.Println("  kvctl [options] DELETE <key>       - Delete key")
	fmt.Println("  kvctl [options] STATUS             - Get node status")
	fmt.Println("\nOptions:")
	fmt.Println("  -addr string   HTTP address of the node (default \"localhost:8081\")")
	fmt.Println("  -stale         Allow reading stale data from follower (GET only)")
}
