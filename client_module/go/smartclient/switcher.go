package smartclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"sync"
)

type Switcher struct {
	candidates []NodeInfo
	db         string
}

var switcher *Switcher

func (s *Switcher) Init(nodes []NodeEntry) error {
	candidates := make([]NodeInfo, len(nodes))

	haveError := false
	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node NodeEntry) {
			defer wg.Done()
			health, err := fetchHealz(node.BaseURL, node.SecretKey)
			if err != nil {
				log.Printf("healz %s: %v", node.BaseURL, err)
				haveError = true
				return
			}
			health.BaseURL = node.BaseURL
			health.SecretKey = node.SecretKey
			candidates[i] = *health
		}(i, node)
	}
	wg.Wait()
	s.candidates = candidates

	if haveError {
		return fmt.Errorf("failed to fetch healz")
	}
	return nil
}

func (s *Switcher) request(nodeIdx int, url string, method string, query map[string]string, body any, retryCount int) (*http.Response, error) {
	node := s.candidates[nodeIdx]
	baseURL := node.BaseURL
	u := baseURL + url
	req, err := http.NewRequest(method, u, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Secret-Key", node.SecretKey)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, err
		}
		req.Body = io.NopCloser(&buf)
		req.ContentLength = int64(buf.Len())
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusTemporaryRedirect {
		resp.Body.Close()
		return s.request(nodeIdx, resp.Header.Get("Location"), method, query, body, retryCount-1)
	}
	if resp.StatusCode != http.StatusOK {
		// TODO: retry
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("query status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	return resp, nil
}

func (s *Switcher) selectNode(dbName string, endpoint endpointType) (nodeIdx int, err error) {
	type candidate struct {
		node   *NodeInfo
		dsIdx  int
		weight float64
	}
	var candidates []candidate

	for i := range s.candidates {
		n := &s.candidates[i]
		if n.Status != "SERVING" {
			continue
		}

		for j := range n.Datasources {
			ds := &n.Datasources[j]
			if !ds.Active || ds.DatabaseName != dbName {
				continue
			}
			if ds.Readonly && (endpoint == epExecute || endpoint == epBeginTx) {
				continue
			}
			if endpoint == epBeginTx && ds.MaxTxConns == 0 {
				continue
			}
			var w float64
			switch endpoint {
			case epQuery:
				w = float64(ds.MaxOpenConns - ds.MaxTxConns)
			case epExecute:
				w = float64(ds.MaxOpenConns - ds.MaxTxConns/2)
			case epBeginTx:
				w = float64(ds.MaxTxConns)
			default:
				w = float64(ds.MaxOpenConns)
			}
			if w <= 0 {
				continue
			}
			candidates = append(candidates, candidate{node: n, dsIdx: j, weight: w})
		}
	}
	if len(candidates) == 0 {
		return -1, fmt.Errorf("no available datasource for database %s and endpoint type %d", dbName, endpoint)
	}
	total := 0.0
	for _, c := range candidates {
		total += c.weight
	}
	r := rand.Float64() * total
	for idx, cand := range candidates {
		r -= cand.weight
		if r <= 0 {
			return idx, nil
		}
	}

	return len(candidates) - 1, nil
}

func fetchHealz(baseUrl string, secretKey string) (*NodeInfo, error) {
	req, err := http.NewRequest("GET", baseUrl+"/healz", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Secret-Key", secretKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var nodeHealth NodeInfo
	err = json.Unmarshal(body, &nodeHealth)
	if err != nil {
		return nil, err
	}
	return &nodeHealth, nil
}
