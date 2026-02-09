package smartclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type Switcher struct {
	candidates []NodeInfo
	db         string
}

var switcher = &Switcher{}

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
			health.BaseURL = node.BaseURL + "/rdb"
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

func (s *Switcher) Request(dbName string, endpoint EndpointType, method string, query map[string]string, body any, redirectCount int) (*http.Response, error) {
	nodeIdx, dsIdx, err := s.selectNode(dbName, endpoint)
	if err != nil {
		return nil, err
	}
	return s.request(nodeIdx, dsIdx, endpoint, method, query, body, redirectCount)
}
func (s *Switcher) request(nodeIdx int, dsIdx int, endpoint EndpointType, method string, query map[string]string, body any, redirectCount int) (*http.Response, error) {

	node := s.candidates[nodeIdx]
	baseURL := node.BaseURL
	u := baseURL + GetEndpointPath(endpoint)
	req, err := http.NewRequest(method, u, nil)
	if err != nil {
		return nil, err
	}
	q := req.URL.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	q.Set("_RCount", strconv.Itoa(redirectCount))

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
		if redirectCount <= 0 {
			return nil, fmt.Errorf("Redirect count exceeded.")
		}
		if redirectCount == 2 {
			time.Sleep(300 * time.Millisecond)
		}
		if redirectCount == 1 {
			time.Sleep(500 * time.Millisecond)
		}

		redirectNodeId := resp.Header.Get("Location")
		if redirectNodeId == "" {
			return nil, fmt.Errorf("Redirect location is empty.")
		}
		nodeIdx = -1
		for i := range s.candidates {
			if s.candidates[i].NodeID == redirectNodeId {
				nodeIdx = i
				break
			}
		}
		if nodeIdx == -1 {
			return nil, fmt.Errorf("Redirect node id not found. Node[%s]", redirectNodeId)
		}
		log.Printf("Redirect to node %s, RedirectCount: %d", redirectNodeId, redirectCount)
		return s.request(nodeIdx, dsIdx, endpoint, method, query, body, redirectCount-1)
	}
	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		// try else node
		node := &s.candidates[nodeIdx]
		node.Mu.Lock()
		node.Datasources[dsIdx].Active = false
		node.Mu.Unlock()
		go func() {
			node.Mu.Lock()
			if time.Since(node.CheckTime) < 5*time.Second {
				node.Mu.Unlock()
				return
			}
			node.CheckTime = time.Now()
			node.Mu.Unlock()

			time.Sleep(5 * time.Second)
			health, err := fetchHealz(node.BaseURL, node.SecretKey)
			if err != nil {
				log.Printf("healz %s: %v", node.BaseURL, err)
				return
			}
			node.Mu.Lock()
			node.Datasources = health.Datasources
			node.CheckTime = time.Now()
			node.Mu.Unlock()
		}()

		nodeIdx, dsIdx, err = s.selectNode(node.Datasources[dsIdx].DatabaseName, endpoint)
		if err != nil {
			return nil, fmt.Errorf("query status %d: %s", resp.StatusCode, string(bodyBytes))
		}
		return s.request(nodeIdx, dsIdx, endpoint, method, query, body, redirectCount-1)
	}
	return resp, nil
}

func (s *Switcher) selectNode(dbName string, endpoint EndpointType) (nodeIdx int, dsIdx int, err error) {
	if len(s.candidates) == 0 {
		return -1, -1, fmt.Errorf("Not initialized yet")
	}

	type candidate struct {
		node    *NodeInfo
		nodeIdx int
		dsIdx   int
		weight  float64
	}
	var candidates []candidate

	for i := range s.candidates {
		n := &s.candidates[i]
		n.Mu.RLock()

		if n.Status != "SERVING" {
			if time.Since(n.CheckTime) < 5*time.Second {
				n.Mu.Unlock()
				continue
			}
			n.CheckTime = time.Now()
			n.Mu.Unlock()

			health, err := fetchHealz(n.BaseURL, n.SecretKey)
			if err != nil {
				log.Printf("healz %s: %v", n.BaseURL, err)
			}

			n.Mu.Lock()
			n.Status = health.Status
			n.Datasources = health.Datasources

			if n.Status != "SERVING" {
				n.Mu.Unlock()
				continue
			}
		}

		defer n.Mu.RUnlock()
		for j := range n.Datasources {
			ds := &n.Datasources[j]
			if !ds.Active || ds.DatabaseName != dbName {
				continue
			}
			if ds.Readonly && (endpoint == EP_EXECUTE || endpoint == EP_BEGIN_TX) {
				continue
			}
			if endpoint == EP_BEGIN_TX && ds.MaxTxConns == 0 {
				continue
			}
			var w float64
			switch endpoint {
			case EP_QUERY:
				w = float64(ds.MaxOpenConns - ds.MaxTxConns)
			case EP_EXECUTE:
				w = float64(ds.MaxOpenConns - ds.MaxTxConns/2)
			case EP_BEGIN_TX:
				w = float64(ds.MaxTxConns)
			default:
				w = float64(ds.MaxOpenConns)
			}
			if w <= 0 {
				continue
			}
			candidates = append(candidates, candidate{node: n, nodeIdx: i, dsIdx: j, weight: w})
		}
	}
	if len(candidates) == 0 {
		return -1, -1, fmt.Errorf("No available datasource for database %s and endpoint type %d", dbName, endpoint)
	}
	total := 0.0
	for _, c := range candidates {
		total += c.weight
	}
	r := rand.Float64() * total
	for _, cand := range candidates {
		r -= cand.weight
		if r <= 0 {
			return cand.nodeIdx, cand.dsIdx, nil
		}
	}

	return -1, -1, nil
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
