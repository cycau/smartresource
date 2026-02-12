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
	candidates []nodeInfo
	db         string
}

var switcher = &Switcher{}

func (s *Switcher) Init(entries []NodeEntry) error {
	candidates := make([]nodeInfo, len(entries))

	errNode := ""
	var wg sync.WaitGroup
	for i, entry := range entries {
		wg.Add(1)

		candidates[i].BaseURL = entry.BaseURL
		candidates[i].SecretKey = entry.SecretKey
		candidates[i].CheckTime = time.Now().Add(-1 * time.Hour)

		go func(i int, node *nodeInfo) {
			defer wg.Done()
			nodeInfo, err := fetchNodeInfo(node.BaseURL, node.SecretKey, node.CheckTime)
			if err != nil {
				log.Printf("fetch node info %s: %v", node.BaseURL, err)
				errNode = node.BaseURL
				return
			}
			node.NodeID = nodeInfo.NodeID
			node.Status = nodeInfo.Status
			node.MaxHttpQueue = nodeInfo.MaxHttpQueue
			node.Datasources = nodeInfo.Datasources
			node.CheckTime = nodeInfo.CheckTime
		}(i, &candidates[i])
	}
	wg.Wait()
	s.candidates = candidates

	if errNode != "" {
		return fmt.Errorf("failed to connect to node. %s", errNode)
	}
	return nil
}

func (s *Switcher) Request(dbName string, endpoint EndpointType, method string, query map[string]string, body map[string]any, redirectCount int, retryCount int) (*http.Response, int, error) {
	nodeIdx, dsIdx, err := s.selectNode(dbName, endpoint)
	if err != nil {
		return nil, -1, err
	}
	node := &s.candidates[nodeIdx]

	resp, nodeIdx, err := s.requestHttp(nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, query, body, redirectCount)
	if resp == nil {
		return nil, nodeIdx, err
	}
	defer resp.Body.Close()
	// OK response
	if err == nil {
		return resp, nodeIdx, nil
	}

	// retry only when network error or connection timeout or capacity error
	if resp.StatusCode != http.StatusBadGateway &&
		resp.StatusCode != http.StatusGatewayTimeout &&
		resp.StatusCode != http.StatusServiceUnavailable {

		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, nodeIdx, fmt.Errorf("Request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	node.Mu.Lock()
	switch resp.StatusCode {
	case http.StatusBadGateway:
		node.Status = "DRAINING"
	case http.StatusGatewayTimeout:
		node.Status = "DRAINING"
	case http.StatusServiceUnavailable:
		node.Datasources[dsIdx].Active = false
	}
	node.CheckTime = time.Now()
	node.Mu.Unlock()

	retryCount--
	if retryCount < 0 {
		return nil, nodeIdx, fmt.Errorf("Failed to connect to service. Retry count exceeded.")
	}

	// retry
	return s.Request(dbName, endpoint, method, query, body, redirectCount, retryCount)
}

func (s *Switcher) RequestTargetNode(nodeIdx int, endpoint EndpointType, method string, query map[string]string, body map[string]any) (*http.Response, error) {
	node := &s.candidates[nodeIdx]

	resp, _, err := s.requestHttp(nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, query, body, 0)
	return resp, err
}

func (s *Switcher) requestHttp(nodeIdx int, baseURL string, secretKey string, endpoint EndpointType, method string, query map[string]string, body any, redirectCount int) (*http.Response, int, error) {

	u := baseURL + "/rdb" + GetEndpointPath(endpoint)
	req, err := http.NewRequest(method, u, nil)
	if err != nil {
		return nil, nodeIdx, err
	}
	q := req.URL.Query()
	for k, v := range query {
		q.Set(k, v)
	}
	q.Set("_RCount", strconv.Itoa(redirectCount))

	req.URL.RawQuery = q.Encode()
	req.Header.Set("X-Secret-Key", secretKey)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			return nil, nodeIdx, err
		}
		req.Body = io.NopCloser(&buf)
		req.ContentLength = int64(buf.Len())
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, nodeIdx, err
	}

	if resp.StatusCode == http.StatusOK {
		return resp, nodeIdx, nil
	}

	if resp.StatusCode != http.StatusTemporaryRedirect {
		return resp, nodeIdx, fmt.Errorf("query status %d", resp.StatusCode)
	}

	redirectCount--
	// redirect control
	if redirectCount == 1 {
		time.Sleep(300 * time.Millisecond)
	}
	if redirectCount == 0 {
		time.Sleep(500 * time.Millisecond)
	}
	if redirectCount < 0 {
		return nil, nodeIdx, fmt.Errorf("Redirect count exceeded.")
	}

	redirectNodeId := resp.Header.Get("Location")
	if redirectNodeId == "" {
		return nil, nodeIdx, fmt.Errorf("Redirect location is empty.")
	}

	for i := range s.candidates {
		redirectNode := &s.candidates[i]
		if redirectNode.NodeID == redirectNodeId {
			log.Printf("Redirect to node %s, RedirectCount: %d", redirectNode.NodeID, redirectCount)
			return s.requestHttp(i, redirectNode.BaseURL, redirectNode.SecretKey, endpoint, method, query, body, redirectCount)
		}
	}

	return nil, nodeIdx, fmt.Errorf("Redirect node id not found. Node[%s]", redirectNodeId)
}

const PROBLEMATIC_NODE_CHECK_INTERVAL = 15 * time.Second

func (s *Switcher) selectNode(dbName string, endpoint EndpointType) (nodeIdx int, dsIdx int, err error) {
	nodeIdx, dsIdx, problematicNodes, err := s.selectRandomNode(dbName, endpoint)

	if err != nil {
		// full scan and try again
		wg := sync.WaitGroup{}
		for _, i := range problematicNodes {
			n := &s.candidates[i]
			n.Mu.Lock()
			if time.Since(n.CheckTime) < PROBLEMATIC_NODE_CHECK_INTERVAL {
				n.Mu.Unlock()
				continue
			}

			wg.Add(1)
			go func(tarNode *nodeInfo) {
				defer tarNode.Mu.Unlock()
				defer wg.Done()

				nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey, tarNode.CheckTime)
				tarNode.CheckTime = time.Now()
				if err != nil {
					log.Printf("Failed to fetch node info %s: %v", tarNode.BaseURL, err)
					tarNode.Status = "HEALZERR"
					return
				}
				tarNode.NodeID = nodeInfo.NodeID
				tarNode.Status = nodeInfo.Status
				tarNode.MaxHttpQueue = nodeInfo.MaxHttpQueue
				tarNode.Datasources = nodeInfo.Datasources
			}(n)
		}
		wg.Wait()

		nodeIdx, dsIdx, _, err = s.selectRandomNode(dbName, endpoint)
		return nodeIdx, dsIdx, err
	}

	// recover problematic nodes for next request
	for _, i := range problematicNodes {
		n := &s.candidates[i]
		n.Mu.Lock()
		if time.Since(n.CheckTime) < PROBLEMATIC_NODE_CHECK_INTERVAL {
			n.Mu.Unlock()
			continue
		}

		go func(tarNode *nodeInfo) {
			defer tarNode.Mu.Unlock()

			nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey, tarNode.CheckTime)
			tarNode.CheckTime = time.Now()
			if err != nil {
				log.Printf("Failed to fetch node info %s: %v", tarNode.BaseURL, err)
				tarNode.Status = "HEALZERR"
				return
			}
			tarNode.NodeID = nodeInfo.NodeID
			tarNode.Status = nodeInfo.Status
			tarNode.MaxHttpQueue = nodeInfo.MaxHttpQueue
			tarNode.Datasources = nodeInfo.Datasources
		}(n)
	}

	return nodeIdx, dsIdx, err
}

type dsCandidate struct {
	nodeIdx int
	dsIdx   int
	weight  float64
}

func (s *Switcher) selectRandomNode(dbName string, endpoint EndpointType) (nodeIdx int, dsIdx int, problematicNodes []int, err error) {
	if len(s.candidates) == 0 {
		return -1, -1, nil, fmt.Errorf("Node Candidates are not initialized yet.")
	}

	var dsCandidates []dsCandidate
	var nodeIndexes []int

	for i := range s.candidates {
		n := &s.candidates[i]
		n.Mu.RLock()

		if n.Status != "SERVING" {
			if time.Since(n.CheckTime) > PROBLEMATIC_NODE_CHECK_INTERVAL {
				nodeIndexes = append(nodeIndexes, i)
			}
			n.Mu.RUnlock()
			continue
		}

		problematic := false
		for j := range n.Datasources {
			ds := &n.Datasources[j]
			if ds.DatabaseName != dbName {
				continue
			}
			if !ds.Active {
				problematic = true
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
			dsCandidates = append(dsCandidates, dsCandidate{nodeIdx: i, dsIdx: j, weight: w})
		}
		n.Mu.RUnlock()

		if problematic && time.Since(n.CheckTime) > PROBLEMATIC_NODE_CHECK_INTERVAL {
			nodeIndexes = append(nodeIndexes, i)
		}
	}

	if len(dsCandidates) == 0 {
		return -1, -1, nodeIndexes, fmt.Errorf("No available datasource for database %s and endpoint type %d", dbName, endpoint)
	}
	total := 0.0
	for _, c := range dsCandidates {
		total += c.weight
	}
	r := rand.Float64() * total
	for _, cand := range dsCandidates {
		r -= cand.weight
		if r <= 0 {
			return cand.nodeIdx, cand.dsIdx, nodeIndexes, nil
		}
	}

	return -1, -1, nodeIndexes, fmt.Errorf("No available datasource for database %s and endpoint type %d", dbName, endpoint)
}

func fetchNodeInfo(baseURL string, secretKey string, checkTime time.Time) (*nodeInfo, error) {
	if time.Since(checkTime) < 5*time.Second {
		return nil, nil
	}

	req, err := http.NewRequest("GET", baseURL+"/healz", nil)
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
	var nodeInfo nodeInfo
	err = json.Unmarshal(body, &nodeInfo)
	if err != nil {
		return nil, err
	}

	return &nodeInfo, nil
}
