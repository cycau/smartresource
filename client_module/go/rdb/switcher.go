package smartclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

type Switcher struct {
	candidates []nodeInfo
	httpClient *http.Client
	sem        *semaphore.Weighted
}

var switcher = &Switcher{}
var httpClient *http.Client

func (s *Switcher) Init(entries []NodeEntry, maxConcurrency int) (defaultDatabase string, err error) {
	count := len(entries)
	if count == 0 {
		return "", fmt.Errorf("No cluster nodes configured.")
	}

	httpClient = &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrency * 2, // 最大アイドル接続数（全ホスト合計）
			MaxIdleConnsPerHost: maxConcurrency,     // ホストあたりのアイドル接続数（未設定時は2のため接続が閉じられTIME_WAITが増える）
			MaxConnsPerHost:     maxConcurrency,     // ホストあたりの同時接続上限
			DisableKeepAlives:   false,              // Keep-Aliveを有効化

			DialContext: (&net.Dialer{ // 接続を確立する際のタイムアウト
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			IdleConnTimeout: 60 * time.Second, // アイドル接続のタイムアウト
		},
	}

	candidates := make([]nodeInfo, count)

	errNode := ""
	var wg sync.WaitGroup
	for i, entry := range entries {
		n := &candidates[i]
		n.BaseURL = entry.BaseURL
		n.SecretKey = entry.SecretKey

		wg.Add(1)
		go func(i int, node *nodeInfo) {
			defer wg.Done()

			nodeInfo, err := fetchNodeInfo(node.BaseURL, node.SecretKey)
			if err != nil {
				log.Printf("fetch node info %s: %v", node.BaseURL, err)
				errNode = node.BaseURL
				return
			}
			node.NodeID = nodeInfo.NodeID
			node.Status = nodeInfo.Status
			node.MaxHttpQueue = nodeInfo.MaxHttpQueue
			node.Datasources = nodeInfo.Datasources
			node.CheckTime = time.Now()
		}(i, n)
	}
	wg.Wait()

	if errNode != "" {
		return "", fmt.Errorf("failed to connect to node. %s", errNode)
	}

	s.candidates = candidates
	s.sem = semaphore.NewWeighted(int64(maxConcurrency))

	return candidates[0].Datasources[0].DatabaseName, nil
}

func (s *Switcher) Request(dbName string, endpoint endpointType, method string, query map[string]string, body map[string]any, redirectCount int, retryCount int) (*http.Response, int, error) {
	nodeIdx, dsIdx, err := s.selectNode(dbName, endpoint)
	if err != nil {
		return nil, -1, err
	}
	node := &s.candidates[nodeIdx]

	resp, nodeIdx, err := s.requestHttp(nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, query, body, redirectCount)
	if resp == nil {
		return nil, nodeIdx, err
	}
	// OK response
	if err == nil {
		return resp, nodeIdx, nil
	}

	// retry only when network error or connection timeout or capacity error
	if resp.StatusCode != http.StatusBadGateway &&
		resp.StatusCode != http.StatusGatewayTimeout &&
		resp.StatusCode != http.StatusServiceUnavailable {

		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, nodeIdx, fmt.Errorf("Request failed with status %d: %s", resp.StatusCode, string(bodyBytes))
	}
	resp.Body.Close()

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
		return nil, nodeIdx, fmt.Errorf("Failed to connect to service. Retry count exceeded. Last status: %d error: %v", resp.StatusCode, err)
	}

	// retry
	return s.Request(dbName, endpoint, method, query, body, redirectCount, retryCount)
}

func (s *Switcher) RequestTargetNode(nodeIdx int, endpoint endpointType, method string, query map[string]string, body map[string]any) (*http.Response, error) {
	node := &s.candidates[nodeIdx]

	resp, _, err := s.requestHttp(nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, query, body, 0)
	return resp, err
}

func (s *Switcher) requestHttp(nodeIdx int, baseURL string, secretKey string, endpoint endpointType, method string, query map[string]string, body any, redirectCount int) (*http.Response, int, error) {
	// redirect多発する場合は並列数調整役割も兼ねる
	if err := s.sem.Acquire(context.Background(), 1); err != nil {
		return nil, nodeIdx, err
	}
	defer s.sem.Release(1)

	u := baseURL + "/rdb" + getEndpointPath(endpoint)
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
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Request failed with Error: %v", err)
		return nil, nodeIdx, err
	}

	if resp.StatusCode == http.StatusOK {
		return resp, nodeIdx, nil
	}

	resp.Body.Close()
	if resp.StatusCode != http.StatusTemporaryRedirect {
		return nil, nodeIdx, fmt.Errorf("query status %d", resp.StatusCode)
	}

	redirectCount--
	// redirect control
	if redirectCount == 1 {
		time.Sleep(300 * time.Millisecond)
	} else if redirectCount == 0 {
		time.Sleep(500 * time.Millisecond)
	} else if redirectCount < 0 {
		return nil, nodeIdx, fmt.Errorf("Redirect count exceeded.")
	}

	redirectNodeId := resp.Header.Get("Location")
	if redirectNodeId == "" {
		return nil, nodeIdx, fmt.Errorf("Redirect location is empty.")
	}

	for i := range s.candidates {
		redirectNode := &s.candidates[i]
		if redirectNode.NodeID == redirectNodeId {
			//log.Printf("Redirect to node %s, RedirectCount: %d", redirectNode.NodeID, redirectCount)
			return s.requestHttp(i, redirectNode.BaseURL, redirectNode.SecretKey, endpoint, method, query, body, redirectCount)
		}
	}

	return nil, nodeIdx, fmt.Errorf("Redirect node id not found. Node[%s]", redirectNodeId)
}

const PROBLEMATIC_NODE_CHECK_INTERVAL = 15 * time.Second

func (s *Switcher) selectNode(dbName string, endpoint endpointType) (nodeIdx int, dsIdx int, err error) {
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

				nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey)
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

			nodeInfo, err := fetchNodeInfo(tarNode.BaseURL, tarNode.SecretKey)
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

func (s *Switcher) selectRandomNode(dbName string, endpoint endpointType) (nodeIdx int, dsIdx int, problematicNodes []int, err error) {
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
			if ds.MaxWriteConns < 1 && (endpoint == ep_EXECUTE || endpoint == ep_BEGIN_TX) {
				continue
			}

			weight := 0.0
			switch endpoint {
			case ep_QUERY:
				weight = float64(ds.MaxOpenConns - ds.MinWriteConns)
			case ep_EXECUTE:
				weight = float64(ds.MaxWriteConns)
			case ep_BEGIN_TX:
				weight = float64(ds.MaxWriteConns+ds.MinWriteConns) / 2.0
			default:
				weight = float64(ds.MaxOpenConns)
			}
			if weight <= 0 {
				continue
			}
			dsCandidates = append(dsCandidates, dsCandidate{nodeIdx: i, dsIdx: j, weight: weight})
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

	return -1, -1, nodeIndexes, fmt.Errorf("No available datasource for database %s and endpoint type %d.", dbName, endpoint)
}

func fetchNodeInfo(baseURL string, secretKey string) (*nodeInfo, error) {

	req, err := http.NewRequest("GET", baseURL+"/healz", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Secret-Key", secretKey)
	resp, err := httpClient.Do(req)

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
