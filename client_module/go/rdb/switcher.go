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

type switcher struct {
	defaultRequestTimeoutSec int
	candidates               []nodeInfo
	semNoneTx                *semaphore.Weighted
	semTx                    *semaphore.Weighted
}

var executor = &switcher{}
var httpClient *http.Client

func (s *switcher) Init(entries []NodeEntry, maxConcurrency int, defaultRequestTimeoutSec int) (defaultDatabase string, err error) {

	httpClient = &http.Client{
		Timeout: time.Duration(defaultRequestTimeoutSec) * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        maxConcurrency * 2, // 最大アイドル接続数（全ホスト合計）
			MaxIdleConnsPerHost: maxConcurrency,     // ホストあたりのアイドル接続数（未設定時は2のため接続が閉じられTIME_WAITが増える）
			MaxConnsPerHost:     maxConcurrency,     // ホストあたりの同時接続上限
			DisableKeepAlives:   false,              // Keep-Aliveを有効化

			DialContext: (&net.Dialer{ // 接続を確立する際のタイムアウト
				Timeout:   5 * time.Second,
				KeepAlive: 15 * time.Second,
			}).DialContext,
			IdleConnTimeout: 60 * time.Second, // アイドル接続のタイムアウト
		},
	}

	candidates := make([]nodeInfo, len(entries))

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

	s.defaultRequestTimeoutSec = defaultRequestTimeoutSec
	s.candidates = candidates
	txMaxConcurrency := maxConcurrency / 10
	if txMaxConcurrency < 1 {
		txMaxConcurrency = 1
	}
	s.semNoneTx = semaphore.NewWeighted(int64(maxConcurrency - txMaxConcurrency))
	s.semTx = semaphore.NewWeighted(int64(txMaxConcurrency))

	return candidates[0].Datasources[0].DatabaseName, nil
}

// RequestTimeout は 0 のときクライアント共通タイムアウトのみ。>0 のときこのリクエスト専用のタイムアウトを指定（先に切れた方が有効）。
func (s *switcher) Request(dbName string, endpoint endpointType, method string, headers map[string]string, body map[string]any, timoutSec int, retryCount int) (*smartResponse, error) {
	if err := s.semNoneTx.Acquire(context.Background(), 1); err != nil {
		return nil, err
	}
	defer s.semNoneTx.Release(1)

	nodeIdx, dsIdx, err := s.selectNode(dbName, endpoint)
	if err != nil {
		return nil, err
	}
	node := &s.candidates[nodeIdx]

	resp, err := s.requestHttp(nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, headers, body, timoutSec, 2)
	if resp == nil {
		return nil, err
	}
	// OK response
	if err == nil {
		return resp, nil
	}
	bodyBytes, _ := io.ReadAll(resp.Body) // 読み切る
	resp.Body.Close()

	// retry only when network error or connection timeout or capacity error
	if !isRetryable(resp.StatusCode) {
		return nil, fmt.Errorf("Request failed with status %d: %s. error: %v", resp.StatusCode, string(bodyBytes), err)
	}

	// 他のノードを探すために、ノードの状態を更新
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
		return nil, fmt.Errorf("Failed to connect to service. Retry count exceeded. Last status: %d error: %v", resp.StatusCode, err)
	}
	time.Sleep(300 * time.Millisecond)

	// retry
	return s.Request(dbName, endpoint, method, headers, body, timoutSec, retryCount)
}

func (s *switcher) RequestTx(nodeIdx int, endpoint endpointType, method string, headers map[string]string, body map[string]any, timoutSec int, retryCount int) (*smartResponse, error) {
	if err := s.semTx.Acquire(context.Background(), 1); err != nil {
		return nil, err
	}
	defer s.semTx.Release(1)

	node := &s.candidates[nodeIdx]
	resp, err := s.requestHttp(nodeIdx, node.BaseURL, node.SecretKey, endpoint, method, headers, body, timoutSec, 0)
	if resp == nil {
		return nil, err
	}
	// OK response
	if err == nil {
		return resp, nil
	}
	bodyBytes, _ := io.ReadAll(resp.Body) // 読み切る
	resp.Body.Close()

	// retry only when network error or connection timeout or capacity error
	if !isRetryable(resp.StatusCode) {
		return nil, fmt.Errorf("Request failed with status %d: %s. error: %v", resp.StatusCode, string(bodyBytes), err)
	}

	retryCount--
	switch retryCount {
	case 2:
		time.Sleep(500 * time.Millisecond)
	case 1:
		time.Sleep(500 * time.Millisecond)
	case 0:
		time.Sleep(2500 * time.Millisecond)
	case -1:
		return nil, fmt.Errorf("Failed to connect to service. Retry count exceeded. Last status: %d error: %v", resp.StatusCode, err)
	}

	// retry
	return s.RequestTx(nodeIdx, endpoint, method, headers, body, timoutSec, retryCount)

}
func isRetryable(statusCode int) bool {
	if statusCode == http.StatusBadGateway {
		return true
	}
	if statusCode == http.StatusGatewayTimeout {
		return true
	}
	if statusCode == http.StatusServiceUnavailable {
		return true
	}
	return false
}

type smartResponse struct {
	http.Response
	NodeIdx int
	Release func()
}

func (s *switcher) requestHttp(nodeIdx int, baseURL string, secretKey string, endpoint endpointType, method string, headers map[string]string, body any, timoutSec int, redirectCount int) (*smartResponse, error) {
	if timoutSec < 1 {
		timoutSec = s.defaultRequestTimeoutSec
	}
	// +3sec to let server side to handle timeout. important!
	ctx, ctxCancel := context.WithTimeout(context.Background(), time.Duration(timoutSec+3)*time.Second)
	req, err := http.NewRequestWithContext(ctx, method, baseURL+"/rdb"+getEndpointPath(endpoint), nil)
	if err != nil {
		ctxCancel()
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set(hEADER_SECRET_KEY, secretKey)
	req.Header.Set(hEADER_REDIRECT_COUNT, strconv.Itoa(redirectCount))
	req.Header.Set(hEADER_TIMEOUT_SEC, strconv.Itoa(timoutSec))
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			ctxCancel()
			return nil, err
		}
		req.Body = io.NopCloser(&buf)
		req.ContentLength = int64(buf.Len())
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Request failed with Error: %v", err)
		if resp != nil && resp.Body != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
		ctxCancel()
		return nil, err
	}

	if resp.StatusCode == http.StatusOK {
		sResponse := &smartResponse{
			Response: *resp,
			NodeIdx:  nodeIdx,
			Release: func() {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				ctxCancel()
			},
		}
		return sResponse, nil
	}

	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	ctxCancel()

	if resp.StatusCode != http.StatusTemporaryRedirect {
		return nil, fmt.Errorf("query status %d", resp.StatusCode)
	}

	redirectCount--
	switch redirectCount {
	case 1:
		time.Sleep(300 * time.Millisecond)
	case 0:
		time.Sleep(900 * time.Millisecond)
	case -1:
		return nil, fmt.Errorf("Redirect count exceeded.")
	}

	redirectNodeId := resp.Header.Get("Location")
	if redirectNodeId == "" {
		return nil, fmt.Errorf("Redirect location is empty.")
	}

	for i := range s.candidates {
		redirectNode := &s.candidates[i]
		if redirectNode.NodeID == redirectNodeId {
			//log.Printf("Redirect to node %s, RedirectCount: %d", redirectNode.NodeID, redirectCount)
			return s.requestHttp(i, redirectNode.BaseURL, redirectNode.SecretKey, endpoint, method, headers, body, redirectCount, timoutSec)
		}
	}

	return nil, fmt.Errorf("Redirect node id not found. Node[%s]", redirectNodeId)
}

const pROBLEMATIC_NODE_CHECK_INTERVAL = 15 * time.Second

func (s *switcher) selectNode(dbName string, endpoint endpointType) (nodeIdx int, dsIdx int, err error) {
	nodeIdx, dsIdx, problematicNodes, err := s.selectRandomNode(dbName, endpoint)

	if err != nil {
		// full scan and try again
		wg := sync.WaitGroup{}
		for _, i := range problematicNodes {
			n := &s.candidates[i]
			n.Mu.Lock()
			if time.Since(n.CheckTime) < pROBLEMATIC_NODE_CHECK_INTERVAL {
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
		if time.Since(n.CheckTime) < pROBLEMATIC_NODE_CHECK_INTERVAL {
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

func (s *switcher) selectRandomNode(dbName string, endpoint endpointType) (nodeIdx int, dsIdx int, problematicNodes []int, err error) {
	if len(s.candidates) == 0 {
		return -1, -1, nil, fmt.Errorf("Node Candidates are not initialized yet.")
	}

	var dsCandidates []dsCandidate
	var nodeIndexes []int

	for i := range s.candidates {
		n := &s.candidates[i]
		n.Mu.RLock()

		if n.Status != "SERVING" {
			if time.Since(n.CheckTime) > pROBLEMATIC_NODE_CHECK_INTERVAL {
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
			if ds.MaxWriteConns < 1 && (endpoint == ep_EXECUTE || endpoint == ep_TX_BEGIN) {
				continue
			}

			weight := 0.0
			switch endpoint {
			case ep_QUERY:
				weight = float64(ds.PoolConns - ds.MinWriteConns)
			case ep_EXECUTE:
				weight = float64(ds.MaxWriteConns)
			case ep_TX_BEGIN:
				weight = float64(ds.MaxWriteConns+ds.MinWriteConns) / 2.0
			default:
				weight = float64(ds.PoolConns)
			}
			if weight <= 0 {
				continue
			}
			dsCandidates = append(dsCandidates, dsCandidate{nodeIdx: i, dsIdx: j, weight: weight})
		}
		n.Mu.RUnlock()

		if problematic && time.Since(n.CheckTime) > pROBLEMATIC_NODE_CHECK_INTERVAL {
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
