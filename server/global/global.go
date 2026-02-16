package global

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

type Config struct {
	NodeName      string             `yaml:"nodeName"`
	NodePort      int                `yaml:"nodePort"`
	SecretKey     string             `yaml:"secretKey"`
	MaxHttpQueue  int                `yaml:"maxHttpQueue"`
	MyDatasources []DatasourceConfig `yaml:"myDatasources"`
	ClusterNodes  []string           `yaml:"clusterNodes"`
}

type DatasourceConfig struct {
	DatasourceID       string `yaml:"datasourceId"`
	DatabaseName       string `yaml:"databaseName"`
	Driver             string `yaml:"driver"`
	DSN                string `yaml:"dsn"`
	MaxOpenConns       int    `yaml:"maxOpenConns"`
	MaxIdleConns       int    `yaml:"maxIdleConns"`
	MaxConnLifetimeSec int    `yaml:"maxConnLifetimeSec"`

	MaxWriteConns          int `yaml:"maxWriteConns"`
	MinWriteConns          int `yaml:"minWriteConns"`
	MaxTxIdleTimeoutSec    int `yaml:"maxTxIdleTimeoutSec"`
	DefaultQueryTimeoutSec int `yaml:"defaultQueryTimeoutSec"`
}

const HEADER_SECRET_KEY = "X-Secret-Key"
const HEADER_DB_NAME = "_Cy_DbName"
const HEADER_TX_ID = "_Cy_TxID"
const HEADER_REDIRECT_COUNT = "_Cy_RdCount"
const HEADER_TIMEOUT_SEC = "_Cy_TimeoutSec"

const EP_PATH_QUERY = "/query"
const EP_PATH_EXECUTE = "/execute"
const EP_PATH_TX_BEGIN = "/begin"
const EP_PATH_TX_COMMIT = "/commit"
const EP_PATH_TX_ROLLBACK = "/rollback"
const EP_PATH_TX_CLOSE = "/close"

// 定数定義
type ENDPOINT_TYPE int

const (
	EP_Query ENDPOINT_TYPE = iota
	EP_Execute
	EP_BeginTx
	EP_Other
)

// エンドポイントタイプ取得
func GetEndpointType(path string) ENDPOINT_TYPE {

	if strings.HasSuffix(path, EP_PATH_QUERY) {
		return EP_Query
	}
	if strings.HasSuffix(path, EP_PATH_EXECUTE) {
		return EP_Execute
	}
	if strings.HasSuffix(path, EP_PATH_TX_BEGIN) {
		return EP_BeginTx
	}

	return EP_Other
}

const CTX_DS_IDX = "$S_IDX"

func GetCtxDsIdx(r *http.Request) (int, bool) {
	value, ok := r.Context().Value(CTX_DS_IDX).(int)
	return value, ok
}

/*****************************
 * TxIDGenerator
 *****************************/
const (
	issuedAtSecondsSize = 4
	sequenceNumberSize  = 4
	datasourceIndexSize = 1
	randomSize          = 6
	txIDPayloadSize     = issuedAtSecondsSize + sequenceNumberSize + datasourceIndexSize + randomSize
)

var (
	ErrInvalidTxID = errors.New("invalid txID format")
)

// TxIDGenerator handles transaction ID generation and verification
type TxIDGenerator struct {
	sequenceNumber uint32 // 0-4294967295 (0xFFFFFFFF) counter
}

// NewTxIDGenerator creates a new TxIDGenerator with the given secret
func NewTxIDGenerator() *TxIDGenerator {
	seedBytes := make([]byte, 4)
	if _, err := rand.Read(seedBytes); err != nil {
		// If an error occurs, start from 0 (very rare case)
		return &TxIDGenerator{}
	}
	return &TxIDGenerator{
		sequenceNumber: binary.BigEndian.Uint32(seedBytes),
	}
}

// Generate creates a new transaction ID
// Format: base64url( issuedAtSeconds|sequenceNumber|datasourceIndex|clientNodeIndex|randomBytes )
func (g *TxIDGenerator) Generate(datasourceIndex int) (txID string, err error) {
	// Allocate buffer for payload
	payload := make([]byte, txIDPayloadSize)
	offset := 0

	// issuedAtSeconds: 4 bytes (uint32)
	issuedAtSeconds := uint32(time.Now().Unix())
	binary.BigEndian.PutUint32(payload[offset:], issuedAtSeconds)
	offset += issuedAtSecondsSize

	// sequenceNumber: 4 bytes (0-4294967295で繰り返す)
	seqNum := atomic.AddUint32(&g.sequenceNumber, 1)
	binary.BigEndian.PutUint32(payload[offset:], seqNum)
	offset += sequenceNumberSize

	// datasourceIndex: 1 bytes
	payload[offset] = uint8(datasourceIndex)
	offset += datasourceIndexSize

	// randomBytes: 5 bytes, and also used for security
	randomBytes := make([]byte, randomSize)
	if _, err := rand.Read(randomBytes); err != nil {
		// This should almost never happen, but if it does, return an error
		return "", fmt.Errorf("failed to generate random bytes for security: %w", err)
	}
	copy(payload[offset:], randomBytes)

	// Encode as base64url
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

// VerifyAndParse verifies and parses a transaction ID
func GetDsIdxFromTxID(txID string) (dsIdx int, err error) {
	// Decode base64url
	payload, err := base64.RawURLEncoding.DecodeString(txID)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidTxID, err)
	}

	// Check size
	if len(payload) != txIDPayloadSize {
		return 0, fmt.Errorf("%w: invalid size", ErrInvalidTxID)
	}

	// Parse payload
	offset := 0

	// issuedAtSeconds: 4 bytes
	offset += issuedAtSecondsSize

	// sequenceNumber: 4 bytes
	offset += sequenceNumberSize

	// datasourceIndex: 1 byte
	datasourceIndex := payload[offset]
	offset += datasourceIndexSize

	return int(datasourceIndex), nil
}
