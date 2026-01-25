package rdb

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	clientDataHolderSize = 1
	datasourceIndexSize  = 1
	issuedAtMsSize       = 8
	sequenceNumberSize   = 2
	randomSize           = 8
	txIDPayloadSize      = clientDataHolderSize + datasourceIndexSize + issuedAtMsSize + sequenceNumberSize + randomSize
)

var (
	ErrInvalidTxID = errors.New("invalid txID format")
)

// TxInfo contains decoded transaction ID information
type TxInfo struct {
	DatasourceIndex int
	IssuedAt        time.Time
}

// TxIDGenerator handles transaction ID generation and verification
type TxIDGenerator struct {
	sequenceNumber uint32 // 0-65535 (0xFFFF) で繰り返すカウンター
}

// NewTxIDGenerator creates a new TxIDGenerator with the given secret
func NewTxIDGenerator() *TxIDGenerator {
	return &TxIDGenerator{}
}

// Generate creates a new transaction ID
// Format: base64url( clientDataHolder|datasourceIndex|issuedAtMs|sequenceNumber|randomBytes )
func (g *TxIDGenerator) Generate(clientNodeIndex int, datasourceIndex int) (string, error) {
	// Allocate buffer for payload
	payload := make([]byte, txIDPayloadSize)
	offset := 0

	// ownerNodeShort: 1 bytes
	payload[offset] = uint8(clientNodeIndex)
	offset += clientDataHolderSize

	// datasourceShort: 1 bytes
	payload[offset] = uint8(datasourceIndex)
	offset += datasourceIndexSize

	// issuedAtMs: 8 bytes (uint64)
	issuedAtMs := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(payload[offset:], issuedAtMs)
	offset += issuedAtMsSize

	// sequenceNumber: 2 bytes (0-65533で繰り返す)
	seqNum := atomic.AddUint32(&g.sequenceNumber, 1)
	seqNum = seqNum % 65534 // 0-65533の範囲に収める
	binary.BigEndian.PutUint16(payload[offset:], uint16(seqNum))
	offset += sequenceNumberSize

	// randomBytes: 8 bytes, and also used for security
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
func (g *TxIDGenerator) VerifyAndParse(txId string) (*TxInfo, error) {
	// Decode base64url
	payload, err := base64.RawURLEncoding.DecodeString(txId)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidTxID, err)
	}

	// Check size
	if len(payload) != txIDPayloadSize {
		return nil, fmt.Errorf("%w: invalid size", ErrInvalidTxID)
	}

	// Parse payload
	offset := 0

	// ownerNodeShort: 1 byte
	offset += clientDataHolderSize

	// datasourceShort: 1 byte
	datasourceIndex := payload[offset]
	offset += datasourceIndexSize

	// issuedAtMs: 8 bytes
	issuedAtMs := binary.BigEndian.Uint64(payload[offset:])
	offset += issuedAtMsSize
	issuedAt := time.UnixMilli(int64(issuedAtMs))

	// sequenceNumber: 2 bytes
	offset += sequenceNumberSize

	// random8: 8 bytes
	offset += randomSize

	return &TxInfo{
		DatasourceIndex: int(datasourceIndex),
		IssuedAt:        issuedAt,
	}, nil
}
