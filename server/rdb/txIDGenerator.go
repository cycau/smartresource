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
	issuedAtSecondsSize  = 4
	sequenceNumberSize   = 4
	datasourceIndexSize  = 1
	clientDataHolderSize = 1
	randomSize           = 5
	txIDPayloadSize      = issuedAtSecondsSize + sequenceNumberSize + datasourceIndexSize + clientDataHolderSize + randomSize
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
func (g *TxIDGenerator) Generate(datasourceIndex int, clientNodeIndex int) (txID string, err error) {
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

	// clientDataHolder: 1 bytes
	payload[offset] = uint8(clientNodeIndex)
	offset += clientDataHolderSize

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
func (g *TxIDGenerator) GetDatasourceIndex(txID string) (dsIdx int, err error) {
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
