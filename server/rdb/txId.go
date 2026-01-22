package rdb

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

const (
	txIDVersion         = 1
	txIDVersionSize     = 1
	ownerNodeShortSize  = 2
	datasourceShortSize = 2
	issuedAtMsSize      = 8
	randomSize          = 16
	txIDPayloadSize     = txIDVersionSize + ownerNodeShortSize + datasourceShortSize + issuedAtMsSize + randomSize
)

var (
	ErrInvalidTxID    = errors.New("invalid txid format")
	ErrInvalidVersion = errors.New("invalid txid version")
)

// TxInfo contains decoded transaction ID information
type TxInfo struct {
	DsIndex  int
	IssuedAt time.Time
}

// TxIDGenerator handles transaction ID generation and verification
type TxIDGenerator struct {
}

// NewTxIDGenerator creates a new TxIDGenerator with the given secret
func NewTxIDGenerator() *TxIDGenerator {
	return &TxIDGenerator{}
}

// Generate creates a new transaction ID
// Format: base64url( version|ownerNodeShort|datasourceShort|issuedAtMs|random16|hmac32 )
func (g *TxIDGenerator) Generate(nodeIndex int, datasourceIndex int) (string, error) {
	// Allocate buffer for payload
	payload := make([]byte, txIDPayloadSize)
	offset := 0

	// version: 1 byte
	payload[offset] = txIDVersion
	offset += txIDVersionSize

	// ownerNodeShort: 2 bytes
	binary.BigEndian.PutUint16(payload[offset:], uint16(nodeIndex))
	offset += ownerNodeShortSize

	// datasourceShort: 2 bytes
	binary.BigEndian.PutUint16(payload[offset:], uint16(datasourceIndex))
	offset += datasourceShortSize

	// issuedAtMs: 8 bytes (uint64)
	issuedAtMs := uint64(time.Now().UnixMilli())
	binary.BigEndian.PutUint64(payload[offset:], issuedAtMs)
	offset += issuedAtMsSize

	// random16: 16 bytes
	randomBytes := make([]byte, randomSize)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("failed to generate random bytes: %w", err)
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

	// version: 1 byte
	version := payload[offset]
	if version != txIDVersion {
		return nil, ErrInvalidVersion
	}
	offset += txIDVersionSize

	// ownerNodeShort: 2 bytes
	offset += ownerNodeShortSize

	// datasourceShort: 2 bytes
	dsIndex := binary.BigEndian.Uint16(payload[offset:])
	offset += datasourceShortSize

	// issuedAtMs: 8 bytes
	issuedAtMs := binary.BigEndian.Uint64(payload[offset:])
	issuedAt := time.UnixMilli(int64(issuedAtMs))

	return &TxInfo{
		DsIndex:  int(dsIndex),
		IssuedAt: issuedAt,
	}, nil
}
