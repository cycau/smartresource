package module

import (
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"time"
)

var (
	ErrInvalidTxID = errors.New("invalid txid format")
)

// ExtractNodeIndexFromTxID extracts the node index from a transaction ID
// According to spec.md, the node index is in the first byte after base64 decoding
func ExtractNodeIndexFromTxID(txID string) (int, error) {
	// Decode base64url
	payload, err := base64.RawURLEncoding.DecodeString(txID)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrInvalidTxID, err)
	}

	if len(payload) == 0 {
		return 0, fmt.Errorf("%w: empty payload", ErrInvalidTxID)
	}

	// First byte contains the node index (according to spec.md)
	nodeIndex := int(payload[0])
	return nodeIndex, nil
}

// SelectRandomNode selects a random node from the list of available nodes
func SelectRandomNode(nodes []*NodeInfo) *NodeInfo {
	if len(nodes) == 0 {
		return nil
	}
	idx := rand.Intn(len(nodes))
	return nodes[idx]
}

// IsNodeAvailable checks if a node is available (not marked as unavailable)
func IsNodeAvailable(node *NodeInfo) bool {
	if node == nil {
		return false
	}
	now := time.Now()
	return node.UnavailableUntil.IsZero() || now.After(node.UnavailableUntil)
}

// IsDatasourceAvailable checks if a datasource is available (not marked as unavailable)
func IsDatasourceAvailable(ds *Datasource) bool {
	if ds == nil {
		return false
	}
	now := time.Now()
	return ds.UnavailableUntil.IsZero() || now.After(ds.UnavailableUntil)
}

// MarkNodeUnavailable marks a node as unavailable for 15 seconds
func MarkNodeUnavailable(node *NodeInfo) {
	if node != nil {
		node.UnavailableUntil = time.Now().Add(15 * time.Second)
	}
}

// MarkDatasourceUnavailable marks a datasource as unavailable for 5 seconds
func MarkDatasourceUnavailable(ds *Datasource) {
	if ds != nil {
		ds.UnavailableUntil = time.Now().Add(5 * time.Second)
	}
}
