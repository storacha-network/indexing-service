// Package providerresults implements utilities for the IPNI provider result type
package providerresults

import (
	"bytes"
	// for importing schema
	_ "embed"
	"fmt"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/node/bindnode"
	"github.com/ipld/go-ipld-prime/schema"
	"github.com/ipni/go-libipni/find/model"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

var (
	//go:embed providerresults.ipldsch
	providerResultsBytes []byte
	peerIDConverter      = bindnode.NamedBytesConverter("PeerID", bytesToPeerID, peerIDtoBytes)
	multiaddrConverter   = bindnode.NamedBytesConverter("Multiaddr", bytesToMultiaddr, multiaddrToBytes)
	providerResultsType  schema.Type
)

func init() {
	typeSystem, err := ipld.LoadSchemaBytes(providerResultsBytes)
	if err != nil {
		panic(fmt.Errorf("failed to load schema: %w", err))
	}
	providerResultsType = typeSystem.TypeByName("ProviderResults")
}

func bytesToPeerID(data []byte) (interface{}, error) {
	id, err := peer.IDFromBytes(data)
	return &id, err
}

func peerIDtoBytes(peerID interface{}) ([]byte, error) {
	return []byte(*peerID.(*peer.ID)), nil
}

func bytesToMultiaddr(data []byte) (interface{}, error) {
	ma, err := multiaddr.NewMultiaddrBytes(data)
	return &ma, err
}

func multiaddrToBytes(ma interface{}) ([]byte, error) {
	return (*ma.(*multiaddr.Multiaddr)).Bytes(), nil
}

// UnmarshalCBOR decodes a list provider results from CBOR-encoded bytes
func UnmarshalCBOR(data []byte) ([]model.ProviderResult, error) {
	var records []model.ProviderResult
	_, err := ipld.Unmarshal([]byte(data), dagcbor.Decode, &records, providerResultsType, peerIDConverter, multiaddrConverter)
	if err != nil {
		return nil, err
	}
	return records, nil
}

// MarshalCBOR encodes a list provider results in CBOR
func MarshalCBOR(records []model.ProviderResult) ([]byte, error) {
	return ipld.Marshal(dagcbor.Encode, &records, providerResultsType, peerIDConverter, multiaddrConverter)
}

func equalProvider(a, b *peer.AddrInfo) bool {
	if a == nil {
		return b == nil
	}
	return b != nil && a.String() == b.String()
}

// Equals compares two ProviderResults
func Equals(a, b model.ProviderResult) bool {
	return bytes.Equal(a.ContextID, b.ContextID) &&
		bytes.Equal(a.Metadata, b.Metadata) &&
		equalProvider(a.Provider, b.Provider)
}
