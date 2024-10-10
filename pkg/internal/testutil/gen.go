package testutil

import (
	crand "crypto/rand"
	"errors"
	"io"
	"math/rand"
	"net"
	"net/url"
	"strconv"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/find/model"
	crypto "github.com/libp2p/go-libp2p/core/crypto"
	peer "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/capability/assert"
)

func RandomBytes(size int) []byte {
	bytes := make([]byte, size)
	_, _ = crand.Read(bytes)
	return bytes
}

func RandomCAR(size int) ([]datamodel.Link, io.Reader) {
	bytes := RandomBytes(size)
	c, _ := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}.Sum(bytes)

	link := cidlink.Link{Cid: c}
	r := car.Encode([]datamodel.Link{link}, func(yield func(block.Block, error) bool) {
		yield(block.NewBlock(link, bytes), nil)
	})
	return []datamodel.Link{link}, r
}

var seedSeq int64

func RandomPeer() peer.ID {
	src := rand.NewSource(seedSeq)
	seedSeq++
	r := rand.New(src)
	_, publicKey, _ := crypto.GenerateEd25519Key(r)
	peerID, _ := peer.IDFromPublicKey(publicKey)
	return peerID
}

func RandomMultiaddr() multiaddr.Multiaddr {
	// generate a random ipv4 address
	addr := &net.TCPAddr{IP: net.IPv4(byte(rand.Intn(255)), byte(rand.Intn(255)), byte(rand.Intn(255)), byte(rand.Intn(255))), Port: rand.Intn(65535)}
	maddr, err := manet.FromIP(addr.IP)
	if err != nil {
		panic(err)
	}
	port, err := multiaddr.NewComponent(multiaddr.ProtocolWithCode(multiaddr.P_TCP).Name, strconv.Itoa(addr.Port))
	if err != nil {
		panic(err)
	}
	return multiaddr.Join(maddr, port)
}

func RandomCID() datamodel.Link {
	bytes := RandomBytes(10)
	c, _ := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}.Sum(bytes)
	return cidlink.Link{Cid: c}
}

func RandomMultihash() mh.Multihash {
	return RandomCID().(cidlink.Link).Hash()
}

func RandomMultihashes(count int) []mh.Multihash {
	if count <= 0 {
		panic(errors.New("count must be greater than 0"))
	}
	mhs := make([]mh.Multihash, 0, count)
	for range count {
		mhs = append(mhs, RandomMultihash())
	}
	return mhs
}

func RandomLocationClaim() ucan.Capability[assert.LocationCaveats] {
	return assert.Location.New(Service.DID().String(), assert.LocationCaveats{
		Content:  assert.FromHash(RandomMultihash()),
		Location: []url.URL{*TestURL},
	})
}

func RandomLocationDelegation() delegation.Delegation {
	did, err := signer.Generate()
	if err != nil {
		panic(err)
	}
	delegation, err := delegation.Delegate(Service, did, []ucan.Capability[assert.LocationCaveats]{RandomLocationClaim()})
	if err != nil {
		panic(err)
	}
	return delegation
}

func RandomIndexClaim() ucan.Capability[assert.IndexCaveats] {
	return assert.Index.New(Service.DID().String(), assert.IndexCaveats{
		Content: RandomCID(),
		Index:   RandomCID(),
	})
}

func RandomIndexDelegation() delegation.Delegation {
	delegation, err := delegation.Delegate(Service, Service, []ucan.Capability[assert.IndexCaveats]{RandomIndexClaim()})
	if err != nil {
		panic(err)
	}
	return delegation
}

func RandomProviderResult() model.ProviderResult {
	return model.ProviderResult{
		ContextID: RandomBytes(10),
		Metadata:  RandomBytes(10),
		Provider: &peer.AddrInfo{
			ID: RandomPeer(),
			Addrs: []multiaddr.Multiaddr{
				RandomMultiaddr(),
				RandomMultiaddr(),
			},
		},
	}
}

func RandomShardedDagIndexView(size int) (cid.Cid, blobindex.ShardedDagIndexView) {
	roots, contentCar := RandomCAR(size)
	contentCarBytes, err := io.ReadAll(contentCar)
	if err != nil {
		panic(err)
	}

	root, err := cid.Prefix{
		Version:  1,
		Codec:    cid.Raw,
		MhType:   mh.SHA2_256,
		MhLength: -1,
	}.Sum(contentCarBytes)

	if err != nil {
		panic(err)
	}

	shard, err := blobindex.FromShardArchives(roots[0], [][]byte{contentCarBytes})
	if err != nil {
		panic(err)
	}

	return root, shard
}
