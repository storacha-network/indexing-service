package blobindex

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/ipld/block"
	"github.com/storacha/go-ucanto/core/ipld/codec/cbor"
	"github.com/storacha/go-ucanto/core/ipld/hash/sha256"
	"github.com/storacha/go-ucanto/core/result/failure"
	dm "github.com/storacha/indexing-service/pkg/blobindex/datamodel"
)

// ExtractError is a union type of UnknownFormatError and DecodeFailureErorr
type ExtractError interface {
	error
	isExtractError()
}

// Extract extracts a sharded dag index from a car
func Extract(r io.Reader) (ShardedDagIndexView, error) {
	dc, err := decodeCar(r)
	if err != nil {
		return nil, NewUnknownFormatError(err)
	}
	return View(dc.root, dc.blocks)
}

type decodedCar struct {
	root   ipld.Link
	blocks map[ipld.Link]ipld.Block
}

func decodeCar(r io.Reader) (decodedCar, error) {
	roots, blocks, err := car.Decode(r)
	if err != nil {
		return decodedCar{}, err
	}

	if len(roots) == 0 {
		return decodedCar{}, errors.New("missing root block")
	}

	codec := roots[0].(cidlink.Link).Cid.Prefix().Codec
	if codec != cid.DagCBOR {
		return decodedCar{}, fmt.Errorf("unexpected root CID codec: %x", codec)
	}
	blockMap := make(map[ipld.Link]ipld.Block)
	for blk, err := range blocks {
		if err != nil {

			return decodedCar{}, err
		}
		blockMap[blk.Link()] = blk
	}
	return decodedCar{roots[0], blockMap}, nil
}

func View(root ipld.Link, blockMap map[ipld.Link]ipld.Block) (ShardedDagIndexView, ExtractError) {
	rootBlock, ok := blockMap[root]
	if !ok {
		return nil, NewDecodeFailureError(fmt.Errorf("missing root block: %s", root))
	}
	var shardedDagIndexData dm.ShardedDagIndexModel
	err := cbor.Decode(rootBlock.Bytes(), &shardedDagIndexData, dm.ShardedDagIndexSchema())
	if err != nil {
		return nil, NewDecodeFailureError(err)
	}
	if shardedDagIndexData.DagO_1 == nil {
		return nil, NewUnknownFormatError(fmt.Errorf("unknown index version"))
	}
	dagIndex := NewShardedDagIndexView(root, len(shardedDagIndexData.DagO_1.Shards))
	for _, shardLink := range shardedDagIndexData.DagO_1.Shards {
		shard, ok := blockMap[shardLink]
		if !ok {
			return nil, NewDecodeFailureError(fmt.Errorf("missing shard block: %s", shardLink))
		}
		var blobIndexData dm.BlobIndexModel
		if err := cbor.Decode(shard.Bytes(), &blobIndexData, dm.BlobIndexSchema()); err != nil {
			return nil, NewDecodeFailureError(err)
		}
		blobIndex := NewMultihashMap[Position](len(blobIndexData.Slices))
		for _, blobSlice := range blobIndexData.Slices {
			blobIndex.Set(blobSlice.Multihash, blobSlice.Position)
		}
		dagIndex.Shards().Set(blobIndexData.Multihash, blobIndex)
	}
	return dagIndex, nil
}

type shardedDagIndex struct {
	content ipld.Link
	shards  MultihashMap[MultihashMap[Position]]
}

// NewShardedDagIndexView constructs an empty ShardedDagIndexView
//   - content sets the content link
//     -- shardSizeHint is used to preallocate the number of shards that will be used. Set to -1 for unknown
func NewShardedDagIndexView(content ipld.Link, shardSizeHint int) ShardedDagIndexView {
	return &shardedDagIndex{content, NewMultihashMap[MultihashMap[Position]](shardSizeHint)}
}

func (sdi *shardedDagIndex) Content() ipld.Link {
	return sdi.content
}

func (sdi *shardedDagIndex) Shards() MultihashMap[MultihashMap[Position]] {
	return sdi.shards
}

func (sdi *shardedDagIndex) SetSlice(shard mh.Multihash, slice mh.Multihash, pos Position) {
	index := sdi.shards.Get(shard)
	if index == nil {
		index = NewMultihashMap[Position](-1)
		sdi.shards.Set(shard, index)
	}
	index.Set(slice, pos)
}

func (sdi *shardedDagIndex) Archive() (io.Reader, error) {
	return Archive(sdi)
}

// UnknownFormatError indicates an error attempting to read
// the car file
type UnknownFormatError struct {
	failure.NamedWithStackTrace
	reason error
}

// NewUnknownFormatError returns an ExtractError for an unknown format
func NewUnknownFormatError(reason error) ExtractError {
	return UnknownFormatError{failure.NamedWithCurrentStackTrace("UnknownFormat"), reason}
}

func (ufe UnknownFormatError) isExtractError() {}

func (ufe UnknownFormatError) Unwrap() error {
	return ufe.reason
}

func (ufe UnknownFormatError) Error() string {
	return fmt.Sprintf("unknown format: %s", ufe.reason.Error())
}

// DecodeFailureErorr indicates an error in the structure of the
// ShardedDagIndex
type DecodeFailureErorr struct {
	failure.NamedWithStackTrace
	reason error
}

// NewDecodeFailureError returns an ExtractError for a decode failure
func NewDecodeFailureError(reason error) ExtractError {
	return DecodeFailureErorr{failure.NamedWithCurrentStackTrace("DecodeFailure"), reason}
}

func (dfe DecodeFailureErorr) isExtractError() {}

func (dfe DecodeFailureErorr) Unwrap() error {
	return dfe.reason
}

func (dfe DecodeFailureErorr) Error() string {
	return fmt.Sprintf("decode failure: %s", dfe.reason.Error())
}

// Archive writes a ShardedDagIndex to a CAR file
func Archive(model ShardedDagIndex) (io.Reader, error) {
	// assemble blob index shards
	blobIndexDatas, err := toList(model.Shards(), func(shardHash mh.Multihash, shard MultihashMap[Position]) (dm.BlobIndexModel, error) {
		// assemble blob slices
		blobSliceDatas, err := toList(shard, func(sliceHash mh.Multihash, pos Position) (dm.BlobSliceModel, error) {
			return dm.BlobSliceModel{Multihash: sliceHash, Position: pos}, nil
		})
		if err != nil {
			return dm.BlobIndexModel{}, err
		}
		// sort blob slices
		if err := sortByMultihash(blobSliceDatas, func(bsm dm.BlobSliceModel) mh.Multihash {
			return bsm.Multihash
		}); err != nil {
			return dm.BlobIndexModel{}, err
		}
		return dm.BlobIndexModel{
			Multihash: shardHash,
			Slices:    blobSliceDatas,
		}, nil
	})
	if err != nil {
		return nil, err
	}
	// sort blob index shards
	if err := sortByMultihash(blobIndexDatas, func(bim dm.BlobIndexModel) mh.Multihash {
		return bim.Multihash
	}); err != nil {
		return nil, err
	}

	// initialize root sharded dag index
	shardedDagIndex := dm.ShardedDagIndexModel_0_1{
		Content: model.Content(),
		Shards:  make([]ipld.Link, 0, len(blobIndexDatas)),
	}
	// encode blob index shards to blocks and add links to sharded dag index
	blks := make([]ipld.Block, 0, len(blobIndexDatas)+1)
	for _, shard := range blobIndexDatas {
		blk, err := block.Encode(&shard, dm.BlobIndexSchema(), cbor.Codec, sha256.Hasher)
		if err != nil {
			return nil, err
		}
		blks = append(blks, blk)
		shardedDagIndex.Shards = append(shardedDagIndex.Shards, blk.Link())
	}
	// encode the root block
	rootBlk, err := block.Encode(&dm.ShardedDagIndexModel{
		DagO_1: &shardedDagIndex,
	}, dm.ShardedDagIndexSchema(), cbor.Codec, sha256.Hasher)
	if err != nil {
		return nil, err
	}
	// add the root block to the block list
	blks = append(blks, rootBlk)

	// encode the CAR file
	return car.Encode([]ipld.Link{rootBlk.Link()}, func(yield func(block.Block, error) bool) {
		for _, b := range blks {
			if !yield(b, nil) {
				return
			}
		}
	}), nil
}

func toList[E, T any](mhm MultihashMap[T], newElem func(mh.Multihash, T) (E, error)) ([]E, error) {
	asList := make([]E, 0, mhm.Size())
	for hash, value := range mhm.Iterator() {
		e, err := newElem(hash, value)
		if err != nil {
			return nil, err
		}
		asList = append(asList, e)
	}
	return asList, nil
}

func sortByMultihash[E any](list []E, getMultihash func(E) mh.Multihash) error {
	decodeds := NewMultihashMap[*mh.DecodedMultihash](len(list))
	for _, e := range list {
		hash := getMultihash(e)
		decoded, err := mh.Decode(hash)
		if err != nil {
			return err
		}
		decodeds.Set(hash, decoded)
	}
	slices.SortFunc(list, func(a, b E) int {
		decodedA := decodeds.Get(getMultihash(a))
		decodedB := decodeds.Get(getMultihash(b))
		return bytes.Compare(decodedA.Digest, decodedB.Digest)
	})
	return nil
}
