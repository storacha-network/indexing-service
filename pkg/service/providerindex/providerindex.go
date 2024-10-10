package providerindex

import (
	"bytes"
	"context"
	"fmt"
	"slices"

	cid "github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipnifind "github.com/ipni/go-libipni/find/client"
	"github.com/ipni/go-libipni/find/model"
	meta "github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/indexing-service/pkg/internal/jobqueue"
	"github.com/storacha/indexing-service/pkg/metadata"
	"github.com/storacha/indexing-service/pkg/service/providerindex/publisher"
	"github.com/storacha/indexing-service/pkg/types"
)

var log = logging.Logger("providerindex")

type QueryKey struct {
	Spaces       []did.DID
	Hash         mh.Multihash
	TargetClaims []multicodec.Code
}

// ProviderIndex is a read/write interface to a local cache of providers that falls back to IPNI
type ProviderIndex struct {
	providerStore types.ProviderStore
	findClient    ipnifind.Finder
	publisher     publisher.Publisher
}

// TBD access to legacy systems
type LegacySystems interface{}

func NewProviderIndex(providerStore types.ProviderStore, findClient ipnifind.Finder, publisher publisher.Publisher, legacySystems LegacySystems) *ProviderIndex {
	publisher.NotifyRemoteSync(func(ctx context.Context, head, prev ipld.Link) {
		HandleRemoteSync(ctx, providerStore, publisher, head, prev)
	})

	return &ProviderIndex{
		providerStore: providerStore,
		findClient:    findClient,
		publisher:     publisher,
	}
}

func HandleRemoteSync(ctx context.Context, providerStore types.ProviderStore, publisher publisher.Publisher, head, prev ipld.Link) {
	log.Infof("handling IPNI remote sync from %s to %s", prev, head)

	q := jobqueue.NewJobQueue(
		func(ctx context.Context, digest mh.Multihash) error {
			return providerStore.SetExpirable(ctx, digest, true)
		},
		jobqueue.WithConcurrency(5),
		jobqueue.WithErrorHandler(func(err error) {
			log.Errorf("setting expirable: %w", err)
		}),
	)
	q.Startup()

	cur := head
	for {
		ad, err := publisher.Store().Advert(ctx, cur)
		if err != nil {
			log.Errorf("getting advert: %s: %w", cur, err)
			return
		}
		for d, err := range publisher.Store().Entries(ctx, ad.Entries) {
			if err != nil {
				log.Errorf("iterating advert entries: %s (advert) -> %s (entries): %w", cur, ad.Entries, err)
				return
			}
			err := q.Queue(ctx, d)
			if err != nil {
				log.Errorf("adding digest to queue: %s: %w", d.B58String(), err)
				return
			}
		}
		if ad.PreviousCid() == cid.Undef || ad.PreviousCid().String() == prev.String() {
			break
		}
		cur = cidlink.Link{Cid: ad.PreviousCid()}
	}

	err := q.Shutdown(ctx)
	if err != nil {
		log.Errorf("shutting down IPNI remote sync job queue: %w", err)
	}
	log.Infof("handled IPNI remote sync from %s to %s", prev, head)
}

// Find should do the following
//  1. Read from the IPNI Storage cache to get a list of providers
//     a. If there is no record in cache, query IPNI, filter out any non-content claims metadata, and store
//     the resulting records in the cache
//     b. the are no records in the cache or IPNI, it can attempt to read from legacy systems -- Dynamo tables & content claims storage, synthetically constructing provider results
//  2. With returned provider results, filter additionally for claim type. If space dids are set, calculate an encodedcontextid's by hashing space DID and Hash, and filter for a matching context id
//     Future TODO: kick off a conversion task to update the recrds
func (pi *ProviderIndex) Find(ctx context.Context, qk QueryKey) ([]model.ProviderResult, error) {
	results, err := pi.getProviderResults(ctx, qk.Hash)
	if err != nil {
		return nil, err
	}
	results, err = pi.filteredCodecs(results, qk.TargetClaims)
	if err != nil {
		return nil, err
	}
	return pi.filterBySpace(results, qk.Hash, qk.Spaces)
}

func (pi *ProviderIndex) getProviderResults(ctx context.Context, mh mh.Multihash) ([]model.ProviderResult, error) {
	res, err := pi.providerStore.Get(ctx, mh)
	if err == nil {
		return res, nil
	}
	if err != types.ErrKeyNotFound {
		return nil, err
	}

	findRes, err := pi.findClient.Find(ctx, mh)
	if err != nil {
		return nil, err
	}
	var results []model.ProviderResult
	for _, mhres := range findRes.MultihashResults {
		results = append(results, mhres.ProviderResults...)
	}
	err = pi.providerStore.Set(ctx, mh, results, true)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (pi *ProviderIndex) filteredCodecs(results []model.ProviderResult, codecs []multicodec.Code) ([]model.ProviderResult, error) {
	if len(codecs) == 0 {
		return results, nil
	}
	return filter(results, func(result model.ProviderResult) (bool, error) {
		md := metadata.MetadataContext.New()
		err := md.UnmarshalBinary(result.Metadata)
		if err != nil {
			return false, err
		}
		return slices.ContainsFunc(codecs, func(code multicodec.Code) bool {
			return slices.ContainsFunc(md.Protocols(), func(mdCode multicodec.Code) bool {
				return mdCode == code
			})
		}), nil
	})
}

func (pi *ProviderIndex) filterBySpace(results []model.ProviderResult, mh mh.Multihash, spaces []did.DID) ([]model.ProviderResult, error) {
	if len(spaces) == 0 {
		return results, nil
	}
	encryptedIds := make([]types.EncodedContextID, 0, len(spaces))
	for _, space := range spaces {
		encryptedId, err := types.ContextID{
			Space: &space,
			Hash:  mh,
		}.ToEncoded()
		if err != nil {
			return nil, err
		}
		encryptedIds = append(encryptedIds, encryptedId)
	}

	filtered, err := filter(results, func(result model.ProviderResult) (bool, error) {
		return slices.ContainsFunc(encryptedIds, func(encyptedID types.EncodedContextID) bool {
			return bytes.Equal(result.ContextID, encyptedID)
		}), nil
	})
	if err != nil {
		return nil, err
	}
	if len(filtered) > 0 {
		return filtered, nil
	}
	return results, nil
}

// Publish should do the following:
// 1. Write the entries to the cache with no expiration until publishing is complete
// 2. Generate an advertisement for the advertised hashes and publish/announce it
func (pi *ProviderIndex) Publish(ctx context.Context, provider *peer.AddrInfo, contextID string, digests []mh.Multihash, meta meta.Metadata) error {
	mdb, err := meta.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	pr := model.ProviderResult{
		ContextID: []byte(contextID),
		Metadata:  mdb,
		Provider:  provider,
	}

	q := jobqueue.NewJobQueue(
		func(ctx context.Context, digest mh.Multihash) error {
			return appendProviderResult(ctx, pi.providerStore, digest, pr)
		},
		jobqueue.WithConcurrency(5),
		jobqueue.WithErrorHandler(func(err error) {
			log.Errorf("appending provider result: %w", err)
		}),
	)
	q.Startup()
	for _, d := range digests {
		err := q.Queue(ctx, d)
		if err != nil {
			return err
		}
	}
	q.Shutdown(ctx)

	id, err := pi.publisher.Publish(ctx, provider, contextID, digests, meta)
	if err != nil {
		return err
	}
	log.Infof("published IPNI advert: %s", id)
	return nil
}

// TODO: atomic append...
func appendProviderResult(ctx context.Context, providerStore types.ProviderStore, digest mh.Multihash, meta model.ProviderResult) error {
	metas, err := providerStore.Get(ctx, digest)
	if err != nil {
		if err != types.ErrKeyNotFound {
			return fmt.Errorf("getting existing provider results for digest: %s: %w", digest.B58String(), err)
		}
	}
	metas = append(metas, meta)
	err = providerStore.Set(ctx, digest, metas, false)
	if err != nil {
		return fmt.Errorf("setting provider results for digest: %s: %w", digest.B58String(), err)
	}
	return nil
}

func filter(results []model.ProviderResult, filterFunc func(model.ProviderResult) (bool, error)) ([]model.ProviderResult, error) {

	filtered := make([]model.ProviderResult, 0, len(results))
	for _, result := range results {
		include, err := filterFunc(result)
		if err != nil {
			return nil, err
		}
		if include {
			filtered = append(filtered, result)
		}
	}
	return filtered, nil
}
