package service

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/ipni/go-libipni/maurl"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/internal/bytemap"
	"github.com/storacha/indexing-service/pkg/internal/jobwalker"
	"github.com/storacha/indexing-service/pkg/internal/jobwalker/parallelwalk"
	"github.com/storacha/indexing-service/pkg/internal/jobwalker/singlewalk"
	"github.com/storacha/indexing-service/pkg/metadata"
	"github.com/storacha/indexing-service/pkg/service/providerindex"
	"github.com/storacha/indexing-service/pkg/service/queryresult"
	"github.com/storacha/indexing-service/pkg/types"
)

// Match narrows parameters for locating providers/claims for a set of multihashes
type Match struct {
	Subject []did.DID
}

// Query is a query for several multihashes
type Query struct {
	Hashes []multihash.Multihash
	Match  Match
}

// ProviderIndex is a read/write interface to a local cache of providers that falls back to IPNI
type ProviderIndex interface {
	// Find should do the following
	//  1. Read from the IPNI Storage cache to get a list of providers
	//     a. If there is no record in cache, query IPNI, filter out any non-content claims metadata, and store
	//     the resulting records in the cache
	//     b. the are no records in the cache or IPNI, it can attempt to read from legacy systems -- Dynamo tables & content claims storage, synthetically constructing provider results
	//  2. With returned provider results, filter additionally for claim type. If space dids are set, calculate an encodedcontextid's by hashing space DID and Hash, and filter for a matching context id
	//     Future TODO: kick off a conversion task to update the recrds
	Find(context.Context, providerindex.QueryKey) ([]model.ProviderResult, error)
	// Publish should do the following:
	// 1. Write the entries to the cache with no expiration until publishing is complete
	// 2. Generate an advertisement for the advertised hashes and publish/announce it
	Publish(context.Context, []multihash.Multihash, model.ProviderResult)
}

// ClaimLookup is used to get full claims from a claim cid
type ClaimLookup interface {
	// LookupClaim should:
	// 1. attempt to read the claim from the cache from the encoded contextID
	// 2. if not found, attempt to fetch the claim from the provided URL. Store the result in cache
	// 3. return the claim
	LookupClaim(ctx context.Context, claimCid cid.Cid, fetchURL url.URL) (delegation.Delegation, error)
}

// BlobIndexLookup is a read through cache for fetching blob indexes
type BlobIndexLookup interface {
	// Find should:
	// 1. attempt to read the sharded dag index from the cache from the encoded contextID
	// 2. if not found, attempt to fetch the index from the provided URL. Store the result in cache
	// 3. return the index
	// 4. asyncronously, add records to the ProviderStore from the parsed blob index so that we can avoid future queries to IPNI for
	// other multihashes in the index
	Find(ctx context.Context, contextID types.EncodedContextID, provider model.ProviderResult, fetchURL url.URL, rng *metadata.Range) (blobindex.ShardedDagIndexView, error)
}

// IndexingService implements read/write logic for indexing data with IPNI, content claims, sharded dag indexes, and a cache layer
type IndexingService struct {
	blobIndexLookup BlobIndexLookup
	claimLookup     ClaimLookup
	providerIndex   ProviderIndex
	jobWalker       jobwalker.JobWalker[job, queryState]
}

type job struct {
	mh                  multihash.Multihash
	indexForMh          *multihash.Multihash
	indexProviderRecord *model.ProviderResult
	jobType             jobType
}

type jobKey string

func (j job) key() jobKey {
	k := jobKey(j.mh) + jobKey(j.jobType)
	if j.indexForMh != nil {
		k += jobKey(*j.indexForMh)
	}
	return k
}

type jobType string

const standardJobType jobType = "standard"
const locationJobType jobType = "location"
const equalsOrLocationJobType jobType = "equals_or_location"

var targetClaims = map[jobType][]multicodec.Code{
	standardJobType:         {metadata.EqualsClaimID, metadata.IndexClaimID, metadata.LocationCommitmentID},
	locationJobType:         {metadata.LocationCommitmentID},
	equalsOrLocationJobType: {metadata.IndexClaimID, metadata.LocationCommitmentID},
}

type queryResult struct {
	Claims  map[cid.Cid]delegation.Delegation
	Indexes bytemap.ByteMap[types.EncodedContextID, blobindex.ShardedDagIndexView]
}

type queryState struct {
	q      *Query
	qr     *queryResult
	visits map[jobKey]struct{}
}

func (is *IndexingService) jobHandler(mhCtx context.Context, j job, spawn func(job) error, state jobwalker.WrappedState[queryState]) error {

	// check if node has already been visited and ignore if that is the case
	if !state.CmpSwap(func(qs queryState) bool {
		_, ok := qs.visits[j.key()]
		return !ok
	}, func(qs queryState) queryState {
		qs.visits[j.key()] = struct{}{}
		return qs
	}) {
		return nil
	}

	// find provider records related to this multihash
	results, err := is.providerIndex.Find(mhCtx, providerindex.QueryKey{
		Hash:         j.mh,
		Spaces:       state.Access().q.Match.Subject,
		TargetClaims: targetClaims[j.jobType],
	})
	if err != nil {
		return err
	}
	for _, result := range results {
		// unmarshall metadata for this provider
		md := metadata.MetadataContext.New()
		err = md.UnmarshalBinary(result.Metadata)
		if err != nil {
			return err
		}
		// the provider may list one or more protocols for this CID
		// in our case, the protocols are just differnt types of content claims
		for _, code := range md.Protocols() {
			protocol := md.Get(code)
			// make sure this is some kind of claim protocol, ignore if not
			hasClaimCid, ok := protocol.(metadata.HasClaim)
			if !ok {
				continue
			}
			// fetch (from cache or url) the actual content claim
			claimCid := hasClaimCid.GetClaim()
			url, err := is.fetchClaimURL(*result.Provider, claimCid)
			if err != nil {
				return err
			}
			claim, err := is.claimLookup.LookupClaim(mhCtx, claimCid, *url)
			if err != nil {
				return err
			}
			// add the fetched claim to the results, if we don't already have it
			state.CmpSwap(
				func(qs queryState) bool {
					_, ok := qs.qr.Claims[claimCid]
					return !ok
				},
				func(qs queryState) queryState {
					qs.qr.Claims[claimCid] = claim
					return qs
				})

			// handle each type of protocol
			switch typedProtocol := protocol.(type) {
			case *metadata.EqualsClaimMetadata:
				// for an equals claim, it's published on both the content and equals multihashes
				// we follow with a query for location claim on the OTHER side of the multihash
				if string(typedProtocol.Equals.Hash()) != string(j.mh) {
					// lookup was the content hash, queue the equals hash
					if err := spawn(job{typedProtocol.Equals.Hash(), nil, nil, locationJobType}); err != nil {
						return err
					}
				} else {
					// lookup was the equals hash, queue the content hash
					if err := spawn(job{multihash.Multihash(result.ContextID), nil, nil, locationJobType}); err != nil {
						return err
					}
				}
			case *metadata.IndexClaimMetadata:
				// for an index claim, we follow by looking for a location claim for the index, and fetching the index
				mh := j.mh
				if err := spawn(job{typedProtocol.Index.Hash(), &mh, &result, equalsOrLocationJobType}); err != nil {
					return err
				}
			case *metadata.LocationCommitmentMetadata:
				// for a location claim, we just store it, unless its for an index CID, in which case get the full idnex
				if j.indexForMh != nil {
					// fetch (from URL or cache) the full index
					shard := typedProtocol.Shard
					if shard == nil {
						c := cid.NewCidV1(cid.Raw, j.mh)
						shard = &c
					}
					url, err := is.fetchRetrievalURL(*result.Provider, *shard)
					if err != nil {
						return err
					}
					index, err := is.blobIndexLookup.Find(mhCtx, result.ContextID, *j.indexProviderRecord, *url, typedProtocol.Range)
					if err != nil {
						return err
					}
					// Add the index to the query results, if we don't already have it
					state.CmpSwap(
						func(qs queryState) bool {
							return !qs.qr.Indexes.Has(result.ContextID)
						},
						func(qs queryState) queryState {
							qs.qr.Indexes.Set(result.ContextID, index)
							return qs
						})

					// add location queries for all shards containing the original CID we're seeing an index for
					shards := index.Shards().Iterator()
					for shard, index := range shards {
						if index.Has(*j.indexForMh) {
							if err := spawn(job{shard, nil, nil, equalsOrLocationJobType}); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	return nil
}

// Query returns back relevant content claims for the given query using the following steps
// 1. Query the IPNIIndex for all matching records
// 2. For any index records, query the IPNIIndex for any location claims for that index cid
// 3. For any index claims, query the IPNIIndex for location claims for the index cid
// 4. Query the BlobIndexLookup to get the full ShardedDagIndex for any index claims
// 5. Query IPNIIndex for any location claims for any shards that contain the multihash based on the ShardedDagIndex
// 6. Read the requisite claims from the ClaimLookup
// 7. Return all discovered claims and sharded dag indexes
func (is *IndexingService) Query(ctx context.Context, q Query) (queryresult.QueryResult, error) {
	initialJobs := make([]job, 0, len(q.Hashes))
	for _, mh := range q.Hashes {
		initialJobs = append(initialJobs, job{mh, nil, nil, standardJobType})
	}
	qs, err := is.jobWalker(ctx, initialJobs, queryState{
		q: &q,
		qr: &queryResult{
			Claims:  make(map[cid.Cid]delegation.Delegation),
			Indexes: bytemap.NewByteMap[types.EncodedContextID, blobindex.ShardedDagIndexView](-1),
		},
		visits: map[jobKey]struct{}{},
	}, is.jobHandler)
	if err != nil {
		return nil, err
	}
	return queryresult.Build(qs.qr.Claims, qs.qr.Indexes)
}

func (is *IndexingService) urlForResource(provider peer.AddrInfo, resourceType string, resourceID string) (*url.URL, error) {
	for _, addr := range provider.Addrs {
		// first, attempt to convert the addr to a url scheme
		url, err := maurl.ToURL(addr)
		// if it can't be converted, skip
		if err != nil {
			continue
		}
		// must be an http url
		if !(url.Scheme == "http" || url.Scheme == "https") {
			continue
		}
		// we must have a place to place the resourceId in the path
		if !strings.Contains(url.Path, resourceType) {
			continue
		}
		// ok we have a matching URL, return with all resource type components replaced with the id
		url.Path = strings.ReplaceAll(url.Path, resourceType, resourceID)
		return url, nil
	}
	return nil, errors.New("no claim endpoint found")
}

func (is *IndexingService) fetchClaimURL(provider peer.AddrInfo, claimCid cid.Cid) (*url.URL, error) {
	return is.urlForResource(provider, "{claim}", claimCid.String())
}

func (is *IndexingService) fetchRetrievalURL(provider peer.AddrInfo, shard cid.Cid) (*url.URL, error) {
	return is.urlForResource(provider, "{shard}", shard.String())
}

// CacheClaim is used to cache a claim without publishing it to IPNI
// this is used cache a location commitment that come from a storage provider on blob/accept, without publishing, since the SP will publish themselves
// (a delegation for a location commitment is already generated on blob/accept)
// ideally however, IPNI would enable UCAN chains for publishing so that we could publish it directly from the storage service
// it doesn't for now, so we let SPs publish themselves them direct cache with us
func (is *IndexingService) CacheClaim(ctx context.Context, claim delegation.Delegation) error {
	return errors.New("not implemented")
}

// PublishClaim caches and publishes a content claim
// I imagine publish claim to work as follows
// For all claims except index, just use the publish API on IPNIIndex
// For index claims, let's assume they fail if a location claim for the index car cid is not already published
// The service should lookup the index cid location claim, and fetch the ShardedDagIndexView, then use the hashes inside
// to assemble all the multihashes in the index advertisement
func (is *IndexingService) PublishClaim(ctx context.Context, claim delegation.Delegation) error {
	return errors.New("not implemented")
}

// Option configures an IndexingService
type Option func(is *IndexingService)

// WithConcurrency causes the indexing service to process find queries parallel, with the given concurrency
func WithConcurrency(concurrency int) Option {
	return func(is *IndexingService) {
		is.jobWalker = parallelwalk.NewParallelWalk[job, queryState](concurrency)
	}
}

// NewIndexingService returns a new indexing service
func NewIndexingService(blobIndexLookup BlobIndexLookup, claimLookup ClaimLookup, providerIndex ProviderIndex, options ...Option) *IndexingService {
	is := &IndexingService{
		blobIndexLookup: blobIndexLookup,
		claimLookup:     claimLookup,
		providerIndex:   providerIndex,
		jobWalker:       singlewalk.SingleWalker[job, queryState],
	}
	for _, option := range options {
		option(is)
	}
	return is
}
