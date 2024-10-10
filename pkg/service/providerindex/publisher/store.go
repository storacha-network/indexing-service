package publisher

import (
	"context"
	"iter"

	"github.com/ipfs/go-datastore"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/ingest/schema"
	"github.com/multiformats/go-multihash"
)

type AdvertStore interface {
	// Advert retrieves an existing advert from the store.
	Advert(ctx context.Context, id ipld.Link) (schema.Advertisement, error)
	// Entries returns an iterable of multihashes from the store for the
	// given root of an existing advertisement entries chain.
	Entries(ctx context.Context, root ipld.Link) iter.Seq2[multihash.Multihash, error]
}

type AdStore struct {
	adverts datastore.Batching
	entries datastore.Batching
}

func (s *AdStore) Advert(ctx context.Context, id ipld.Link) (schema.Advertisement, error) {
	return Advert(ctx, s.adverts, id)
}

func (s *AdStore) Entries(ctx context.Context, root ipld.Link) iter.Seq2[multihash.Multihash, error] {
	return Entries(ctx, s.entries, root)
}

func NewAdvertStore(adverts, entries datastore.Batching) *AdStore {
	return &AdStore{adverts, entries}
}

func Advert(ctx context.Context, ds datastore.Batching, id ipld.Link) (schema.Advertisement, error) {
	var ad schema.Advertisement
	v, err := ds.Get(ctx, datastore.NewKey(id.String()))
	if err != nil {
		return ad, err
	}
	ad, err = schema.BytesToAdvertisement(asCID(id), v)
	if err != nil {
		return ad, err
	}
	return ad, nil
}

func Entries(ctx context.Context, ds datastore.Batching, root ipld.Link) iter.Seq2[multihash.Multihash, error] {
	return func(yield func(multihash.Multihash, error) bool) {
		cur := root
		for cur != nil && cur != schema.NoEntries {
			v, err := ds.Get(ctx, datastore.NewKey(cur.String()))
			if err != nil {
				yield(nil, err)
				return
			}

			ent, err := schema.BytesToEntryChunk(asCID(cur), v)
			if err != nil {
				yield(nil, err)
				return
			}

			for _, d := range ent.Entries {
				if !yield(d, nil) {
					return
				}
			}

			cur = ent.Next
		}
	}
}
