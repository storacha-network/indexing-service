package publisher

import (
	"context"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/indexing-service/pkg/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestPublish(t *testing.T) {
	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	provInfo := peer.AddrInfo{ID: pid}

	ctx := context.Background()

	t.Run("single advert", func(t *testing.T) {
		publisher, err := New(priv)
		require.NoError(t, err)

		err = publisher.Start(ctx)
		require.NoError(t, err)

		defer func() {
			err := publisher.Shutdown()
			require.NoError(t, err)
		}()

		digests := testutil.RandomMultihashes(rand.IntN(10) + 1)
		adlnk, err := publisher.Publish(ctx, &provInfo, testutil.RandomCID().String(), digests, metadata.Default.New())
		require.NoError(t, err)

		ad, err := publisher.Store().Advert(ctx, adlnk)
		require.NoError(t, err)

		var ents []multihash.Multihash
		for e, err := range publisher.Store().Entries(ctx, ad.Entries) {
			require.NoError(t, err)
			ents = append(ents, e)
		}

		require.Equal(t, digests, ents)
	})

	t.Run("single advert, chunked entries", func(t *testing.T) {
		publisher, err := New(priv)
		require.NoError(t, err)

		err = publisher.Start(ctx)
		require.NoError(t, err)

		defer func() {
			err := publisher.Shutdown()
			require.NoError(t, err)
		}()

		digests := testutil.RandomMultihashes(maxEntryChunkSize + 1)
		adlnk, err := publisher.Publish(ctx, &provInfo, testutil.RandomCID().String(), digests, metadata.Default.New())
		require.NoError(t, err)

		ad, err := publisher.Store().Advert(ctx, adlnk)
		require.NoError(t, err)

		var estrs []string
		for e, err := range publisher.Store().Entries(ctx, ad.Entries) {
			require.NoError(t, err)
			estrs = append(estrs, e.B58String())
		}
		sort.Strings(estrs)

		var dstrs []string
		for _, d := range digests {
			dstrs = append(dstrs, d.B58String())
		}
		sort.Strings(dstrs)

		require.Equal(t, len(digests), len(estrs))
		require.Equal(t, dstrs, estrs)
	})

	t.Run("multiple adverts", func(t *testing.T) {
		publisher, err := New(priv)
		require.NoError(t, err)

		err = publisher.Start(ctx)
		require.NoError(t, err)

		defer func() {
			err := publisher.Shutdown()
			require.NoError(t, err)
		}()

		var adLinks []ipld.Link
		var contextIDs []string
		var digestLists [][]multihash.Multihash
		for range 1 + rand.IntN(100) {
			ctxid := testutil.RandomCID().String()
			digests := testutil.RandomMultihashes(1 + rand.IntN(100))

			l, err := publisher.Publish(ctx, &provInfo, ctxid, digests, metadata.Default.New())
			require.NoError(t, err)

			adLinks = append(adLinks, l)
			contextIDs = append(contextIDs, ctxid)
			digestLists = append(digestLists, digests)
		}

		for i, adLink := range adLinks {
			ad, err := publisher.Store().Advert(ctx, adLink)
			require.NoError(t, err)

			var digests []multihash.Multihash
			for e, err := range publisher.Store().Entries(ctx, ad.Entries) {
				require.NoError(t, err)
				digests = append(digests, e)
			}

			require.Equal(t, contextIDs[i], string(ad.ContextID))
			require.Equal(t, digestLists[i], digests)
		}
	})
}
