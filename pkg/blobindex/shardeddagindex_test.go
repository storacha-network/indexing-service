package blobindex_test

import (
	"io"
	"testing"

	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestFromToArchive(t *testing.T) {
	roots, contentCar := testutil.RandomCAR(32)
	contentCarBytes, _ := io.ReadAll(contentCar)
	index, err := blobindex.FromShardArchives(roots[0], [][]byte{contentCarBytes})
	require.NoError(t, err)
	r, err := index.Archive()
	require.NoError(t, err)
	newIndex, err := blobindex.Extract(r)
	require.NoError(t, err)
	require.NotZero(t, newIndex.Shards().Size())
	require.Equal(t, index.Shards().Size(), newIndex.Shards().Size())
	for key, shard := range newIndex.Shards().Iterator() {
		require.True(t, index.Shards().Has(key))
		require.Equal(t, index.Shards().Get(key).Size(), shard.Size())
	}
}
