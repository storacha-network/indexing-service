package providercacher_test

import (
	"context"
	"testing"

	"github.com/ipni/go-libipni/find/model"
	"github.com/multiformats/go-multihash"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/internal/testutil"
	"github.com/storacha/indexing-service/pkg/service/providercacher"
	"github.com/storacha/indexing-service/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestSimpleProviderCacher_CacheProviderForIndexRecords(t *testing.T) {

	// Create a test context
	ctx := context.Background()

	// Create test providers
	testProvider := testutil.RandomProviderResult()
	testProvider2 := testutil.RandomProviderResult()

	// Create a test index with random CIDs
	testCid1 := testutil.RandomCID()
	shardIndex := blobindex.NewShardedDagIndexView(testCid1, 2)

	shardMhs := testutil.RandomMultihashes(2)
	sliceMhs := testutil.RandomMultihashes(6)
	for i := range 2 {
		for j := range 3 {
			shardIndex.SetSlice(shardMhs[i], sliceMhs[i*3+j], blobindex.Position{})
		}
	}

	shardIndex2 := blobindex.NewShardedDagIndexView(testutil.RandomCID(), 2)
	for j := range 2 {
		shardIndex2.SetSlice(shardMhs[0], sliceMhs[j], blobindex.Position{})
	}

	evensFilled := func() map[string][]model.ProviderResult {
		starter := make(map[string][]model.ProviderResult)
		for i, sliceMh := range sliceMhs {
			if i%2 == 0 {
				starter[sliceMh.String()] = []model.ProviderResult{testProvider}
			}
		}
		return starter
	}

	// Define test cases
	testCases := []struct {
		name          string
		provider      model.ProviderResult
		index         blobindex.ShardedDagIndexView
		getErr        error
		setErr        error
		initialStore  map[string][]model.ProviderResult
		expectedCount uint64
		expectedErr   error
		testStore     func(t *testing.T, store map[string][]model.ProviderResult)
	}{
		{
			name:          "Cache new provider",
			provider:      testProvider,
			index:         shardIndex,
			expectedCount: 6,
			expectedErr:   nil,
			testStore: func(t *testing.T, store map[string][]model.ProviderResult) {
				require.Len(t, store, 6)
				for _, sliceMh := range sliceMhs {
					require.Equal(t, store[sliceMh.String()], []model.ProviderResult{testProvider})
				}
			},
		},
		{
			name:          "Cache provider already present",
			provider:      testProvider,
			index:         shardIndex,
			initialStore:  evensFilled(),
			expectedCount: 3,
			expectedErr:   nil,
			testStore: func(t *testing.T, store map[string][]model.ProviderResult) {
				require.Len(t, store, 6)
				for _, sliceMh := range sliceMhs {
					require.Equal(t, store[sliceMh.String()], []model.ProviderResult{testProvider})
				}
			},
		},
		{
			name:          "Cache another provider on top",
			provider:      testProvider2,
			index:         shardIndex,
			initialStore:  evensFilled(),
			expectedCount: 6,
			expectedErr:   nil,
			testStore: func(t *testing.T, store map[string][]model.ProviderResult) {
				require.Len(t, store, 6)
				for i, sliceMh := range sliceMhs {
					expected := []model.ProviderResult{testProvider2}
					if i%2 == 0 {
						expected = []model.ProviderResult{testProvider, testProvider2}
					}
					require.Equal(t, store[sliceMh.String()], expected)
				}
			},
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Initialize mock store
			initialStore := tc.initialStore
			if initialStore == nil {
				initialStore = make(map[string][]model.ProviderResult)
			}
			mockStore := &MockProviderStore{
				setErr: tc.setErr,
				getErr: tc.getErr,
				store:  initialStore,
			}

			// Create SimpleProviderCacher instance
			cacher := providercacher.NewSimpleProviderCacher(mockStore)

			count, err := cacher.CacheProviderForIndexRecords(ctx, tc.provider, tc.index)
			if tc.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.expectedErr.Error())
			}
			require.Equal(t, tc.expectedCount, count)
			if tc.testStore != nil {
				tc.testStore(t, mockStore.store)
			}
		})
	}
}

// MockProviderStore is a mock implementation of the ProviderStore interface
type MockProviderStore struct {
	setErr, getErr error
	store          map[string][]model.ProviderResult
}

var _ types.ProviderStore = &MockProviderStore{}

func (m *MockProviderStore) Get(ctx context.Context, hash multihash.Multihash) ([]model.ProviderResult, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	results, exists := m.store[hash.String()]
	if !exists {
		return nil, types.ErrKeyNotFound
	}
	return results, nil
}

func (m *MockProviderStore) Set(ctx context.Context, hash multihash.Multihash, providers []model.ProviderResult, expires bool) error {
	if m.setErr != nil {
		return m.setErr
	}
	m.store[hash.String()] = providers
	return nil
}

// SetExpirable implements types.ProviderStore.
func (m *MockProviderStore) SetExpirable(ctx context.Context, key multihash.Multihash, expires bool) error {
	panic("unimplemented")
}
