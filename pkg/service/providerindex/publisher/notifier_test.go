package publisher

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/find/model"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/storacha/indexing-service/pkg/internal/testutil"
	"github.com/stretchr/testify/require"
)

func mockIpniApi(id peer.ID) (*httptest.Server, []ipld.Link) {
	var ads []ipld.Link
	for range 10 {
		ads = append(ads, testutil.RandomCID())
	}

	n := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n >= len(ads) {
			w.WriteHeader(500)
			return
		}
		resp := model.ProviderInfo{
			AddrInfo:          peer.AddrInfo{ID: id},
			LastAdvertisement: ads[n].(cidlink.Link).Cid,
		}
		bytes, _ := json.Marshal(resp)
		w.Write(bytes)
		n++
	}))

	return ts, ads
}

func TestNotifier(t *testing.T) {
	NotifierPollInterval = time.Millisecond

	priv, _, err := crypto.GenerateEd25519Key(nil)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(priv)
	require.NoError(t, err)

	t.Run("notifies all CIDs", func(t *testing.T) {
		ts, ads := mockIpniApi(pid)
		defer ts.Close()

		notifier, err := NewRemoteSyncNotifier(ts.URL, pid, nil)
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(len(ads))

		var notifications []ipld.Link
		notifier.Notify(func(ctx context.Context, head, prev ipld.Link) {
			notifications = append(notifications, head)
			wg.Done()
		})

		notifier.Start(context.Background())
		wg.Wait()
		notifier.Stop()

		require.Equal(t, ads, notifications)
	})

	t.Run("notifies all CIDs with known head", func(t *testing.T) {
		ts, chain := mockIpniApi(pid)
		defer ts.Close()

		notifier, err := NewRemoteSyncNotifier(ts.URL, pid, chain[0])
		require.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(len(chain) - 1)

		var notifications []ipld.Link
		notifier.Notify(func(ctx context.Context, head, prev ipld.Link) {
			notifications = append(notifications, head)
			wg.Done()
		})

		notifier.Start(context.Background())
		wg.Wait()
		notifier.Stop()

		require.Equal(t, chain[1:], notifications)
	})
}
