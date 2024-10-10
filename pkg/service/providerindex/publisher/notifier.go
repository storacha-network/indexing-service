package publisher

import (
	"context"
	"time"

	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipnifind "github.com/ipni/go-libipni/find/client"
	"github.com/libp2p/go-libp2p/core/peer"
)

var NotifierPollInterval = time.Second * 30

// NotifyRemoteSyncFunc is a function that is called when a remote IPNI node has been seen
// to perform a sync.
type NotifyRemoteSyncFunc func(ctx context.Context, head, prev ipld.Link)

// RemoteSyncNotifier enables notifications of remote sync events.
type RemoteSyncNotifier interface {
	// Notify adds the passed notification function to the list of functions that
	// should be called when a remote IPNI node has been seen to perform a sync.
	Notify(NotifyRemoteSyncFunc)
}

type Notifier struct {
	client   *ipnifind.Client
	provider peer.ID
	head     ipld.Link
	ts       time.Time
	done     chan bool
	notify   []NotifyRemoteSyncFunc
}

func (n *Notifier) Start(ctx context.Context) {
	n.done = make(chan bool)
	ticker := time.NewTicker(NotifierPollInterval)
	go func() {
		for {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return
			case <-n.done:
				ticker.Stop()
				return
			case <-ticker.C:
				head, err := GetLastAdvertisement(ctx, n.client, n.provider)
				if err != nil {
					log.Errorf("fetching last advert CID: %w", err)
					continue
				}
				if !DidSync(head, n.head) {
					log.Warnf("remote IPNI subscriber did not sync for %s", time.Since(n.ts))
					continue
				}
				for _, f := range n.notify {
					f(ctx, head, n.head)
				}
				n.head = head
				n.ts = time.Now()
			}
		}
	}()
}

func (n *Notifier) Notify(f NotifyRemoteSyncFunc) {
	n.notify = append(n.notify, f)
}

func (n *Notifier) Stop() {
	n.done <- true
}

func GetLastAdvertisement(ctx context.Context, client *ipnifind.Client, provider peer.ID) (ipld.Link, error) {
	info, err := client.GetProvider(ctx, provider)
	if err != nil {
		return nil, err
	}
	return cidlink.Link{Cid: info.LastAdvertisement}, nil
}

func DidSync(head, prev ipld.Link) bool {
	return prev == nil || head.String() != prev.String()
}

// NewRemoteSyncNotifier creates an IPNI sync notifier instance that calls
// notification functions when a remote IPNI instance has performed a sync and
// it's latest advertisement has changed. The head parameter is optional.
//
// Note: not guaranteed to notify for every sync event.
func NewRemoteSyncNotifier(addr string, provider peer.ID, head ipld.Link) (*Notifier, error) {
	c, err := ipnifind.New(addr)
	if err != nil {
		return nil, err
	}
	return &Notifier{client: c, head: head, ts: time.Now(), provider: provider}, nil
}
