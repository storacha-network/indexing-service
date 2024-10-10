package publisher

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"sync"

	cid "github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipni/go-libipni/metadata"
	provider "github.com/ipni/index-provider"
	"github.com/ipni/index-provider/engine"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

var log = logging.Logger("publisher")
var remoteHeadPrefix = datastore.NewKey("head/remote/")

// The below definitions allow us to actually read entry blocks from the
// IPNI datastore. For some reason there's not accessor for this.
// https://github.com/ipni/index-provider/blob/69eb98045424f6074fc351b9d4771d0725a28620/engine/engine.go#L39
var entriesNamespace = datastore.NewKey("/cache/links")

// maxEntryChunkSize is the maximum number of multihashes each advertisement
// entry chunk may contain.
var maxEntryChunkSize = 16384

type Publisher interface {
	// Publish publishes an advert to indexer(s). Note: it is not necessary to
	// sign the advert - this is done automatically.
	Publish(ctx context.Context, provider *peer.AddrInfo, contextID string, digests []mh.Multihash, meta metadata.Metadata) (ipld.Link, error)
	// NotifyRemoteSync calls the notification function when the configured remote
	// IPNI node is observed to have performed a sync.
	NotifyRemoteSync(NotifyRemoteSyncFunc)
	// Store returns the storage interface used to access published data.
	Store() AdvertStore
}

type IPNIPublisher struct {
	engine   *engine.Engine
	notifier *Notifier
	store    AdvertStore
	mutex    sync.RWMutex
}

func (p *IPNIPublisher) NotifyRemoteSync(f NotifyRemoteSyncFunc) {
	if p.notifier != nil {
		p.notifier.Notify(f)
	} else {
		log.Errorf("notification for sync events requested but no remote IPNI node configured")
	}
}

func (p *IPNIPublisher) Start(ctx context.Context) error {
	err := p.engine.Start(ctx)
	if err != nil {
		return fmt.Errorf("starting IPNI index provider: %w", err)
	}
	if p.notifier != nil {
		p.notifier.Start(ctx)
	}
	return nil
}

func (p *IPNIPublisher) Publish(ctx context.Context, providerInfo *peer.AddrInfo, contextID string, digests []mh.Multihash, meta metadata.Metadata) (ipld.Link, error) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	// such a weird API
	p.engine.RegisterMultihashLister(func(ctx context.Context, _ peer.ID, _ []byte) (provider.MultihashIterator, error) {
		return provider.SliceMultihashIterator(digests), nil
	})
	cid, err := p.engine.NotifyPut(ctx, providerInfo, []byte(contextID), meta)
	if err != nil {
		return nil, fmt.Errorf("publishing IPNI advert: %w", err)
	}
	return cidlink.Link{Cid: cid}, nil
}

func (p *IPNIPublisher) Store() AdvertStore {
	return p.store
}

func (p *IPNIPublisher) Shutdown() error {
	if p.notifier != nil {
		p.notifier.Stop()
	}
	err := p.engine.Shutdown()
	return err
}

func New(id crypto.PrivKey, opts ...Option) (*IPNIPublisher, error) {
	o := &options{}
	for _, opt := range opts {
		err := opt(o)
		if err != nil {
			return nil, err
		}
	}

	ds := o.datastore
	if ds == nil {
		log.Warnf("datastore not configured, using in-memory store")
		ds = datastore.NewMapDatastore()
	}

	listenAddr := o.listenAddr
	if listenAddr == "" {
		log.Warnf(`no HTTP listen address configured, using default "0.0.0.0:3104"`)
		listenAddr = "0.0.0.0:3104"
	}

	// not sure why we need a libp2p host, but we get this error otherwise:
	// > Libp2p host is not configured, but required; created a new host.
	h, err := libp2p.New(libp2p.Identity(id))
	if err != nil {
		return nil, fmt.Errorf("creating libp2p host: %w", err)
	}

	engopts := []engine.Option{
		engine.WithPrivateKey(id),
		engine.WithHost(h),
		engine.WithPublisherKind(engine.HttpPublisher),
		engine.WithHttpPublisherListenAddr(listenAddr),
		engine.WithDatastore(ds),
		engine.WithChainedEntries(maxEntryChunkSize),
	}
	for _, addr := range o.announceAddrs {
		engopts = append(engopts, engine.WithHttpPublisherAnnounceAddr(addr))
	}
	if len(o.announceAddrs) == 0 {
		log.Warnf("no HTTP address(es) configured for announcements")
	}

	engine, err := engine.New(engopts...)
	if err != nil {
		return nil, fmt.Errorf("creating IPNI index provider: %w", err)
	}

	ads := NewAdvertStore(ds, namespace.Wrap(ds, entriesNamespace))
	pubr := IPNIPublisher{engine: engine, store: ads}
	if o.remoteSyncNotifyAddr == "" {
		log.Warnf("no HTTP address configured for remote sync notifications")
		return &pubr, nil
	}

	peer, err := peer.IDFromPrivateKey(id)
	if err != nil {
		return nil, fmt.Errorf("creating peer ID for IPNI publisher: %w", err)
	}

	remoteSyncNotifURL, err := url.Parse(o.remoteSyncNotifyAddr)
	if err != nil {
		return nil, fmt.Errorf("parsing URL for remote sync notifications: %w", err)
	}

	var hd ipld.Link
	hdkey := remoteHeadPrefix.ChildString(remoteSyncNotifURL.Hostname())
	v, err := ds.Get(context.Background(), hdkey)
	if err != nil {
		if !errors.Is(err, datastore.ErrNotFound) {
			return nil, fmt.Errorf("getting remote IPNI head CID from datastore: %w", err)
		}
	} else {
		c, err := cid.Cast(v)
		if err != nil {
			return nil, fmt.Errorf("parsing remote IPNI head CID: %w", err)
		}
		hd = cidlink.Link{Cid: c}
	}

	notifier, err := NewRemoteSyncNotifier(o.remoteSyncNotifyAddr, peer, hd)
	if err != nil {
		return nil, fmt.Errorf("creating IPNI remote sync notifier: %w", err)
	}

	// save the head to the datastore when it is updated
	notifier.Notify(func(ctx context.Context, head, prev ipld.Link) {
		err := ds.Put(ctx, hdkey, []byte(head.Binary()))
		if err != nil {
			log.Errorf("saving remote IPNI sync'd head: %w", err)
		}
	})

	pubr.notifier = notifier
	return &pubr, nil
}

func asCID(link ipld.Link) cid.Cid {
	if cl, ok := link.(cidlink.Link); ok {
		return cl.Cid
	}
	return cid.MustParse(link.String())
}
