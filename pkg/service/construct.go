package service

import (
	"context"
	"fmt"
	"net/http"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	flatfs "github.com/ipfs/go-ds-flatfs"
	logging "github.com/ipfs/go-log/v2"
	ipnifind "github.com/ipni/go-libipni/find/client"
	"github.com/libp2p/go-libp2p/core/crypto"
	goredis "github.com/redis/go-redis/v9"
	"github.com/storacha/indexing-service/pkg/internal/jobqueue"
	"github.com/storacha/indexing-service/pkg/redis"
	"github.com/storacha/indexing-service/pkg/service/blobindexlookup"
	"github.com/storacha/indexing-service/pkg/service/claimlookup"
	"github.com/storacha/indexing-service/pkg/service/providercacher"
	"github.com/storacha/indexing-service/pkg/service/providerindex"
	"github.com/storacha/indexing-service/pkg/service/providerindex/publisher"
)

var log = logging.Logger("service")
var providerIndexNamespace = datastore.NewKey("providerindex/")
var providerIndexPublisherNamespace = providerIndexNamespace.ChildString("publisher/")

type ServiceConfig struct {
	// DataPath configures the filesystem path where local data is stored.
	DataPath string
	// PrivateKey configures the private key for the service.
	PrivateKey  crypto.PrivKey
	RedisURL    string
	RedisPasswd string
	ProvidersDB int
	ClaimsDB    int
	IndexesDB   int
	IndexerURL  string
	// PublisherListenAddr configures the HTTP address the publisher binds to.
	// This allows a remote IPNI subscriber to fetch advertisements.
	PublisherListenAddr string
	// PublisherAnnounceAddrs configures the multiaddrs of IPNI nodes that new
	// advertisements should be announced to.
	PublisherAnnounceAddrs []string
}

func Construct(ctx context.Context, sc ServiceConfig) (*IndexingService, func(context.Context), error) {

	// connect to redis
	providersClient := goredis.NewClient(&goredis.Options{
		Addr:     sc.RedisURL,
		Password: sc.RedisPasswd,
		DB:       sc.ProvidersDB,
	})
	claimsClient := goredis.NewClient(&goredis.Options{
		Addr:     sc.RedisURL,
		Password: sc.RedisPasswd,
		DB:       sc.ClaimsDB,
	})
	indexesClient := goredis.NewClient(&goredis.Options{
		Addr:     sc.RedisURL,
		Password: sc.RedisPasswd,
		DB:       sc.IndexesDB,
	})

	// build caches
	providersCache := redis.NewProviderStore(providersClient)
	claimsCache := redis.NewContentClaimsStore(claimsClient)
	shardDagIndexesCache := redis.NewShardedDagIndexStore(indexesClient)

	// setup and start the provider caching queue for indexes
	cachingJobHandler := providercacher.NewJobHandler(providercacher.NewSimpleProviderCacher(providersCache))
	jobQueue := jobqueue.NewJobQueue(cachingJobHandler.Handle,
		jobqueue.WithBuffer(5),
		jobqueue.WithConcurrency(5),
		jobqueue.WithErrorHandler(func(err error) {
			log.Errorw("caching provider index", "error", err)
		}))
	cachingQueue := providercacher.NewCachingQueue(jobQueue)

	// setup IPNI
	// TODO: switch to double hashed client for reader privacy?
	findClient, err := ipnifind.New(sc.IndexerURL)
	if err != nil {
		return nil, nil, err
	}

	var ds datastore.Batching
	if sc.DataPath != "" {
		fds, err := flatfs.CreateOrOpen(sc.DataPath, flatfs.IPFS_DEF_SHARD, true)
		if err != nil {
			return nil, nil, fmt.Errorf("creating or opening IPNI publisher datastore: %w", err)
		}
		ds = fds
	} else {
		log.Warnf("datastore not configured, using in-memory store")
		ds = datastore.NewMapDatastore()
	}

	publisher, err := publisher.New(
		sc.PrivateKey,
		publisher.WithDatastore(namespace.Wrap(ds, providerIndexPublisherNamespace)),
		publisher.WithListenAddr(sc.PublisherListenAddr),
		publisher.WithAnnounceAddr(sc.PublisherAnnounceAddrs...),
		publisher.WithRemoteSyncNotifyAddr(sc.IndexerURL),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("creating IPNI publisher: %w", err)
	}

	// build read through fetchers
	// TODO: add sender / publisher / linksystem / legacy systems
	providerIndex := providerindex.NewProviderIndex(providersCache, findClient, publisher, nil)
	claimLookup := claimlookup.WithCache(claimlookup.NewClaimLookup(http.DefaultClient), claimsCache)
	blobIndexLookup := blobindexlookup.WithCache(
		blobindexlookup.NewBlobIndexLookup(http.DefaultClient),
		shardDagIndexesCache,
		cachingQueue,
	)

	// setup walker
	service := NewIndexingService(blobIndexLookup, claimLookup, providerIndex, WithConcurrency(5))

	err = publisher.Start(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("starting IPNI publisher: %w", err)
	}

	// start the job queue
	jobQueue.Startup()

	return service, func(ctx context.Context) {
		jobQueue.Shutdown(ctx)
		publisher.Shutdown()
	}, nil
}
