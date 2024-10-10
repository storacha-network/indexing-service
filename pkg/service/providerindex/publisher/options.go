package publisher

import (
	"github.com/ipfs/go-datastore"
)

// Option is an option configuring a publisher.
type Option func(cfg *options) error

type options struct {
	datastore            datastore.Batching
	listenAddr           string
	announceAddrs        []string
	remoteSyncNotifyAddr string
}

// WithDatastore configures the data store for adverts and entries.
func WithDatastore(ds datastore.Batching) Option {
	return func(opts *options) error {
		opts.datastore = ds
		return nil
	}
}

// WithListenAddr configures the HTTP address the publisher binds to.
// This allows a remote IPNI subscriber to fetch advertisements.
func WithListenAddr(addr string) Option {
	return func(opts *options) error {
		opts.listenAddr = addr
		return nil
	}
}

// WithAnnounceAddr configures the multiaddr(s) of IPNI nodes that new
// advertisements should be announced to.
func WithAnnounceAddr(addrs ...string) Option {
	return func(opts *options) error {
		opts.announceAddrs = addrs
		return nil
	}
}

// WithRemoteSyncNotifyAddr configures the URL of a remote IPNI node that
// should be used to provide remote sync notifications.
func WithRemoteSyncNotifyAddr(addr string) Option {
	return func(opts *options) error {
		opts.remoteSyncNotifyAddr = addr
		return nil
	}
}
