package blobindexlookup

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ipni/go-libipni/find/model"
	"github.com/storacha/indexing-service/pkg/blobindex"
	"github.com/storacha/indexing-service/pkg/metadata"
	"github.com/storacha/indexing-service/pkg/types"
)

type simpleLookup struct {
	httpClient *http.Client
}

func NewBlobIndexLookup(httpClient *http.Client) BlobIndexLookup {
	return &simpleLookup{httpClient}
}

// Find fetches the blob index from the given fetchURL
func (s *simpleLookup) Find(ctx context.Context, _ types.EncodedContextID, _ model.ProviderResult, fetchURL url.URL, rng *metadata.Range) (blobindex.ShardedDagIndexView, error) {
	// attempt to fetch the index from provided url
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fetchURL.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("constructing request: %w", err)
	}
	if rng != nil {
		rangeHeader := fmt.Sprintf("bytes=%d-", rng.Offset)
		if rng.Length != nil {
			rangeHeader += strconv.FormatUint(rng.Offset+*rng.Length-1, 10)
		}
		req.Header.Set("Range", rangeHeader)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch index: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, _ := io.ReadAll(resp.Body)

		return nil, fmt.Errorf("failure response fetching index. status: %s, message: %s", resp.Status, string(body))
	}
	return blobindex.Extract(resp.Body)
}
