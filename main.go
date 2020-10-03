// Copyright 2020 Nao Yonashiro
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"archive/zip"
	"bytes"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/dgraph-io/ristretto"
	"github.com/dustin/go-humanize"
	"github.com/google/go-github/v32/github"
	"github.com/google/gops/agent"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

const (
	maxSize = 5 * 1024 * 1024
)

type UnzipHandler struct {
	httpClient *http.Client
	cache      *ristretto.Cache

	// List of GitHub owners who permitted to use this proxy.
	// You can set multiple owners via environment variables like:
	// PROXY_ALLOWED_OWNERS="orisano:c-bata"
	allowedOwners map[string]struct{}
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	handler := UnzipHandler{}
	owners := os.Getenv("PROXY_ALLOWED_OWNERS")
	if owners != "" {
		x := strings.Split(owners, ":")
		handler.allowedOwners = make(map[string]struct{}, len(owners))
		for _, r := range x {
			handler.allowedOwners[r] = struct{}{}
		}
	}

	if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		handler.httpClient = oauth2.NewClient(
			nil,
			oauth2.StaticTokenSource(&oauth2.Token{
				AccessToken: token,
			}),
		)
	} else {
		handler.httpClient = http.DefaultClient
	}

	if maxCacheSizeStr := os.Getenv("MAX_CACHE_SIZE"); maxCacheSizeStr != "" {
		maxCacheSize, err := humanize.ParseBytes(maxCacheSizeStr)
		if err != nil {
			log.Fatal().Err(err).Msgf("invalid MAX_CACHE_SIZE: %q", maxCacheSizeStr)
		}
		if maxCacheSize > 0 {
			handler.cache, _ = ristretto.NewCache(&ristretto.Config{
				NumCounters: maxInt64(int64(maxCacheSize/(32*1024)*10), 10000),
				MaxCost:     int64(maxCacheSize),
				BufferItems: 64,
			})
		}
	}

	if err := agent.Listen(agent.Options{}); err != nil {
		log.Fatal().Err(err).Msg("failed to listen gops agent")
	}

	log.Info().Msgf("listen on :%s", port)
	s := http.Server{
		Addr:              ":" + port,
		Handler:           &handler,
		ReadHeaderTimeout: 1 * time.Second,
	}
	if err := s.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal().Err(err).Msg("failed to listen")
	}
}

func (h *UnzipHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// GET /:owner/:repo/actions/artifacts/:artifact_id?path=foobar
	if req.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if req.URL.Path == "/favicon.ico" {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	logger := log.With().
		Str("path", req.URL.Path).
		Str("query", req.URL.RawQuery).
		Logger()

	if strings.Count(req.URL.Path, "/") != 5 {
		logger.Info().Msg("invalid path depth")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	tokens := strings.SplitN(req.URL.Path, "/", 6)
	owner := tokens[1]
	repo := tokens[2]

	if _, ok := h.allowedOwners[owner]; !ok {
		logger.Info().Msg("unauthorized owners")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	artifactID, err := strconv.ParseInt(tokens[5], 10, 64)
	if err != nil {
		logger.Info().Msgf("artifact_id is not a number: %s", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if tokens[3] != "actions" || tokens[4] != "artifacts" {
		logger.Info().Msg("invalid path format")
		w.WriteHeader(http.StatusNotFound)
		return
	}

	itemPath := req.URL.Query().Get("path")
	if itemPath == "" {
		logger.Info().Msg("path is empty")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	cacheKey := req.URL.Path
	if h.cache != nil {
		value, ok := h.cache.Get(cacheKey)
		if ok { // fast path
			zippedArtifact := value.([]byte)
			writeItem(logger, w, zippedArtifact, itemPath)
			return
		}
	}

	ctx := req.Context()
	client := github.NewClient(h.httpClient)
	signedURL, redirectRes, err := client.Actions.DownloadArtifact(ctx, owner, repo, artifactID, true)
	if err != nil {
		if re, ok := err.(*github.RateLimitError); ok {
			logger.Warn().Msgf("reached rate limit: %s", re.Message)
			w.Header().Set("retry-after", re.Rate.Reset.Format(http.TimeFormat))
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		event := logger.Info().Err(err)
		if redirectRes != nil {
			event.Int("status", redirectRes.StatusCode)
		}
		event.Msg("failed to request download the artifact")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	redirectRes.Body.Close()

	artifactReq, err := http.NewRequestWithContext(ctx, http.MethodGet, signedURL.String(), nil)
	if err != nil {
		logger.Info().Str("signed_url", signedURL.String()).Msg("got invalid signed url")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	artifactRes, err := http.DefaultClient.Do(artifactReq)
	if err != nil {
		logger.Info().Err(err).Msg("failed to get the artifact")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer artifactRes.Body.Close()
	if artifactRes.ContentLength > maxSize {
		logger.Info().Int64("content_length", artifactRes.ContentLength).Msg("too large content")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	zippedArtifact, err := ioutil.ReadAll(io.LimitReader(artifactRes.Body, maxSize+1))
	if err != nil {
		logger.Info().Err(err).Msg("failed to read the artifact from github")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	artifactRes.Body.Close()

	if len(zippedArtifact) > maxSize {
		logger.Info().Int64("content_length", artifactRes.ContentLength).Msg("too large content")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if writeItem(logger, w, zippedArtifact, itemPath) {
		if h.cache != nil {
			h.cache.Set(cacheKey, zippedArtifact, int64(len(zippedArtifact)))
		}
	}
}

func writeItem(logger zerolog.Logger, w http.ResponseWriter, zippedArtifact []byte, itemPath string) bool {
	contentType := mime.TypeByExtension(path.Ext(itemPath))
	cacheControl := "max-age=7776000, public"

	logger = logger.With().Int("artifact_size", len(zippedArtifact)).Logger()

	br := bytes.NewReader(zippedArtifact)
	zr, err := zip.NewReader(br, br.Size())
	if err != nil {
		logger.Info().Err(err).Msg("failed to read zip")
		w.WriteHeader(http.StatusServiceUnavailable)
		return false
	}
	var item *zip.File
	for _, f := range zr.File {
		if f.Name == itemPath {
			item = f
			break
		}
	}
	if item == nil {
		logger.Info().Msg("not found the item in the archive")
		w.WriteHeader(http.StatusNotFound)
		return true
	}
	if item.UncompressedSize64 > maxSize {
		logger.Info().Uint64("uncompressed_size", item.UncompressedSize64).Msg("too large content")
		w.WriteHeader(http.StatusBadRequest)
		return true
	}
	rc, err := item.Open()
	if err != nil {
		logger.Info().Err(err).Msg("failed to open the item")
		w.WriteHeader(http.StatusServiceUnavailable)
		return true
	}
	defer rc.Close()

	header := w.Header()
	header.Set("content-type", contentType)
	header.Set("cache-control", cacheControl)
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, rc); err != nil {
		logger.Info().Err(err).Msg("failed to write response")
	}
	return true
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}
