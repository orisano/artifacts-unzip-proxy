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

	"github.com/google/go-github/v32/github"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/oauth2"
)

const (
	maxSize = 5 * 1024 * 1024
)

type UnzipHandler struct {
	httpClient *http.Client
}

func main() {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	handler := UnzipHandler{}
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

	ctx := req.Context()
	client := github.NewClient(h.httpClient)
	signedURL, redirectRes, err := client.Actions.DownloadArtifact(ctx, owner, repo, artifactID, true)
	if err != nil {
		if re, ok := err.(*github.RateLimitError); ok {
			logger.Warn().Msgf("reached rate limit: %s", re.Message)
			w.Header().Set("Retry-After", re.Rate.Reset.Format(http.TimeFormat))
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
	logger = logger.With().Int("artifact_size", len(zippedArtifact)).Logger()

	br := bytes.NewReader(zippedArtifact)
	zr, err := zip.NewReader(br, br.Size())
	if err != nil {
		logger.Info().Str("content_type", artifactRes.Header.Get("Content-Type")).Err(err).Msg("failed to read zip")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
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
		return
	}
	if item.UncompressedSize64 > maxSize {
		logger.Info().Uint64("uncompressed_size", item.UncompressedSize64).Msg("too large content")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	rc, err := item.Open()
	if err != nil {
		logger.Info().Err(err).Msg("failed to open the item")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer rc.Close()
	mimeType := mime.TypeByExtension(path.Ext(item.Name))

	header := w.Header()
	header.Add("Content-Type", mimeType)
	header.Add("Cache-Control", "max-age=7776000, public")
	w.WriteHeader(http.StatusOK)
	if _, err := io.Copy(w, rc); err != nil {
		logger.Info().Err(err).Msg("failed to write response")
	}
}
