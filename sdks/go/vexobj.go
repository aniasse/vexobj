// Package vexobj provides a Go client for the vexobj API.
package vexobj

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is the vexobj API client.
type Client struct {
	BaseURL    string
	APIKey     string
	HTTPClient *http.Client
}

// New creates a new vexobj client.
func New(baseURL, apiKey string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// ── Models ──────────────────────────────────────────────

type Bucket struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	CreatedAt string `json:"created_at"`
	Public    bool   `json:"public"`
}

type ObjectMeta struct {
	ID          string                 `json:"id"`
	Bucket      string                 `json:"bucket"`
	Key         string                 `json:"key"`
	Size        int64                  `json:"size"`
	ContentType string                 `json:"content_type"`
	SHA256      string                 `json:"sha256"`
	CreatedAt   string                 `json:"created_at"`
	UpdatedAt   string                 `json:"updated_at"`
	Metadata    map[string]interface{} `json:"metadata"`
}

type ListObjectsResponse struct {
	Objects               []ObjectMeta `json:"objects"`
	CommonPrefixes        []string     `json:"common_prefixes"`
	IsTruncated           bool         `json:"is_truncated"`
	NextContinuationToken *string      `json:"next_continuation_token"`
}

type Permissions struct {
	Read   bool `json:"read"`
	Write  bool `json:"write"`
	Delete bool `json:"delete"`
	Admin  bool `json:"admin"`
}

type APIKey struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	KeyPrefix   string      `json:"key_prefix"`
	CreatedAt   string      `json:"created_at"`
	Permissions Permissions `json:"permissions"`
}

type PresignedURL struct {
	URL       string `json:"url"`
	Method    string `json:"method"`
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	ExpiresAt string `json:"expires_at"`
}

type ImageTransform struct {
	Width   int
	Height  int
	Format  string // jpeg, png, webp, avif, gif
	Quality int
	Fit     string // cover, contain, fill
}

type Stats struct {
	Buckets       int                      `json:"buckets"`
	TotalObjects  int64                    `json:"total_objects"`
	TotalSize     int64                    `json:"total_size"`
	TotalSizeHuman string                  `json:"total_size_human"`
	DiskUsage     int64                    `json:"disk_usage"`
	DiskUsageHuman string                  `json:"disk_usage_human"`
	BucketDetails []map[string]interface{} `json:"bucket_details"`
	Version       string                   `json:"version"`
}

type GCResult struct {
	BlobsScanned   int64 `json:"blobs_scanned"`
	OrphansRemoved int64 `json:"orphans_removed"`
	BytesFreed     int64 `json:"bytes_freed"`
}

type ObjectVersion struct {
	ID             string `json:"id"`
	Bucket         string `json:"bucket"`
	Key            string `json:"key"`
	VersionID      string `json:"version_id"`
	Size           int64  `json:"size"`
	ContentType    string `json:"content_type"`
	SHA256         string `json:"sha256"`
	CreatedAt      string `json:"created_at"`
	IsLatest       bool   `json:"is_latest"`
	IsDeleteMarker bool   `json:"is_delete_marker"`
}

// ObjectLock describes the retention and legal-hold state of a live
// object. RetainUntil is an RFC-3339 timestamp (UTC); nil means no
// retention.
type ObjectLock struct {
	RetainUntil *string `json:"retain_until"`
	LegalHold   bool    `json:"legal_hold"`
}

type LifecycleRule struct {
	ID         string `json:"id"`
	Bucket     string `json:"bucket"`
	Prefix     string `json:"prefix"`
	ExpireDays int64  `json:"expire_days"`
	CreatedAt  string `json:"created_at"`
}

// Error is returned by the vexobj API on failure.
type Error struct {
	StatusCode int
	Message    string
}

func (e *Error) Error() string {
	return fmt.Sprintf("vexobj [%d]: %s", e.StatusCode, e.Message)
}

// ── Internal ────────────────────────────────────────────

func (c *Client) do(req *http.Request) (*http.Response, error) {
	req.Header.Set("Authorization", "Bearer "+c.APIKey)
	return c.HTTPClient.Do(req)
}

func (c *Client) doJSON(req *http.Request, out interface{}) error {
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return parseError(resp)
	}

	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func parseError(resp *http.Response) error {
	var body struct {
		Err string `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	msg := body.Err
	if msg == "" {
		msg = resp.Status
	}
	return &Error{StatusCode: resp.StatusCode, Message: msg}
}

// ── Buckets ─────────────────────────────────────────────

// CreateBucket creates a new storage bucket.
func (c *Client) CreateBucket(name string, public bool) (*Bucket, error) {
	body, _ := json.Marshal(map[string]interface{}{"name": name, "public": public})
	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/buckets", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	var bucket Bucket
	if err := c.doJSON(req, &bucket); err != nil {
		return nil, err
	}
	return &bucket, nil
}

// ListBuckets returns all buckets.
func (c *Client) ListBuckets() ([]Bucket, error) {
	req, _ := http.NewRequest("GET", c.BaseURL+"/v1/buckets", nil)

	var resp struct {
		Buckets []Bucket `json:"buckets"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return nil, err
	}
	return resp.Buckets, nil
}

// GetBucket returns a single bucket by name.
func (c *Client) GetBucket(name string) (*Bucket, error) {
	req, _ := http.NewRequest("GET", c.BaseURL+"/v1/buckets/"+url.PathEscape(name), nil)

	var bucket Bucket
	if err := c.doJSON(req, &bucket); err != nil {
		return nil, err
	}
	return &bucket, nil
}

// DeleteBucket deletes an empty bucket.
func (c *Client) DeleteBucket(name string) error {
	req, _ := http.NewRequest("DELETE", c.BaseURL+"/v1/buckets/"+url.PathEscape(name), nil)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return &Error{StatusCode: resp.StatusCode, Message: "delete bucket failed"}
	}
	return nil
}

// ── Objects ─────────────────────────────────────────────

// PutObject uploads data to a bucket.
func (c *Client) PutObject(bucket, key string, data io.Reader, contentType string) (*ObjectMeta, error) {
	u := fmt.Sprintf("%s/v1/objects/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("PUT", u, data)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	var meta ObjectMeta
	if err := c.doJSON(req, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetObject downloads an object and returns its body. Caller must close the body.
func (c *Client) GetObject(bucket, key string) (io.ReadCloser, *ObjectMeta, error) {
	u := fmt.Sprintf("%s/v1/objects/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("GET", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, nil, parseError(resp)
	}

	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	meta := &ObjectMeta{
		Bucket:      bucket,
		Key:         key,
		ContentType: resp.Header.Get("Content-Type"),
		Size:        size,
		SHA256:      strings.Trim(resp.Header.Get("ETag"), `"`),
	}

	return resp.Body, meta, nil
}

// GetImage downloads an image with on-the-fly transforms.
func (c *Client) GetImage(bucket, key string, t *ImageTransform) (io.ReadCloser, error) {
	params := url.Values{}
	if t != nil {
		if t.Width > 0 {
			params.Set("w", strconv.Itoa(t.Width))
		}
		if t.Height > 0 {
			params.Set("h", strconv.Itoa(t.Height))
		}
		if t.Format != "" {
			params.Set("format", t.Format)
		}
		if t.Quality > 0 {
			params.Set("quality", strconv.Itoa(t.Quality))
		}
		if t.Fit != "" {
			params.Set("fit", t.Fit)
		}
	}

	u := fmt.Sprintf("%s/v1/objects/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	if qs := params.Encode(); qs != "" {
		u += "?" + qs
	}

	req, _ := http.NewRequest("GET", u, nil)
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, parseError(resp)
	}
	return resp.Body, nil
}

// ImageURL builds a URL for an image with transforms (useful for templates).
func (c *Client) ImageURL(bucket, key string, t *ImageTransform) string {
	params := url.Values{}
	if t != nil {
		if t.Width > 0 {
			params.Set("w", strconv.Itoa(t.Width))
		}
		if t.Height > 0 {
			params.Set("h", strconv.Itoa(t.Height))
		}
		if t.Format != "" {
			params.Set("format", t.Format)
		}
		if t.Quality > 0 {
			params.Set("quality", strconv.Itoa(t.Quality))
		}
		if t.Fit != "" {
			params.Set("fit", t.Fit)
		}
	}

	u := fmt.Sprintf("%s/v1/objects/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	if qs := params.Encode(); qs != "" {
		u += "?" + qs
	}
	return u
}

// HeadObject returns object metadata without downloading the body.
func (c *Client) HeadObject(bucket, key string) (*ObjectMeta, error) {
	u := fmt.Sprintf("%s/v1/objects/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("HEAD", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()

	if resp.StatusCode >= 400 {
		return nil, &Error{StatusCode: resp.StatusCode, Message: "not found"}
	}

	size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
	return &ObjectMeta{
		Bucket:      bucket,
		Key:         key,
		ContentType: resp.Header.Get("Content-Type"),
		Size:        size,
		SHA256:      strings.Trim(resp.Header.Get("ETag"), `"`),
	}, nil
}

// DeleteObject removes an object from a bucket.
func (c *Client) DeleteObject(bucket, key string) error {
	u := fmt.Sprintf("%s/v1/objects/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("DELETE", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return &Error{StatusCode: resp.StatusCode, Message: "delete failed"}
	}
	return nil
}

// ListObjectsOptions configures the ListObjects call.
type ListObjectsOptions struct {
	Prefix            string
	Delimiter         string
	MaxKeys           int
	ContinuationToken string
}

// ListObjects lists objects in a bucket.
func (c *Client) ListObjects(bucket string, opts *ListObjectsOptions) (*ListObjectsResponse, error) {
	params := url.Values{}
	if opts != nil {
		if opts.Prefix != "" {
			params.Set("prefix", opts.Prefix)
		}
		if opts.Delimiter != "" {
			params.Set("delimiter", opts.Delimiter)
		}
		if opts.MaxKeys > 0 {
			params.Set("max_keys", strconv.Itoa(opts.MaxKeys))
		}
		if opts.ContinuationToken != "" {
			params.Set("continuation_token", opts.ContinuationToken)
		}
	}

	u := fmt.Sprintf("%s/v1/objects/%s", c.BaseURL, url.PathEscape(bucket))
	if qs := params.Encode(); qs != "" {
		u += "?" + qs
	}

	req, _ := http.NewRequest("GET", u, nil)

	var resp ListObjectsResponse
	if err := c.doJSON(req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ── Streaming ───────────────────────────────────────────

// StreamUpload uploads a large file using streaming (constant RAM).
func (c *Client) StreamUpload(bucket, key string, data io.Reader, contentType string) (*ObjectMeta, error) {
	u := fmt.Sprintf("%s/v1/stream/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("PUT", u, data)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	var meta ObjectMeta
	if err := c.doJSON(req, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// StreamDownload downloads a large file as a stream. Caller must close the body.
func (c *Client) StreamDownload(bucket, key string) (io.ReadCloser, error) {
	u := fmt.Sprintf("%s/v1/stream/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("GET", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, parseError(resp)
	}
	return resp.Body, nil
}

// ── Presigned URLs ──────────────────────────────────────

// Presign generates a presigned URL for temporary access.
func (c *Client) Presign(method, bucket, key string, expiresIn int) (*PresignedURL, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"method":     method,
		"bucket":     bucket,
		"key":        key,
		"expires_in": expiresIn,
	})
	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/presign", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	var presigned PresignedURL
	if err := c.doJSON(req, &presigned); err != nil {
		return nil, err
	}
	return &presigned, nil
}

// ── Admin ───────────────────────────────────────────────

// CreateAPIKey creates a new API key (admin only).
func (c *Client) CreateAPIKey(name string, perms *Permissions) (*APIKey, string, error) {
	payload := map[string]interface{}{"name": name}
	if perms != nil {
		payload["permissions"] = perms
	}
	body, _ := json.Marshal(payload)
	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/admin/keys", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	var resp struct {
		Key    APIKey `json:"key"`
		Secret string `json:"secret"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return nil, "", err
	}
	return &resp.Key, resp.Secret, nil
}

// ListAPIKeys returns all API keys (admin only).
func (c *Client) ListAPIKeys() ([]APIKey, error) {
	req, _ := http.NewRequest("GET", c.BaseURL+"/v1/admin/keys", nil)

	var resp struct {
		Keys []APIKey `json:"keys"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return nil, err
	}
	return resp.Keys, nil
}

// DeleteAPIKey revokes an API key (admin only).
func (c *Client) DeleteAPIKey(id string) error {
	req, _ := http.NewRequest("DELETE", c.BaseURL+"/v1/admin/keys/"+url.PathEscape(id), nil)
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return &Error{StatusCode: resp.StatusCode, Message: "delete key failed"}
	}
	return nil
}

// Stats returns storage statistics (admin only).
func (c *Client) Stats() (*Stats, error) {
	req, _ := http.NewRequest("GET", c.BaseURL+"/v1/stats", nil)

	var stats Stats
	if err := c.doJSON(req, &stats); err != nil {
		return nil, err
	}
	return &stats, nil
}

// GC runs garbage collection on orphan blobs (admin only).
func (c *Client) GC() (*GCResult, error) {
	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/admin/gc", nil)

	var result GCResult
	if err := c.doJSON(req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ── Versioning ──────────────────────────────────────────

// EnableVersioning enables versioning on a bucket. Once enabled, it
// cannot be turned off.
func (c *Client) EnableVersioning(bucket string) error {
	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/admin/versioning/"+url.PathEscape(bucket), nil)
	return c.doJSON(req, nil)
}

// ListVersions returns every version (and delete-marker) for a key,
// newest first.
func (c *Client) ListVersions(bucket, key string) ([]ObjectVersion, error) {
	u := fmt.Sprintf("%s/v1/versions/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("GET", u, nil)

	var resp struct {
		Versions []ObjectVersion `json:"versions"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return nil, err
	}
	return resp.Versions, nil
}

// GetObjectVersion downloads a specific historical version of an object.
func (c *Client) GetObjectVersion(bucket, key, versionID string) (io.ReadCloser, error) {
	u := fmt.Sprintf(
		"%s/v1/objects/%s/%s?version_id=%s",
		c.BaseURL, url.PathEscape(bucket), key, url.QueryEscape(versionID),
	)
	req, _ := http.NewRequest("GET", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, parseError(resp)
	}
	return resp.Body, nil
}

// DeleteObjectVersion removes a specific version of an object. If the
// live version is locked, the server returns 409.
func (c *Client) DeleteObjectVersion(bucket, key, versionID string) error {
	u := fmt.Sprintf(
		"%s/v1/objects/%s/%s?version_id=%s",
		c.BaseURL, url.PathEscape(bucket), key, url.QueryEscape(versionID),
	)
	req, _ := http.NewRequest("DELETE", u, nil)
	return c.doJSON(req, nil)
}

// PurgeVersions hard-deletes every version and the live object for a key.
// Returns the number of blobs removed from disk.
func (c *Client) PurgeVersions(bucket, key string) (int64, error) {
	u := fmt.Sprintf("%s/v1/versions/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("DELETE", u, nil)

	var resp struct {
		BlobsRemoved int64 `json:"blobs_removed"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return 0, err
	}
	return resp.BlobsRemoved, nil
}

// ── Object lock ─────────────────────────────────────────

// GetLock reads the object-lock state for a live object.
func (c *Client) GetLock(bucket, key string) (*ObjectLock, error) {
	u := fmt.Sprintf("%s/v1/admin/lock/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("GET", u, nil)

	var lock ObjectLock
	if err := c.doJSON(req, &lock); err != nil {
		return nil, err
	}
	return &lock, nil
}

// SetLock applies retention and/or a legal hold to a live object.
// RetainUntil can only be extended once set — shortening it while still
// in the future returns 409.
func (c *Client) SetLock(bucket, key string, lock ObjectLock) (*ObjectLock, error) {
	body, _ := json.Marshal(lock)
	u := fmt.Sprintf("%s/v1/admin/lock/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("PUT", u, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	var result ObjectLock
	if err := c.doJSON(req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ReleaseLegalHold clears the legal-hold flag. Retention, if any, remains
// in effect.
func (c *Client) ReleaseLegalHold(bucket, key string) error {
	u := fmt.Sprintf("%s/v1/admin/lock/%s/%s", c.BaseURL, url.PathEscape(bucket), key)
	req, _ := http.NewRequest("DELETE", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return parseError(resp)
	}
	return nil
}

// ── Lifecycle ───────────────────────────────────────────

// CreateLifecycleRule configures automatic expiration for objects under
// `prefix` (leave empty for the whole bucket) after `expireDays`.
func (c *Client) CreateLifecycleRule(bucket, prefix string, expireDays int64) (*LifecycleRule, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"prefix":      prefix,
		"expire_days": expireDays,
	})
	u := fmt.Sprintf("%s/v1/admin/lifecycle/%s", c.BaseURL, url.PathEscape(bucket))
	req, _ := http.NewRequest("POST", u, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	var rule LifecycleRule
	if err := c.doJSON(req, &rule); err != nil {
		return nil, err
	}
	return &rule, nil
}

// ListLifecycleRules returns every rule configured on a bucket.
func (c *Client) ListLifecycleRules(bucket string) ([]LifecycleRule, error) {
	u := fmt.Sprintf("%s/v1/admin/lifecycle/%s", c.BaseURL, url.PathEscape(bucket))
	req, _ := http.NewRequest("GET", u, nil)

	var resp struct {
		Rules []LifecycleRule `json:"rules"`
	}
	if err := c.doJSON(req, &resp); err != nil {
		return nil, err
	}
	return resp.Rules, nil
}

// DeleteLifecycleRule removes a single rule by id.
func (c *Client) DeleteLifecycleRule(id string) error {
	u := fmt.Sprintf("%s/v1/admin/lifecycle/rule/%s", c.BaseURL, url.PathEscape(id))
	req, _ := http.NewRequest("DELETE", u, nil)

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 400 {
		return parseError(resp)
	}
	return nil
}

// RunLifecycle triggers an immediate sweep — eligible objects are expired
// right away rather than at the next scheduled run.
func (c *Client) RunLifecycle() (expired, bytesFreed int64, err error) {
	req, _ := http.NewRequest("POST", c.BaseURL+"/v1/admin/lifecycle/run", nil)

	var resp struct {
		ObjectsExpired int64 `json:"objects_expired"`
		BytesFreed     int64 `json:"bytes_freed"`
	}
	if err = c.doJSON(req, &resp); err != nil {
		return 0, 0, err
	}
	return resp.ObjectsExpired, resp.BytesFreed, nil
}
