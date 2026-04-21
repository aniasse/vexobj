export interface VexObjConfig {
  baseUrl: string;
  apiKey: string;
}

export interface Bucket {
  id: string;
  name: string;
  created_at: string;
  public: boolean;
}

export interface ObjectMeta {
  id: string;
  bucket: string;
  key: string;
  size: number;
  content_type: string;
  sha256: string;
  created_at: string;
  updated_at: string;
  metadata: Record<string, unknown>;
}

export interface ListObjectsResponse {
  objects: ObjectMeta[];
  common_prefixes: string[];
  is_truncated: boolean;
  next_continuation_token: string | null;
}

export interface Permissions {
  read: boolean;
  write: boolean;
  delete: boolean;
  admin: boolean;
}

export interface ApiKey {
  id: string;
  name: string;
  key_prefix: string;
  created_at: string;
  permissions: Permissions;
}

export interface PresignedUrl {
  url: string;
  method: string;
  bucket: string;
  key: string;
  expires_at: string;
}

export interface ImageTransform {
  w?: number;
  h?: number;
  format?: "jpeg" | "png" | "webp" | "avif" | "gif";
  quality?: number;
  fit?: "cover" | "contain" | "fill";
}

export interface ObjectVersion {
  id: string;
  bucket: string;
  key: string;
  version_id: string;
  size: number;
  content_type: string;
  sha256: string;
  created_at: string;
  is_latest: boolean;
  is_delete_marker: boolean;
}

export interface ObjectLock {
  /** ISO-8601 timestamp (UTC). Null means no retention. */
  retain_until: string | null;
  legal_hold: boolean;
}

export interface LifecycleRule {
  id: string;
  bucket: string;
  prefix: string;
  expire_days: number;
  created_at: string;
}

export class VexObj {
  private baseUrl: string;
  private apiKey: string;

  constructor(config: VexObjConfig) {
    this.baseUrl = config.baseUrl.replace(/\/$/, "");
    this.apiKey = config.apiKey;
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    return {
      Authorization: `Bearer ${this.apiKey}`,
      ...extra,
    };
  }

  private async request<T>(
    path: string,
    init?: RequestInit
  ): Promise<T> {
    const resp = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers: { ...this.headers(), ...((init?.headers as Record<string, string>) || {}) },
    });
    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new VexObjError(resp.status, (body as any).error || resp.statusText);
    }
    const ct = resp.headers.get("content-type") || "";
    if (ct.includes("application/json")) {
      return resp.json() as Promise<T>;
    }
    return resp as unknown as T;
  }

  // ─── Buckets ──────────────────────────────────────────

  async createBucket(name: string, isPublic = false): Promise<Bucket> {
    return this.request("/v1/buckets", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, public: isPublic }),
    });
  }

  async listBuckets(): Promise<Bucket[]> {
    const resp = await this.request<{ buckets: Bucket[] }>("/v1/buckets");
    return resp.buckets;
  }

  async getBucket(name: string): Promise<Bucket> {
    return this.request(`/v1/buckets/${encodeURIComponent(name)}`);
  }

  async deleteBucket(name: string): Promise<void> {
    await fetch(`${this.baseUrl}/v1/buckets/${encodeURIComponent(name)}`, {
      method: "DELETE",
      headers: this.headers(),
    });
  }

  // ─── Objects ──────────────────────────────────────────

  async putObject(
    bucket: string,
    key: string,
    data: BodyInit,
    contentType?: string
  ): Promise<ObjectMeta> {
    const headers: Record<string, string> = {};
    if (contentType) headers["Content-Type"] = contentType;

    return this.request(`/v1/objects/${enc(bucket)}/${key}`, {
      method: "PUT",
      headers,
      body: data,
    });
  }

  async getObject(
    bucket: string,
    key: string,
    options?: { versionId?: string }
  ): Promise<Response> {
    const qs = options?.versionId
      ? `?version_id=${encodeURIComponent(options.versionId)}`
      : "";
    const resp = await fetch(
      `${this.baseUrl}/v1/objects/${enc(bucket)}/${key}${qs}`,
      { headers: this.headers() }
    );
    if (!resp.ok) throw new VexObjError(resp.status, "object not found");
    return resp;
  }

  async getObjectBytes(bucket: string, key: string): Promise<ArrayBuffer> {
    const resp = await this.getObject(bucket, key);
    return resp.arrayBuffer();
  }

  async getObjectText(bucket: string, key: string): Promise<string> {
    const resp = await this.getObject(bucket, key);
    return resp.text();
  }

  /**
   * Get image with on-the-fly transforms.
   * Returns the transformed image as a Response.
   */
  async getImage(
    bucket: string,
    key: string,
    transform: ImageTransform
  ): Promise<Response> {
    const params = new URLSearchParams();
    if (transform.w) params.set("w", String(transform.w));
    if (transform.h) params.set("h", String(transform.h));
    if (transform.format) params.set("format", transform.format);
    if (transform.quality) params.set("quality", String(transform.quality));
    if (transform.fit) params.set("fit", transform.fit);

    const qs = params.toString();
    const url = `${this.baseUrl}/v1/objects/${enc(bucket)}/${key}${qs ? "?" + qs : ""}`;

    const resp = await fetch(url, { headers: this.headers() });
    if (!resp.ok) throw new VexObjError(resp.status, "image transform failed");
    return resp;
  }

  /** Build a URL for an image with transforms (useful for <img src=...>) */
  imageUrl(bucket: string, key: string, transform?: ImageTransform): string {
    const params = new URLSearchParams();
    if (transform?.w) params.set("w", String(transform.w));
    if (transform?.h) params.set("h", String(transform.h));
    if (transform?.format) params.set("format", transform.format);
    if (transform?.quality) params.set("quality", String(transform.quality));
    if (transform?.fit) params.set("fit", transform.fit);

    const qs = params.toString();
    return `${this.baseUrl}/v1/objects/${enc(bucket)}/${key}${qs ? "?" + qs : ""}`;
  }

  async headObject(bucket: string, key: string): Promise<ObjectMeta> {
    return this.request(`/v1/objects/${enc(bucket)}/${key}`, { method: "HEAD" }) as any;
  }

  async deleteObject(
    bucket: string,
    key: string,
    options?: { versionId?: string }
  ): Promise<void> {
    const qs = options?.versionId
      ? `?version_id=${encodeURIComponent(options.versionId)}`
      : "";
    const resp = await fetch(
      `${this.baseUrl}/v1/objects/${enc(bucket)}/${key}${qs}`,
      { method: "DELETE", headers: this.headers() }
    );
    if (!resp.ok && resp.status !== 404) {
      const body = await resp.json().catch(() => ({}));
      throw new VexObjError(resp.status, (body as any).error || resp.statusText);
    }
  }

  async listObjects(
    bucket: string,
    options?: {
      prefix?: string;
      delimiter?: string;
      maxKeys?: number;
      continuationToken?: string;
    }
  ): Promise<ListObjectsResponse> {
    const params = new URLSearchParams();
    if (options?.prefix) params.set("prefix", options.prefix);
    if (options?.delimiter) params.set("delimiter", options.delimiter);
    if (options?.maxKeys) params.set("max_keys", String(options.maxKeys));
    if (options?.continuationToken) params.set("continuation_token", options.continuationToken);

    const qs = params.toString();
    return this.request(`/v1/objects/${enc(bucket)}${qs ? "?" + qs : ""}`);
  }

  // ─── Streaming (large files) ──────────────────────────

  async streamUpload(
    bucket: string,
    key: string,
    data: BodyInit,
    contentType?: string
  ): Promise<ObjectMeta> {
    const headers: Record<string, string> = {};
    if (contentType) headers["Content-Type"] = contentType;

    return this.request(`/v1/stream/${enc(bucket)}/${key}`, {
      method: "PUT",
      headers,
      body: data,
    });
  }

  async streamDownload(bucket: string, key: string): Promise<ReadableStream<Uint8Array>> {
    const resp = await fetch(`${this.baseUrl}/v1/stream/${enc(bucket)}/${key}`, {
      headers: this.headers(),
    });
    if (!resp.ok) throw new VexObjError(resp.status, "object not found");
    return resp.body!;
  }

  // ─── Presigned URLs ───────────────────────────────────

  async presign(
    method: "GET" | "PUT" | "HEAD" | "DELETE",
    bucket: string,
    key: string,
    expiresIn = 3600
  ): Promise<PresignedUrl> {
    return this.request("/v1/presign", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ method, bucket, key, expires_in: expiresIn }),
    });
  }

  // ─── Admin ────────────────────────────────────────────

  async createApiKey(
    name: string,
    permissions?: Partial<Permissions>
  ): Promise<{ key: ApiKey; secret: string }> {
    return this.request("/v1/admin/keys", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ name, permissions }),
    });
  }

  async listApiKeys(): Promise<ApiKey[]> {
    const resp = await this.request<{ keys: ApiKey[] }>("/v1/admin/keys");
    return resp.keys;
  }

  async deleteApiKey(id: string): Promise<void> {
    await fetch(`${this.baseUrl}/v1/admin/keys/${id}`, {
      method: "DELETE",
      headers: this.headers(),
    });
  }

  async stats(): Promise<Record<string, unknown>> {
    return this.request("/v1/stats");
  }

  async gc(): Promise<{ blobs_scanned: number; orphans_removed: number; bytes_freed: number }> {
    return this.request("/v1/admin/gc", { method: "POST" });
  }

  // ─── Versioning ───────────────────────────────────────

  /** Enable versioning on a bucket. Once enabled, it cannot be disabled. */
  async enableVersioning(bucket: string): Promise<void> {
    await this.request(`/v1/admin/versioning/${enc(bucket)}`, { method: "POST" });
  }

  async listVersions(bucket: string, key: string): Promise<ObjectVersion[]> {
    const resp = await this.request<{ versions: ObjectVersion[] }>(
      `/v1/versions/${enc(bucket)}/${key}`
    );
    return resp.versions;
  }

  /** Hard-delete every version and the live object for a key. */
  async purgeVersions(
    bucket: string,
    key: string
  ): Promise<{ bucket: string; key: string; blobs_removed: number }> {
    return this.request(`/v1/versions/${enc(bucket)}/${key}`, { method: "DELETE" });
  }

  // ─── Object lock ──────────────────────────────────────

  async getLock(bucket: string, key: string): Promise<ObjectLock> {
    return this.request(`/v1/admin/lock/${enc(bucket)}/${key}`);
  }

  /**
   * Set retention and/or legal hold. `retain_until` can only be extended
   * once set — shortening while still active returns 409.
   */
  async setLock(
    bucket: string,
    key: string,
    lock: { retain_until?: string | null; legal_hold?: boolean }
  ): Promise<ObjectLock> {
    return this.request(`/v1/admin/lock/${enc(bucket)}/${key}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(lock),
    });
  }

  /** Clear the legal hold flag. Retention (if any) stays in effect. */
  async releaseLegalHold(bucket: string, key: string): Promise<void> {
    await fetch(`${this.baseUrl}/v1/admin/lock/${enc(bucket)}/${key}`, {
      method: "DELETE",
      headers: this.headers(),
    });
  }

  // ─── Lifecycle ────────────────────────────────────────

  async createLifecycleRule(
    bucket: string,
    expireDays: number,
    prefix = ""
  ): Promise<LifecycleRule> {
    return this.request(`/v1/admin/lifecycle/${enc(bucket)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ prefix, expire_days: expireDays }),
    });
  }

  async listLifecycleRules(bucket: string): Promise<LifecycleRule[]> {
    const resp = await this.request<{ rules: LifecycleRule[] }>(
      `/v1/admin/lifecycle/${enc(bucket)}`
    );
    return resp.rules;
  }

  async deleteLifecycleRule(id: string): Promise<void> {
    await fetch(`${this.baseUrl}/v1/admin/lifecycle/rule/${enc(id)}`, {
      method: "DELETE",
      headers: this.headers(),
    });
  }

  async runLifecycle(): Promise<{ objects_expired: number; bytes_freed: number }> {
    return this.request("/v1/admin/lifecycle/run", { method: "POST" });
  }
}

export class VexObjError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = "VexObjError";
  }
}

function enc(s: string): string {
  return encodeURIComponent(s);
}
