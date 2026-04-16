export interface VaultFSConfig {
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

export class VaultFS {
  private baseUrl: string;
  private apiKey: string;

  constructor(config: VaultFSConfig) {
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
      throw new VaultFSError(resp.status, (body as any).error || resp.statusText);
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

  async getObject(bucket: string, key: string): Promise<Response> {
    const resp = await fetch(`${this.baseUrl}/v1/objects/${enc(bucket)}/${key}`, {
      headers: this.headers(),
    });
    if (!resp.ok) throw new VaultFSError(resp.status, "object not found");
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
    if (!resp.ok) throw new VaultFSError(resp.status, "image transform failed");
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

  async deleteObject(bucket: string, key: string): Promise<void> {
    await fetch(`${this.baseUrl}/v1/objects/${enc(bucket)}/${key}`, {
      method: "DELETE",
      headers: this.headers(),
    });
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
    if (!resp.ok) throw new VaultFSError(resp.status, "object not found");
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
}

export class VaultFSError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.status = status;
    this.name = "VaultFSError";
  }
}

function enc(s: string): string {
  return encodeURIComponent(s);
}
