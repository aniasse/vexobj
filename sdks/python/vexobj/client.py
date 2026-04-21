"""vexobj Python SDK — simple, typed client for vexobj API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, BinaryIO, Optional
from urllib.parse import quote

import httpx


class VexObjError(Exception):
    def __init__(self, status: int, message: str):
        self.status = status
        super().__init__(f"[{status}] {message}")


@dataclass
class ObjectMeta:
    id: str
    bucket: str
    key: str
    size: int
    content_type: str
    sha256: str
    created_at: str
    updated_at: str
    metadata: dict

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectMeta":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


@dataclass
class Bucket:
    id: str
    name: str
    created_at: str
    public: bool

    @classmethod
    def from_dict(cls, d: dict) -> "Bucket":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


@dataclass
class ObjectVersion:
    id: str
    bucket: str
    key: str
    version_id: str
    size: int
    content_type: str
    sha256: str
    created_at: str
    is_latest: bool
    is_delete_marker: bool

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectVersion":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


@dataclass
class ObjectLock:
    """Retention timestamp and legal-hold flag on a live object."""

    retain_until: Optional[str]
    legal_hold: bool

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectLock":
        return cls(
            retain_until=d.get("retain_until"),
            legal_hold=bool(d.get("legal_hold", False)),
        )


@dataclass
class LifecycleRule:
    id: str
    bucket: str
    prefix: str
    expire_days: int
    created_at: str

    @classmethod
    def from_dict(cls, d: dict) -> "LifecycleRule":
        return cls(**{k: d.get(k) for k in cls.__dataclass_fields__})


class VexObj:
    """vexobj API client.

    Usage:
        vfs = VexObj("http://localhost:8000", "vfs_your_api_key")
        vfs.create_bucket("photos")
        vfs.put_object("photos", "cat.jpg", open("cat.jpg", "rb"), "image/jpeg")
        data = vfs.get_object("photos", "cat.jpg")
    """

    def __init__(self, base_url: str, api_key: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=timeout,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "VexObj":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _check(self, resp: httpx.Response) -> httpx.Response:
        if resp.status_code >= 400:
            try:
                msg = resp.json().get("error", resp.text)
            except Exception:
                msg = resp.text
            raise VexObjError(resp.status_code, msg)
        return resp

    # ─── Buckets ──────────────────────────────────────────

    def create_bucket(self, name: str, public: bool = False) -> Bucket:
        resp = self._check(
            self._client.post("/v1/buckets", json={"name": name, "public": public})
        )
        return Bucket.from_dict(resp.json())

    def list_buckets(self) -> list[Bucket]:
        resp = self._check(self._client.get("/v1/buckets"))
        return [Bucket.from_dict(b) for b in resp.json()["buckets"]]

    def get_bucket(self, name: str) -> Bucket:
        resp = self._check(self._client.get(f"/v1/buckets/{quote(name)}"))
        return Bucket.from_dict(resp.json())

    def delete_bucket(self, name: str) -> None:
        self._check(self._client.delete(f"/v1/buckets/{quote(name)}"))

    # ─── Objects ──────────────────────────────────────────

    def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes | BinaryIO,
        content_type: Optional[str] = None,
    ) -> ObjectMeta:
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type

        if hasattr(data, "read"):
            content = data.read()
        else:
            content = data

        resp = self._check(
            self._client.put(
                f"/v1/objects/{quote(bucket)}/{key}",
                content=content,
                headers=headers,
            )
        )
        return ObjectMeta.from_dict(resp.json())

    def get_object(
        self,
        bucket: str,
        key: str,
        *,
        version_id: Optional[str] = None,
    ) -> bytes:
        params = {"version_id": version_id} if version_id else None
        resp = self._check(
            self._client.get(
                f"/v1/objects/{quote(bucket)}/{key}",
                params=params,
            )
        )
        return resp.content

    def get_image(
        self,
        bucket: str,
        key: str,
        *,
        w: Optional[int] = None,
        h: Optional[int] = None,
        format: Optional[str] = None,
        quality: Optional[int] = None,
        fit: Optional[str] = None,
    ) -> bytes:
        params: dict[str, Any] = {}
        if w is not None:
            params["w"] = w
        if h is not None:
            params["h"] = h
        if format is not None:
            params["format"] = format
        if quality is not None:
            params["quality"] = quality
        if fit is not None:
            params["fit"] = fit

        resp = self._check(
            self._client.get(f"/v1/objects/{quote(bucket)}/{key}", params=params)
        )
        return resp.content

    def image_url(
        self,
        bucket: str,
        key: str,
        *,
        w: Optional[int] = None,
        h: Optional[int] = None,
        format: Optional[str] = None,
        quality: Optional[int] = None,
        fit: Optional[str] = None,
    ) -> str:
        params = []
        if w is not None:
            params.append(f"w={w}")
        if h is not None:
            params.append(f"h={h}")
        if format is not None:
            params.append(f"format={format}")
        if quality is not None:
            params.append(f"quality={quality}")
        if fit is not None:
            params.append(f"fit={fit}")
        qs = "&".join(params)
        url = f"{self.base_url}/v1/objects/{quote(bucket)}/{key}"
        return f"{url}?{qs}" if qs else url

    def head_object(self, bucket: str, key: str) -> dict[str, str]:
        resp = self._check(
            self._client.head(f"/v1/objects/{quote(bucket)}/{key}")
        )
        return dict(resp.headers)

    def delete_object(
        self,
        bucket: str,
        key: str,
        *,
        version_id: Optional[str] = None,
    ) -> None:
        params = {"version_id": version_id} if version_id else None
        self._check(
            self._client.delete(
                f"/v1/objects/{quote(bucket)}/{key}",
                params=params,
            )
        )

    def list_objects(
        self,
        bucket: str,
        *,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        max_keys: Optional[int] = None,
        continuation_token: Optional[str] = None,
    ) -> dict:
        params: dict[str, Any] = {}
        if prefix:
            params["prefix"] = prefix
        if delimiter:
            params["delimiter"] = delimiter
        if max_keys:
            params["max_keys"] = max_keys
        if continuation_token:
            params["continuation_token"] = continuation_token

        resp = self._check(
            self._client.get(f"/v1/objects/{quote(bucket)}", params=params)
        )
        return resp.json()

    # ─── Streaming (large files) ──────────────────────────

    def stream_upload(
        self,
        bucket: str,
        key: str,
        file: BinaryIO,
        content_type: Optional[str] = None,
    ) -> ObjectMeta:
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type

        resp = self._check(
            self._client.put(
                f"/v1/stream/{quote(bucket)}/{key}",
                content=file,
                headers=headers,
            )
        )
        return ObjectMeta.from_dict(resp.json())

    def stream_download(self, bucket: str, key: str) -> httpx.Response:
        """Returns a streaming response. Use .iter_bytes() to read chunks."""
        resp = self._client.stream(
            "GET", f"/v1/stream/{quote(bucket)}/{key}"
        )
        return resp

    # ─── Presigned URLs ───────────────────────────────────

    def presign(
        self,
        method: str,
        bucket: str,
        key: str,
        expires_in: int = 3600,
    ) -> dict:
        resp = self._check(
            self._client.post(
                "/v1/presign",
                json={
                    "method": method,
                    "bucket": bucket,
                    "key": key,
                    "expires_in": expires_in,
                },
            )
        )
        return resp.json()

    # ─── Admin ────────────────────────────────────────────

    def create_api_key(
        self,
        name: str,
        permissions: Optional[dict] = None,
    ) -> dict:
        body: dict[str, Any] = {"name": name}
        if permissions:
            body["permissions"] = permissions
        resp = self._check(
            self._client.post("/v1/admin/keys", json=body)
        )
        return resp.json()

    def list_api_keys(self) -> list[dict]:
        resp = self._check(self._client.get("/v1/admin/keys"))
        return resp.json()["keys"]

    def delete_api_key(self, key_id: str) -> None:
        self._check(self._client.delete(f"/v1/admin/keys/{key_id}"))

    def stats(self) -> dict:
        resp = self._check(self._client.get("/v1/stats"))
        return resp.json()

    def gc(self) -> dict:
        resp = self._check(self._client.post("/v1/admin/gc"))
        return resp.json()

    # ─── Versioning ───────────────────────────────────────

    def enable_versioning(self, bucket: str) -> None:
        """Enable versioning on a bucket. Once on, cannot be turned off."""
        self._check(self._client.post(f"/v1/admin/versioning/{quote(bucket)}"))

    def list_versions(self, bucket: str, key: str) -> list[ObjectVersion]:
        resp = self._check(
            self._client.get(f"/v1/versions/{quote(bucket)}/{key}")
        )
        return [ObjectVersion.from_dict(v) for v in resp.json()["versions"]]

    def purge_versions(self, bucket: str, key: str) -> dict:
        """Hard-delete every version and the live object for a key."""
        resp = self._check(
            self._client.delete(f"/v1/versions/{quote(bucket)}/{key}")
        )
        return resp.json()

    # ─── Object lock ──────────────────────────────────────

    def get_lock(self, bucket: str, key: str) -> ObjectLock:
        resp = self._check(
            self._client.get(f"/v1/admin/lock/{quote(bucket)}/{key}")
        )
        return ObjectLock.from_dict(resp.json())

    def set_lock(
        self,
        bucket: str,
        key: str,
        *,
        retain_until: Optional[str] = None,
        legal_hold: bool = False,
    ) -> ObjectLock:
        """Set retention and/or legal hold.

        `retain_until` can only be extended once set — shortening while
        still active returns HTTP 409.
        """
        body: dict[str, Any] = {"legal_hold": legal_hold}
        if retain_until is not None:
            body["retain_until"] = retain_until
        resp = self._check(
            self._client.put(
                f"/v1/admin/lock/{quote(bucket)}/{key}",
                json=body,
            )
        )
        return ObjectLock.from_dict(resp.json())

    def release_legal_hold(self, bucket: str, key: str) -> None:
        """Clear the legal-hold flag. Retention (if any) stays in effect."""
        self._check(
            self._client.delete(f"/v1/admin/lock/{quote(bucket)}/{key}")
        )

    # ─── Lifecycle ────────────────────────────────────────

    def create_lifecycle_rule(
        self,
        bucket: str,
        expire_days: int,
        *,
        prefix: str = "",
    ) -> LifecycleRule:
        resp = self._check(
            self._client.post(
                f"/v1/admin/lifecycle/{quote(bucket)}",
                json={"prefix": prefix, "expire_days": expire_days},
            )
        )
        return LifecycleRule.from_dict(resp.json())

    def list_lifecycle_rules(self, bucket: str) -> list[LifecycleRule]:
        resp = self._check(
            self._client.get(f"/v1/admin/lifecycle/{quote(bucket)}")
        )
        return [LifecycleRule.from_dict(r) for r in resp.json()["rules"]]

    def delete_lifecycle_rule(self, rule_id: str) -> None:
        self._check(self._client.delete(f"/v1/admin/lifecycle/rule/{rule_id}"))

    def run_lifecycle(self) -> dict:
        """Run all lifecycle rules now (expire eligible objects)."""
        resp = self._check(self._client.post("/v1/admin/lifecycle/run"))
        return resp.json()
