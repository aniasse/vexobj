"""VaultFS Python SDK — simple, typed client for VaultFS API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, BinaryIO, Optional
from urllib.parse import quote

import httpx


class VaultFSError(Exception):
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


class VaultFS:
    """VaultFS API client.

    Usage:
        vfs = VaultFS("http://localhost:8000", "vfs_your_api_key")
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

    def __enter__(self) -> "VaultFS":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def _check(self, resp: httpx.Response) -> httpx.Response:
        if resp.status_code >= 400:
            try:
                msg = resp.json().get("error", resp.text)
            except Exception:
                msg = resp.text
            raise VaultFSError(resp.status_code, msg)
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

    def get_object(self, bucket: str, key: str) -> bytes:
        resp = self._check(
            self._client.get(f"/v1/objects/{quote(bucket)}/{key}")
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

    def delete_object(self, bucket: str, key: str) -> None:
        self._check(
            self._client.delete(f"/v1/objects/{quote(bucket)}/{key}")
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
