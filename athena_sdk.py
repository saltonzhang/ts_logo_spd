"""Athena Python SDK client."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib import error, request

import boto3


class AthenaSDKError(Exception):
    """Athena SDK request error."""

    def __init__(self, message: str, status_code: Optional[int] = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class CredentialsTokenResponse:
    access_key_id: str
    secret_access_key: str
    session_token: str
    expires_at: datetime
    expires_in: int
    context: Optional[Dict[str, str]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CredentialsTokenResponse":
        expires_at_raw = str(data["expires_at"])
        # 兼容类似 2026-01-01T00:00:00Z 的时间格式
        if expires_at_raw.endswith("Z"):
            expires_at_raw = expires_at_raw[:-1] + "+00:00"
        return cls(
            access_key_id=str(data["access_key_id"]),
            secret_access_key=str(data["secret_access_key"]),
            session_token=str(data["session_token"]),
            expires_at=datetime.fromisoformat(expires_at_raw),
            expires_in=int(data["expires_in"]),
            context=(data.get("context") or None),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "access_key_id": self.access_key_id,
            "secret_access_key": self.secret_access_key,
            "session_token": self.session_token,
            "expires_at": self.expires_at.isoformat(),
            "expires_in": self.expires_in,
            "context": self.context or {},
        }


class AthenaClient:
    """Athena HTTP client."""

    def __init__(
        self,
        base_url: str,
        access_key: str,
        secret_key: str,
        timeout_seconds: int = 10,
        refresh_before_seconds: int = 600,
        region_name: Optional[str] = None,
        token_cache_file: Optional[str] = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_seconds = timeout_seconds
        self._refresh_before = timedelta(seconds=refresh_before_seconds)
        self._token_cache: Dict[str, CredentialsTokenResponse] = {}
        self._access_key = access_key
        self._secret_key = secret_key
        self._region_name = region_name
        self._token_cache_file = Path(token_cache_file).expanduser() if token_cache_file else None

    def get_token(
        self,
        force_refresh: bool = False,
    ) -> CredentialsTokenResponse:
        cache_key = f"{self._access_key}:{self._secret_key}"
        fallback_token: Optional[CredentialsTokenResponse] = None
        if not force_refresh:
            cached = self._token_cache.get(cache_key)
            if cached and self._is_token_valid(cached):
                return cached
            if cached and self._is_token_not_expired(cached):
                fallback_token = cached
            disk_cached = self._load_token_from_disk(cache_key)
            if disk_cached and self._is_token_valid(disk_cached):
                self._token_cache[cache_key] = disk_cached
                return disk_cached
            if disk_cached and self._is_token_not_expired(disk_cached):
                fallback_token = self._select_later_expire_token(fallback_token, disk_cached)

        payload = {
            "access_key": self._access_key,
            "secret_key": self._secret_key,
        }
        try:
            data = self._post_json("/v1/credentials/token", payload)
        except AthenaSDKError as exc:
            if (
                exc.status_code == 429
                and fallback_token is not None
                and self._is_token_not_expired(fallback_token)
            ):
                # 命中限流时优先复用未过期 token，避免流程中断
                self._token_cache[cache_key] = fallback_token
                return fallback_token
            raise
        token = CredentialsTokenResponse.from_dict(data)
        self._token_cache[cache_key] = token
        self._save_token_to_disk(cache_key, token)
        return token

    def upload_file(
        self,
        Filename: str,
        Key: str,
        Bucket: str = "",
        ExtraArgs: Optional[Dict[str, Any]] = None,
        Callback: Optional[Any] = None,
        Config: Optional[Any] = None,
    ) -> Any:
        """
        封装 boto3 S3 upload_file，参数与返回保持原生风格。

        该方法依赖 Athena token 自动创建临时凭证 S3 client。
        """
        token = self.get_token()
        region_name = self._region_name or (token.context or {}).get("region")
        if not region_name:
            raise AthenaSDKError(
                "Missing region_name. Set AthenaClient(region_name=...) "
                "or ensure token.context contains region."
            )
        _bucket, resolved_key = self._resolve_bucket_and_key(token, Bucket=Bucket, Key=Key)

        s3 = boto3.client(
            "s3",
            region_name=region_name,
            aws_access_key_id=token.access_key_id,
            aws_secret_access_key=token.secret_access_key,
            aws_session_token=token.session_token,
        )
        return s3.upload_file(
            Filename=Filename,
            Bucket=_bucket,
            Key=resolved_key,
            ExtraArgs=ExtraArgs,
            Callback=Callback,
            Config=Config,
        )

    def resolve_upload_target(self, Key: str, Bucket: str = "") -> Dict[str, Optional[str]]:
        """解析最终 bucket/key，并返回可用的 CDN URL（如有）。"""
        token = self.get_token()
        bucket_name, final_key = self._resolve_bucket_and_key(token, Bucket=Bucket, Key=Key)
        cdn_domain = str((token.context or {}).get("cdn_domain") or "").rstrip("/")
        cdn_url = f"{cdn_domain}/{final_key}" if cdn_domain else None
        return {
            "bucket": bucket_name,
            "final_key": final_key,
            "cdn_url": cdn_url,
        }

    def clear_token_cache(self) -> None:
        """清空本地 token 缓存。"""
        self._token_cache.clear()
        if self._token_cache_file and self._token_cache_file.exists():
            try:
                self._token_cache_file.unlink()
            except Exception:
                pass

    def _post_json(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._base_url + path
        req = request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        return self._send(req)

    def _send(self, req: request.Request) -> Dict[str, Any]:
        try:
            with request.urlopen(req, timeout=self._timeout_seconds) as resp:
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="ignore")
            raise AthenaSDKError(
                f"Athena request failed: status={exc.code}, body={raw}",
                status_code=exc.code,
            ) from exc
        except error.URLError as exc:
            raise AthenaSDKError(f"Athena request failed: {exc}") from exc

    def _is_token_valid(self, token: CredentialsTokenResponse) -> bool:
        expires_at = token.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return (expires_at - now) > self._refresh_before

    def _is_token_not_expired(self, token: CredentialsTokenResponse) -> bool:
        expires_at = token.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        return expires_at > now

    def _resolve_bucket_and_key(
        self,
        token: CredentialsTokenResponse,
        Bucket: str,
        Key: str,
    ) -> tuple[str, str]:
        resource_prefix = ((token.context or {}).get("resource_prefix") or "").strip("/")
        resolved_key = Key.lstrip("/")
        if resource_prefix and not (
            resolved_key == resource_prefix or resolved_key.startswith(f"{resource_prefix}/")
        ):
            resolved_key = f"{resource_prefix}/{resolved_key}"
        bucket_name = str((token.context or {}).get("bucket_name") or Bucket).strip()
        if not bucket_name:
            raise AthenaSDKError("Missing bucket_name in token.context and no Bucket provided.")
        return bucket_name, resolved_key

    def _load_token_from_disk(self, cache_key: str) -> Optional[CredentialsTokenResponse]:
        if not self._token_cache_file or not self._token_cache_file.exists():
            return None
        try:
            data = json.loads(self._token_cache_file.read_text(encoding="utf-8"))
            raw = data.get(cache_key)
            if not isinstance(raw, dict):
                return None
            return CredentialsTokenResponse.from_dict(raw)
        except Exception:
            return None

    def _save_token_to_disk(self, cache_key: str, token: CredentialsTokenResponse) -> None:
        if not self._token_cache_file:
            return
        try:
            if self._token_cache_file.parent:
                self._token_cache_file.parent.mkdir(parents=True, exist_ok=True)
            existing: Dict[str, Any] = {}
            if self._token_cache_file.exists():
                try:
                    existing = json.loads(self._token_cache_file.read_text(encoding="utf-8"))
                    if not isinstance(existing, dict):
                        existing = {}
                except Exception:
                    existing = {}
            existing[cache_key] = token.to_dict()
            self._token_cache_file.write_text(
                json.dumps(existing, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception:
            # 缓存落盘失败不影响主流程
            return

    def _select_later_expire_token(
        self,
        a: Optional[CredentialsTokenResponse],
        b: Optional[CredentialsTokenResponse],
    ) -> Optional[CredentialsTokenResponse]:
        if a is None:
            return b
        if b is None:
            return a

        a_exp = a.expires_at if a.expires_at.tzinfo else a.expires_at.replace(tzinfo=timezone.utc)
        b_exp = b.expires_at if b.expires_at.tzinfo else b.expires_at.replace(tzinfo=timezone.utc)
        return a if a_exp >= b_exp else b
