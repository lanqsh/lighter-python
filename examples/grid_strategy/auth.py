import asyncio
import logging
import time

import lighter

LOGGER = logging.getLogger("smart_grid")


class AuthTokenManager:
    REFRESH_BEFORE_SEC = 120
    TOKEN_MAX_ATTEMPTS = 3

    def __init__(self, client: lighter.SignerClient, ttl_sec: int = 3600):
        self._client     = client
        self._ttl        = ttl_sec
        self._token:     str   = ""
        self._expire_at: float = 0.0

    async def get(self) -> str:
        if time.time() + self.REFRESH_BEFORE_SEC >= self._expire_at:
            last_exc: Exception | None = None
            for attempt in range(1, self.TOKEN_MAX_ATTEMPTS + 1):
                try:
                    token, err = self._client.create_auth_token_with_expiry(deadline=self._ttl)
                    if err is None:
                        self._token = token
                        self._expire_at = time.time() + self._ttl
                        LOGGER.info("[auth] token refreshed (valid %ss)", self._ttl)
                        break

                    last_exc = RuntimeError(f"Failed to create auth token: {err}")
                    if attempt >= self.TOKEN_MAX_ATTEMPTS:
                        raise last_exc
                    delay = 0.6 * attempt
                    LOGGER.warning(
                        "[auth:retry] create token failed attempt=%s/%s reason=%s sleep=%.1fs",
                        attempt,
                        self.TOKEN_MAX_ATTEMPTS,
                        err,
                        delay,
                    )
                    await asyncio.sleep(delay)
                except Exception as exc:
                    last_exc = exc
                    if attempt >= self.TOKEN_MAX_ATTEMPTS:
                        raise RuntimeError(f"Failed to create auth token: {exc}") from exc
                    delay = 0.6 * attempt
                    LOGGER.warning(
                        "[auth:retry] token exception attempt=%s/%s reason=%s sleep=%.1fs",
                        attempt,
                        self.TOKEN_MAX_ATTEMPTS,
                        exc,
                        delay,
                    )
                    await asyncio.sleep(delay)

            if not self._token:
                if last_exc is not None:
                    raise RuntimeError(f"Failed to create auth token: {last_exc}")
                raise RuntimeError("Failed to create auth token: unknown error")
        return self._token

    async def force_refresh(self) -> str:
        self._expire_at = 0.0
        return await self.get()
