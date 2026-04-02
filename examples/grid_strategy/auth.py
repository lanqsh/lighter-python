import logging
import time

import lighter

LOGGER = logging.getLogger("smart_grid")


class AuthTokenManager:
    REFRESH_BEFORE_SEC = 120

    def __init__(self, client: lighter.SignerClient, ttl_sec: int = 3600):
        self._client     = client
        self._ttl        = ttl_sec
        self._token:     str   = ""
        self._expire_at: float = 0.0

    async def get(self) -> str:
        if time.time() + self.REFRESH_BEFORE_SEC >= self._expire_at:
            token, err = self._client.create_auth_token_with_expiry(deadline=self._ttl)
            if err is not None:
                raise RuntimeError(f"Failed to create auth token: {err}")
            self._token     = token
            self._expire_at = time.time() + self._ttl
            LOGGER.info("[auth] token refreshed (valid %ss)", self._ttl)
        return self._token
