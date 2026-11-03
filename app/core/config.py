from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

from app.core.version import __version__


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="allow",
    )

    # Debug is deliberately noisy — per-item and per-cache-hit detail — so it is
    # opt-in rather than the default the app used to run on.
    LOG_LEVEL: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    TMDB_API_KEY: str | None = None
    PORT: int = 8000
    ADDON_ID: str = "com.bimal.watchly"
    ADDON_NAME: str = "Watchly"
    # Semicolon-separated because replica-set URIs may contain commas.
    # MONGODB_URL remains a convenient single-shard alias.
    MONGODB_URLS: str = ""
    MONGODB_URL: str | None = None
    MONGODB_CONTROL_URL: str | None = None
    MONGODB_SHARD_IDS: str = ""
    MONGODB_DATABASE_NAME: str = "watchly"
    MONGODB_CONTROL_DATABASE_NAME: str = "watchly_control"
    MONGODB_MAX_STORAGE_BYTES: int = 512 * 1024 * 1024
    MONGODB_ALLOCATION_THRESHOLD: float = 0.80
    MONGODB_NEW_USER_RESERVE_BYTES: int = 1024 * 1024
    MONGODB_MAX_POOL_SIZE: int = 20
    MONGODB_SERVER_SELECTION_TIMEOUT_MS: int = 5000
    MONGODB_CONNECT_TIMEOUT_MS: int = 5000
    REDIS_TOKEN_KEY: str = "watchly:token:"
    TOKEN_SALT: str = "change-me"
    TOKEN_TTL_SECONDS: int = 0  # 0 = never expire
    ANNOUNCEMENT_HTML: str = ""
    ALLOW_SIGNUPS: bool = True
    AUTO_UPDATE_CATALOGS: bool = True
    CATALOG_REFRESH_INTERVAL_SECONDS: int = 86400  # 24 hours
    APP_ENV: Literal["development", "production", "vercel"] = "production"
    HOST_NAME: str = "https://1ccea4301587-watchly.baby-beamup.club"

    RECOMMENDATION_SOURCE_ITEMS_LIMIT: int = 10
    LIBRARY_ITEMS_LIMIT: int = 20

    # How long a client may reuse a catalog response. Short on purpose: served ids
    # are stable slots now, so this header is the only thing telling Stremio a row's
    # content has moved. stale-while-revalidate keeps the UI instant past it.
    CATALOG_CACHE_TTL: int = 60
    CATALOG_STALE_TTL: int = 604800  # 7 days (soft expiration fallback)

    # External history providers (OAuth app credentials)
    TRAKT_CLIENT_ID: str | None = None
    TRAKT_CLIENT_SECRET: str | None = None
    SIMKL_CLIENT_ID: str | None = None
    SIMKL_CLIENT_SECRET: str | None = None

    @property
    def mongodb_urls(self) -> list[str]:
        urls = [value.strip() for value in self.MONGODB_URLS.split(";") if value.strip()]
        if self.MONGODB_URL and self.MONGODB_URL.strip() and self.MONGODB_URL.strip() not in urls:
            urls.insert(0, self.MONGODB_URL.strip())
        return list(dict.fromkeys(urls))


settings = Settings()

APP_VERSION = __version__
