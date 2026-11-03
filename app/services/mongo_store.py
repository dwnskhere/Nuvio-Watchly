from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.config import settings
from app.services.database.mongo_manager import MongoDBManager, mongo_manager


class MongoStoreService:
    """Small Redis-compatible async KV surface backed by MongoDB.

    Account documents are in each shard's users collection. Per-user caches and
    locks are in cache_entries. Identity indexes and token aliases are in the
    control database, so they can be resolved before the user's shard is known.
    """

    def __init__(self, manager: MongoDBManager = mongo_manager, settings_obj: Any = settings):
        self.manager = manager
        self.settings = settings_obj

    @property
    def token_prefix(self) -> str:
        return getattr(self.settings, "REDIS_TOKEN_KEY", "watchly:token:")

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @classmethod
    def _expires(cls, ttl: int | None) -> datetime | None:
        return cls._now() + timedelta(seconds=ttl) if ttl and ttl > 0 else None

    @staticmethod
    def token_from_key(key: str) -> str | None:
        parts = key.split(":")
        if len(parts) < 3 or parts[0] != "watchly" or parts[1] in {"identity", "token_alias"}:
            return None
        if parts[1] == "manifest":
            return parts[3] if len(parts) > 3 else None
        if parts[1] in {"rowmap", "library_buckets"}:
            return parts[3] if len(parts) > 3 else None
        return parts[2]

    def _kind(self, key: str) -> tuple[str, str | None]:
        if key.startswith(self.token_prefix):
            return "user", key.removeprefix(self.token_prefix)
        if key.startswith("watchly:identity:"):
            return "identity", None
        if key.startswith("watchly:token_alias:"):
            return "alias", None
        token = self.token_from_key(key)
        return ("cache", token) if token else ("global", None)

    async def _collection(self, key: str, *, create: bool = False, for_write: bool = False):
        kind, token = self._kind(key)
        if kind == "identity":
            return self.manager.control_db["identity_index"], kind, None
        if kind == "alias":
            return self.manager.control_db["token_aliases"], kind, None
        if kind == "global":
            return self.manager.control_db["global_cache"], kind, None
        db = await self.manager.database_for_user(token, create=create, for_write=for_write)
        if db is None:
            return None, kind, token
        return db["users" if kind == "user" else "cache_entries"], kind, token

    async def initialize(self) -> None:
        await self.manager.initialize()

    async def set(self, key: str, value: Any, ttl: int | None = None) -> bool:
        collection, kind, token = await self._collection(key, create=True, for_write=True)
        identity = token if kind == "user" else key
        doc = {"_id": identity, "value": str(value), "updated_at": self._now()}
        expires = self._expires(ttl)
        if expires:
            doc["expires_at"] = expires
        if kind == "cache":
            doc["owner_token"] = token
        await collection.replace_one({"_id": identity}, doc, upsert=True)
        return True

    async def get(self, key: str) -> str | None:
        collection, kind, _ = await self._collection(key)
        if collection is None:
            return None
        identity = key.removeprefix(self.token_prefix) if kind == "user" else key
        doc = await collection.find_one({"_id": identity})
        if not doc:
            return None
        if doc.get("expires_at") and doc["expires_at"] <= self._now():
            return None
        return doc.get("value")

    async def delete(self, key: str) -> bool:
        collection, kind, _ = await self._collection(key, for_write=True)
        if collection is None:
            return False
        identity = key.removeprefix(self.token_prefix) if kind == "user" else key
        result = await collection.delete_one({"_id": identity})
        return bool(result.deleted_count)

    async def expire(self, key: str, ttl: int) -> bool:
        collection, kind, _ = await self._collection(key, for_write=True)
        if collection is None:
            return False
        identity = key.removeprefix(self.token_prefix) if kind == "user" else key
        result = await collection.update_one(
            {"_id": identity}, {"$set": {"expires_at": self._expires(ttl), "updated_at": self._now()}}
        )
        return bool(result.matched_count)

    async def exists(self, key: str) -> bool:
        return (await self.get(key)) is not None

    async def delete_by_pattern(self, pattern: str) -> int:
        token = self.token_from_key(pattern)
        if token:
            db = await self.manager.database_for_user(token, for_write=True)
            if db is None:
                return 0
            collection = db["cache_entries"]
        else:
            collection = self.manager.control_db["global_cache"]
        regex = "^" + re.escape(pattern).replace(r"\*", ".*") + "$"
        result = await collection.delete_many({"_id": {"$regex": regex}})
        return int(result.deleted_count)

    async def set_nx(self, key: str, value: Any, ttl: int | None = None) -> bool:
        collection, kind, token = await self._collection(key, create=True, for_write=True)
        identity = token if kind == "user" else key
        doc = {"_id": identity, "value": str(value), "updated_at": self._now()}
        expires = self._expires(ttl)
        if expires:
            doc["expires_at"] = expires
        if kind == "cache":
            doc["owner_token"] = token
        try:
            await collection.insert_one(doc)
            return True
        except Exception as exc:
            if type(exc).__name__ != "DuplicateKeyError":
                raise
        result = await collection.update_one(
            {"_id": identity, "expires_at": {"$lte": self._now()}}, {"$set": doc}
        )
        return bool(result.modified_count)

    async def count_users(self) -> int:
        return await self.manager.count_users()

    async def unassign_user(self, token: str) -> None:
        await self.manager.unassign_user(token)

    async def migrate_user(self, token: str, target_shard_id: str) -> dict[str, Any]:
        return await self.manager.migrate_user(token, target_shard_id)

    async def capacity_overview(self) -> list[dict[str, Any]]:
        return await self.manager.capacity_overview()

    async def close(self) -> None:
        await self.manager.close()


mongo_store = MongoStoreService()
