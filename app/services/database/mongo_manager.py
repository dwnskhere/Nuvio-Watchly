from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from loguru import logger

from app.core.config import settings


class MongoDBError(RuntimeError):
    """Base error for the MongoDB storage layer."""


class NoAvailableShardError(MongoDBError):
    """No configured shard has enough capacity for a new account."""


class UserMigrationInProgressError(MongoDBError):
    """Writes are paused while an account is being migrated."""


class MongoDBManager:
    """User-level shard router with a central control plane.

    The control database stores shard registry, user assignments, identity indexes,
    aliases, and migration jobs. Each data shard stores user documents and cache
    entries. All configured shards must use the same collection/schema layout.
    """

    def __init__(self, settings_obj: Any = settings, client_factory: Any = None):
        self.settings = settings_obj
        self.client_factory = client_factory or self._default_client_factory
        self.clients: dict[str, Any] = {}
        self.shards: dict[str, dict[str, Any]] = {}
        self.order: list[str] = []
        self.control_db: Any | None = None
        self.initialized = False
        self._allocation_lock = asyncio.Lock()

    @staticmethod
    def _default_client_factory(uri: str, **kwargs: Any) -> Any:
        from pymongo import AsyncMongoClient
        return AsyncMongoClient(uri, **kwargs)

    @staticmethod
    def shard_id_for_url(uri: str) -> str:
        host = uri.rsplit("@", 1)[-1].split("/", 1)[0].split("?", 1)[0].lower()
        return "mongo-" + hashlib.sha256(host.encode()).hexdigest()[:12]

    @property
    def urls(self) -> list[str]:
        if hasattr(self.settings, "mongodb_urls"):
            return list(self.settings.mongodb_urls)
        raw = getattr(self.settings, "MONGODB_URLS", "") or ""
        urls = [part.strip() for part in raw.split(";") if part.strip()]
        single = getattr(self.settings, "MONGODB_URL", None)
        if single and single.strip():
            urls.insert(0, single.strip())
        return list(dict.fromkeys(urls))

    @property
    def control_url(self) -> str | None:
        return getattr(self.settings, "MONGODB_CONTROL_URL", None) or (self.urls[0] if self.urls else None)

    def _options(self) -> dict[str, Any]:
        return {
            "maxPoolSize": int(getattr(self.settings, "MONGODB_MAX_POOL_SIZE", 20)),
            "serverSelectionTimeoutMS": int(getattr(self.settings, "MONGODB_SERVER_SELECTION_TIMEOUT_MS", 5000)),
            "connectTimeoutMS": int(getattr(self.settings, "MONGODB_CONNECT_TIMEOUT_MS", 5000)),
            "appname": "Watchly",
            "tz_aware": True,
        }

    @staticmethod
    def now() -> datetime:
        return datetime.now(timezone.utc)

    async def initialize(self) -> None:
        if self.initialized:
            return
        urls = self.urls
        if not urls:
            raise MongoDBError("Configure MONGODB_URLS or MONGODB_URL before starting Watchly.")
        ids = [part.strip() for part in (getattr(self.settings, "MONGODB_SHARD_IDS", "") or "").split(";") if part.strip()]
        if ids and len(ids) != len(urls):
            raise MongoDBError("MONGODB_SHARD_IDS must have one ID per MongoDB URI.")
        db_name = getattr(self.settings, "MONGODB_DATABASE_NAME", "watchly")
        options = self._options()

        for index, uri in enumerate(urls):
            shard_id = ids[index] if ids else self.shard_id_for_url(uri)
            if shard_id in self.shards:
                raise MongoDBError(f"Duplicate MongoDB shard ID: {shard_id}")
            client = self.client_factory(uri, **options)
            self.clients[uri] = client
            await client.admin.command("ping")
            db = client[db_name]
            await db["users"].create_index("expires_at", expireAfterSeconds=0, name="users_ttl")
            await db["cache_entries"].create_index("expires_at", expireAfterSeconds=0, name="cache_ttl")
            await db["cache_entries"].create_index("owner_token", name="cache_owner")
            self.shards[shard_id] = {"db": db, "uri": uri, "ordinal": index}
            self.order.append(shard_id)

        control_url = self.control_url
        control_client = self.clients.get(control_url)
        if control_client is None:
            control_client = self.client_factory(control_url, **options)
            await control_client.admin.command("ping")
            self.clients[control_url] = control_client
        self.control_db = control_client[getattr(self.settings, "MONGODB_CONTROL_DATABASE_NAME", "watchly_control")]

        for name, keys in {
            "identity_index": [("expires_at", 1)],
            "token_aliases": [("expires_at", 1)],
            "allocation_locks": [("expires_at", 1)],
            "global_cache": [("expires_at", 1)],
            "user_shard_map": [("shard_id", 1)],
            "migration_jobs": [("created_at", 1)],
        }.items():
            await self.control_db[name].create_index(keys, expireAfterSeconds=0 if name in {"identity_index", "token_aliases", "allocation_locks", "global_cache"} else None)

        for shard_id, shard in self.shards.items():
            await self.control_db["shard_registry"].update_one(
                {"_id": shard_id},
                {"$set": {"ordinal": shard["ordinal"], "database": db_name, "enabled": True, "last_seen_at": self.now()},
                 "$setOnInsert": {"created_at": self.now()}},
                upsert=True,
            )
        self.initialized = True
        await self.recover_interrupted_migrations()

    def _ready(self) -> None:
        if not self.initialized or self.control_db is None:
            raise MongoDBError("MongoDB is not initialized.")

    @staticmethod
    def select_shard(overview: list[dict[str, Any]], order: list[str], threshold: float, reserve: int) -> str | None:
        candidates = [
            item for item in overview
            if item.get("eligible") and item.get("capacity_bytes", 0) > 0
            and item.get("used_bytes", 0) + reserve < item.get("capacity_bytes", 0) * threshold
        ]
        return min(candidates, key=lambda item: order.index(item["shard_id"]))["shard_id"] if candidates else None

    async def shard_capacity(self, shard_id: str) -> dict[str, Any]:
        self._ready()
        shard = self.shards.get(shard_id)
        if shard is None:
            raise MongoDBError(f"Unknown MongoDB shard: {shard_id}")
        stats = await shard["db"].command("dbStats", scale=1)
        used = int(stats.get("dataSize", 0) or 0) + int(stats.get("indexSize", 0) or 0)
        capacity = int(getattr(self.settings, "MONGODB_MAX_STORAGE_BYTES", 512 * 1024 * 1024))
        ratio = used / capacity if capacity else 1.0
        threshold = float(getattr(self.settings, "MONGODB_ALLOCATION_THRESHOLD", 0.80))
        return {"shard_id": shard_id, "used_bytes": used, "capacity_bytes": capacity,
                "ratio": ratio, "eligible": ratio < threshold}

    async def capacity_overview(self) -> list[dict[str, Any]]:
        self._ready()
        result = []
        for shard_id in self.order:
            try:
                item = await self.shard_capacity(shard_id)
                result.append(item)
                await self.control_db["shard_registry"].update_one(
                    {"_id": shard_id},
                    {"$set": {"last_used_bytes": item["used_bytes"], "last_ratio": item["ratio"], "last_seen_at": self.now()}},
                )
            except Exception as exc:
                logger.warning(f"Could not measure MongoDB shard {shard_id}: {type(exc).__name__}")
                result.append({"shard_id": shard_id, "eligible": False, "error": type(exc).__name__})
        return result

    async def _acquire_allocation_lock(self, owner: str) -> bool:
        collection = self.control_db["allocation_locks"]
        now = self.now()
        try:
            await collection.insert_one({"_id": "new-user-allocation", "owner": owner, "expires_at": now + timedelta(seconds=30)})
            return True
        except Exception as exc:
            if type(exc).__name__ != "DuplicateKeyError":
                raise
        result = await collection.update_one(
            {"_id": "new-user-allocation", "expires_at": {"$lte": now}},
            {"$set": {"owner": owner, "expires_at": now + timedelta(seconds=30)}},
        )
        return bool(result.modified_count)

    async def ensure_user_shard(self, token: str) -> str:
        self._ready()
        mappings = self.control_db["user_shard_map"]
        current = await mappings.find_one({"_id": token})
        if current:
            return str(current["shard_id"])
        owner = uuid4().hex
        for _ in range(40):
            if await self._acquire_allocation_lock(owner):
                break
            await asyncio.sleep(0.05)
        else:
            raise MongoDBError("MongoDB allocation is busy; retry shortly.")
        try:
            current = await mappings.find_one({"_id": token})
            if current:
                return str(current["shard_id"])
            overview = await self.capacity_overview()
            chosen = self.select_shard(
                overview, self.order,
                float(getattr(self.settings, "MONGODB_ALLOCATION_THRESHOLD", 0.80)),
                int(getattr(self.settings, "MONGODB_NEW_USER_RESERVE_BYTES", 1024 * 1024)),
            )
            if chosen is None:
                raise NoAvailableShardError("All configured MongoDB shards reached their allocation threshold. Add another URI.")
            await mappings.insert_one({"_id": token, "shard_id": chosen, "status": "active",
                                       "assigned_at": self.now(), "updated_at": self.now()})
            return chosen
        finally:
            await self.control_db["allocation_locks"].delete_one({"_id": "new-user-allocation", "owner": owner})

    async def get_user_shard(self, token: str, *, create: bool = False, for_write: bool = False) -> str | None:
        self._ready()
        mapping = await self.control_db["user_shard_map"].find_one({"_id": token})
        if mapping is None:
            return await self.ensure_user_shard(token) if create else None
        if for_write and mapping.get("status") == "migrating":
            raise UserMigrationInProgressError("Account migration is in progress; retry this write shortly.")
        return str(mapping["shard_id"])

    async def database_for_user(self, token: str, *, create: bool = False, for_write: bool = False) -> Any | None:
        shard_id = await self.get_user_shard(token, create=create, for_write=for_write)
        if shard_id is None:
            return None
        if shard_id not in self.shards:
            raise MongoDBError(f"User is mapped to an unconfigured MongoDB shard: {shard_id}")
        return self.shards[shard_id]["db"]

    async def count_users(self) -> int:
        self._ready()
        now = self.now()
        total = 0
        for shard in self.shards.values():
            total += await shard["db"]["users"].count_documents(
                {"$or": [{"expires_at": {"$exists": False}}, {"expires_at": None}, {"expires_at": {"$gt": now}}]}
            )
        return total

    async def unassign_user(self, token: str) -> None:
        self._ready()
        await self.control_db["user_shard_map"].delete_one({"_id": token, "status": {"$ne": "migrating"}})

    async def migrate_user(self, token: str, target_shard_id: str) -> dict[str, Any]:
        """Admin-triggered, write-frozen migration. Automatic migration stays disabled."""
        self._ready()
        if target_shard_id not in self.shards:
            raise MongoDBError(f"Target shard is not configured: {target_shard_id}")
        mappings = self.control_db["user_shard_map"]
        mapping = await mappings.find_one({"_id": token})
        if not mapping:
            raise MongoDBError("Cannot migrate a user without an existing shard assignment.")
        source_id = str(mapping["shard_id"])
        if source_id == target_shard_id:
            return {"status": "already_on_target", "source": source_id, "target": target_shard_id}
        target_capacity = await self.shard_capacity(target_shard_id)
        if not target_capacity["eligible"]:
            raise NoAvailableShardError("Destination shard is at its allocation threshold.")
        job_id, now = uuid4().hex, self.now()
        locked = await mappings.update_one(
            {"_id": token, "shard_id": source_id, "status": "active"},
            {"$set": {"status": "migrating", "target_shard_id": target_shard_id,
                      "migration_id": job_id, "migration_started_at": now}},
        )
        if not locked.matched_count:
            raise MongoDBError("Account mapping changed or another migration is running.")
        source, target = self.shards[source_id]["db"], self.shards[target_shard_id]["db"]
        jobs = self.control_db["migration_jobs"]
        await jobs.insert_one({"_id": job_id, "user_id": token, "source_shard_id": source_id,
                               "target_shard_id": target_shard_id, "status": "copying", "created_at": now})
        copied = False
        switched = False
        try:
            source_user = await source["users"].find_one({"_id": token})
            if source_user is None:
                raise MongoDBError("Source shard has no user document for this account.")
            if await target["users"].find_one({"_id": token}):
                raise MongoDBError("Destination already contains this user; refusing to overwrite it.")
            await target["users"].replace_one({"_id": token}, source_user, upsert=True)
            copied = True
            source_cache = [doc async for doc in source["cache_entries"].find({"owner_token": token})]
            for doc in source_cache:
                await target["cache_entries"].replace_one({"_id": doc["_id"]}, doc, upsert=True)
            target_user = await target["users"].find_one({"_id": token})
            target_cache = [doc async for doc in target["cache_entries"].find({"owner_token": token})]
            if target_user != source_user or {d["_id"]: d for d in target_cache} != {d["_id"]: d for d in source_cache}:
                raise MongoDBError("Migration verification failed; source remains authoritative.")
            result = await mappings.update_one(
                {"_id": token, "migration_id": job_id, "status": "migrating"},
                {"$set": {"shard_id": target_shard_id, "status": "active", "updated_at": self.now(),
                          "migrated_at": self.now(), "previous_shard_id": source_id},
                 "$unset": {"target_shard_id": "", "migration_id": "", "migration_started_at": ""}},
            )
            if not result.matched_count:
                raise MongoDBError("Account mapping changed before migration commit.")
            switched = True
            await jobs.update_one({"_id": job_id}, {"$set": {"status": "verified_and_switched", "completed_at": self.now()}})
            await source["users"].delete_one({"_id": token})
            await source["cache_entries"].delete_many({"owner_token": token})
            return {"status": "completed", "job_id": job_id, "source": source_id, "target": target_shard_id}
        except Exception as exc:
            if switched:
                await jobs.update_one({"_id": job_id}, {"$set": {"status": "cleanup_pending", "error_type": type(exc).__name__}})
                return {"status": "completed_cleanup_pending", "job_id": job_id, "source": source_id, "target": target_shard_id}
            await mappings.update_one(
                {"_id": token, "migration_id": job_id, "status": "migrating"},
                {"$set": {"status": "active", "updated_at": self.now()},
                 "$unset": {"target_shard_id": "", "migration_id": "", "migration_started_at": ""}},
            )
            if copied:
                await target["users"].delete_one({"_id": token})
                await target["cache_entries"].delete_many({"owner_token": token})
            await jobs.update_one({"_id": job_id}, {"$set": {"status": "failed", "error_type": type(exc).__name__}})
            raise

    async def recover_interrupted_migrations(self) -> None:
        cutoff = self.now() - timedelta(minutes=15)
        cursor = self.control_db["user_shard_map"].find(
            {"status": "migrating", "migration_started_at": {"$lt": cutoff}}
        )
        async for mapping in cursor:
            token, target_id, job_id = mapping["_id"], mapping.get("target_shard_id"), mapping.get("migration_id")
            await self.control_db["user_shard_map"].update_one(
                {"_id": token, "status": "migrating", "migration_id": job_id},
                {"$set": {"status": "active", "updated_at": self.now()},
                 "$unset": {"target_shard_id": "", "migration_id": "", "migration_started_at": ""}},
            )
            if target_id in self.shards:
                target = self.shards[target_id]["db"]
                await target["users"].delete_one({"_id": token})
                await target["cache_entries"].delete_many({"owner_token": token})
            if job_id:
                await self.control_db["migration_jobs"].update_one(
                    {"_id": job_id}, {"$set": {"status": "recovered_after_restart", "completed_at": self.now()}}
                )

    async def close(self) -> None:
        unique = {id(client): client for client in self.clients.values()}
        for client in unique.values():
            try:
                await client.close()
            except Exception as exc:
                logger.warning(f"Failed to close MongoDB client: {type(exc).__name__}")
        self.clients.clear()
        self.shards.clear()
        self.order.clear()
        self.control_db = None
        self.initialized = False


mongo_manager = MongoDBManager()
