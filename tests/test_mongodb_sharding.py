from types import SimpleNamespace

from app.services.database.mongo_manager import MongoDBManager
from app.services.mongo_store import MongoStoreService


def test_shard_id_is_stable_when_credentials_rotate():
    first = "mongodb+srv://user:old@cluster-a.example.net/watchly?retryWrites=true"
    rotated = "mongodb+srv://user:new@cluster-a.example.net/watchly?retryWrites=true"
    other = "mongodb+srv://user:pass@cluster-b.example.net/watchly?retryWrites=true"
    assert MongoDBManager.shard_id_for_url(first) == MongoDBManager.shard_id_for_url(rotated)
    assert MongoDBManager.shard_id_for_url(first) != MongoDBManager.shard_id_for_url(other)
    # Credentials are optional in local/managed MongoDB connection strings.
    assert MongoDBManager.shard_id_for_url(
        "mongodb://cluster-c.example.net/watchly"
    ) != MongoDBManager.shard_id_for_url("mongodb://cluster-d.example.net/watchly")


def test_allocator_fills_shards_in_configured_order():
    overview = [
        {"shard_id": "first", "used_bytes": 420, "capacity_bytes": 512, "eligible": False},
        {"shard_id": "second", "used_bytes": 10, "capacity_bytes": 512, "eligible": True},
        {"shard_id": "third", "used_bytes": 5, "capacity_bytes": 512, "eligible": True},
    ]
    assert MongoDBManager.select_shard(overview, ["first", "second", "third"], 0.80, 1) == "second"


def test_allocator_returns_none_when_all_shards_are_full():
    overview = [
        {"shard_id": "first", "used_bytes": 450, "capacity_bytes": 512, "eligible": False},
        {"shard_id": "second", "used_bytes": 430, "capacity_bytes": 512, "eligible": False},
    ]
    assert MongoDBManager.select_shard(overview, ["first", "second"], 0.80, 1) is None


def test_all_current_user_cache_key_formats_resolve_to_owner():
    store = MongoStoreService(manager=SimpleNamespace(), settings_obj=SimpleNamespace(REDIS_TOKEN_KEY="watchly:token:"))
    token = "opaque_token-123"
    keys = [
        f"watchly:library_items:{token}",
        f"watchly:nuvio_refresh:{token}",
        f"watchly:profile:{token}:movie",
        f"watchly:watched_sets:{token}:series",
        f"watchly:catalog:{token}:movie:watchly.rec",
        f"watchly:manifest:1.17.2:{token}",
        f"watchly:rowmap:v1:{token}:movie",
        f"watchly:library_buckets:v1:{token}:series",
    ]
    assert all(store.token_from_key(key) == token for key in keys)
    assert store.token_from_key("watchly:identity:trakt:alice") is None
    assert store.token_from_key("watchly:token_alias:old") is None


def test_identity_and_alias_keys_use_control_plane():
    store = MongoStoreService(manager=SimpleNamespace(), settings_obj=SimpleNamespace(REDIS_TOKEN_KEY="watchly:token:"))
    assert store._kind("watchly:identity:trakt:alice") == ("identity", None)
    assert store._kind("watchly:token_alias:old") == ("alias", None)


class FakeResult:
    def __init__(self, matched_count=0, modified_count=0, deleted_count=0):
        self.matched_count = matched_count
        self.modified_count = modified_count
        self.deleted_count = deleted_count


class FakeCollection:
    def __init__(self):
        self.documents = {}

    async def replace_one(self, query, document, upsert=False):
        existed = query["_id"] in self.documents
        self.documents[query["_id"]] = dict(document)
        return FakeResult(matched_count=int(existed), modified_count=1)

    async def find_one(self, query):
        return self.documents.get(query["_id"])

    async def delete_one(self, query):
        existed = query["_id"] in self.documents
        self.documents.pop(query["_id"], None)
        return FakeResult(deleted_count=int(existed))

    async def insert_one(self, document):
        if document["_id"] in self.documents:
            raise type("DuplicateKeyError", (Exception,), {})()
        self.documents[document["_id"]] = dict(document)

    async def update_one(self, query, update):
        document = self.documents.get(query["_id"])
        if document is None:
            return FakeResult()
        expiry = query.get("expires_at", {}).get("$lte")
        if expiry is not None and not (document.get("expires_at") and document["expires_at"] <= expiry):
            return FakeResult()
        document.update(update.get("$set", {}))
        return FakeResult(matched_count=1, modified_count=1)


class FakeDatabase:
    def __init__(self):
        self.collections = {}

    def __getitem__(self, name):
        return self.collections.setdefault(name, FakeCollection())


class FakeManager:
    def __init__(self):
        self.control_db = FakeDatabase()
        self.data_db = FakeDatabase()
        self.assignments = {}

    async def database_for_user(self, token, create=False, for_write=False):
        if token not in self.assignments and create:
            self.assignments[token] = "shard-1"
        return self.data_db if token in self.assignments else None


def test_mongo_store_routes_user_records_and_cache_entries():
    import asyncio

    manager = FakeManager()
    store = MongoStoreService(manager=manager, settings_obj=SimpleNamespace(REDIS_TOKEN_KEY="watchly:token:"))

    async def scenario():
        token = "user-token"
        await store.set(f"watchly:token:{token}", '{"authKey":"encrypted"}')
        await store.set(f"watchly:profile:{token}:movie", '{"score":1}', ttl=60)
        user = await manager.data_db["users"].find_one({"_id": token})
        cache = await manager.data_db["cache_entries"].find_one({"_id": f"watchly:profile:{token}:movie"})
        assert user["value"] == '{"authKey":"encrypted"}'
        assert cache["owner_token"] == token
        assert await store.get(f"watchly:token:{token}") == '{"authKey":"encrypted"}'
        assert await store.get(f"watchly:profile:{token}:movie") == '{"score":1}'

    asyncio.run(scenario())


def test_mongo_store_keeps_identity_and_alias_records_in_control_database():
    import asyncio

    manager = FakeManager()
    store = MongoStoreService(manager=manager, settings_obj=SimpleNamespace(REDIS_TOKEN_KEY="watchly:token:"))

    async def scenario():
        await store.set("watchly:identity:trakt:alice", "user-token")
        await store.set("watchly:token_alias:old-token", "user-token")
        assert await store.get("watchly:identity:trakt:alice") == "user-token"
        assert await store.get("watchly:token_alias:old-token") == "user-token"
        assert manager.assignments == {}

    asyncio.run(scenario())


def test_mongo_store_set_nx_only_acquires_a_live_lock_once():
    import asyncio

    manager = FakeManager()
    store = MongoStoreService(manager=manager, settings_obj=SimpleNamespace(REDIS_TOKEN_KEY="watchly:token:"))

    async def scenario():
        key = "watchly:nuvio_refresh:user-token"
        assert await store.set_nx(key, "1", 30)
        assert not await store.set_nx(key, "1", 30)

    asyncio.run(scenario())
