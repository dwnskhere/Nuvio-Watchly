from types import SimpleNamespace

from app.services.database.mongo_manager import MongoDBManager
from app.services.mongo_store import MongoStoreService


def test_shard_id_is_stable_when_credentials_rotate():
    first = "mongodb+srv://user:old@cluster-a.example.net/watchly?retryWrites=true"
    rotated = "mongodb+srv://user:new@cluster-a.example.net/watchly?retryWrites=true"
    other = "mongodb+srv://user:pass@cluster-b.example.net/watchly?retryWrites=true"
    assert MongoDBManager.shard_id_for_url(first) == MongoDBManager.shard_id_for_url(rotated)
    assert MongoDBManager.shard_id_for_url(first) != MongoDBManager.shard_id_for_url(other)


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
        f"watchly:library_items:{token}", f"watchly:nuvio_refresh:{token}",
        f"watchly:profile:{token}:movie", f"watchly:watched_sets:{token}:series",
        f"watchly:catalog:{token}:movie:watchly.rec", f"watchly:manifest:1.17.2:{token}",
        f"watchly:rowmap:v1:{token}:movie", f"watchly:library_buckets:v1:{token}:series",
    ]
    assert all(store.token_from_key(key) == token for key in keys)
    assert store.token_from_key("watchly:identity:trakt:alice") is None
    assert store.token_from_key("watchly:token_alias:old") is None


def test_identity_and_alias_keys_use_control_plane():
    store = MongoStoreService(manager=SimpleNamespace(), settings_obj=SimpleNamespace(REDIS_TOKEN_KEY="watchly:token:"))
    assert store._kind("watchly:identity:trakt:alice") == ("identity", None)
    assert store._kind("watchly:token_alias:old") == ("alias", None)
