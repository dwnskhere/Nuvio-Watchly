"""Optional live integration test for two disposable MongoDB clusters.

Set MONGODB_TEST_URLS to two semicolon-separated disposable MongoDB URIs to
exercise real allocation, persistence, and migration. The test creates uniquely
named temporary databases and drops them in a finally block.
"""

import asyncio
import os
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.services.database.mongo_manager import MongoDBManager
from app.services.mongo_store import MongoStoreService


def test_two_cluster_allocation_and_user_migration():
    urls = [part.strip() for part in os.getenv("MONGODB_TEST_URLS", "").split(";") if part.strip()]
    if len(urls) < 2:
        pytest.skip("Set MONGODB_TEST_URLS to two disposable MongoDB URIs to run live integration tests.")

    async def scenario():
        suffix = uuid4().hex
        data_name = f"watchly_it_{suffix}"
        control_name = f"watchly_control_it_{suffix}"
        config = SimpleNamespace(
            MONGODB_URLS=";".join(urls[:2]),
            MONGODB_URL=None,
            MONGODB_CONTROL_URL=os.getenv("MONGODB_TEST_CONTROL_URL") or None,
            MONGODB_SHARD_IDS="integration-a;integration-b",
            MONGODB_DATABASE_NAME=data_name,
            MONGODB_CONTROL_DATABASE_NAME=control_name,
            MONGODB_MAX_STORAGE_BYTES=10**12,
            MONGODB_ALLOCATION_THRESHOLD=0.80,
            MONGODB_NEW_USER_RESERVE_BYTES=1,
            MONGODB_MAX_POOL_SIZE=5,
            MONGODB_SERVER_SELECTION_TIMEOUT_MS=10000,
            MONGODB_CONNECT_TIMEOUT_MS=10000,
        )
        manager = MongoDBManager(settings_obj=config)
        store = MongoStoreService(manager=manager, settings_obj=config)
        try:
            await manager.initialize()
            token = f"integration-{suffix}"
            await store.set(f"watchly:token:{token}", '{"credential":"encrypted-test-value"}')
            await store.set(f"watchly:profile:{token}:movie", '{"profile":"test"}', ttl=300)

            source = await manager.get_user_shard(token)
            assert source == "integration-a"

            result = await manager.migrate_user(token, "integration-b")
            assert result["status"] == "completed"
            assert await manager.get_user_shard(token) == "integration-b"
            assert await store.get(f"watchly:token:{token}") == '{"credential":"encrypted-test-value"}'
            assert await store.get(f"watchly:profile:{token}:movie") == '{"profile":"test"}'
            assert await manager.shards["integration-a"]["db"]["users"].find_one({"_id": token}) is None
        finally:
            for client in {id(client): client for client in manager.clients.values()}.values():
                try:
                    await client.drop_database(data_name)
                    await client.drop_database(control_name)
                except Exception:
                    pass
            await manager.close()

    asyncio.run(scenario())
