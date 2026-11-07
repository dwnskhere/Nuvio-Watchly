"""Copy legacy Watchly Redis keys into the MongoDB storage layer.

Install the optional migration dependency first:
    uv sync --extra redis-migration

Set OLD_REDIS_URL and the normal MONGODB_* environment variables. Run this
before switching production traffic. The script is idempotent for identical
values and refuses to overwrite a different MongoDB value.
"""

from __future__ import annotations

import asyncio
import os
import sys
from math import ceil

import redis.asyncio as redis

from app.services.mongo_store import mongo_store


async def main() -> None:
    source_url = os.getenv("OLD_REDIS_URL")
    if not source_url:
        raise SystemExit("OLD_REDIS_URL must point to the existing Redis instance.")

    await mongo_store.initialize()
    source = redis.from_url(source_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=10)
    copied = skipped = 0
    try:
        await source.ping()
        async for key in source.scan_iter(match="watchly:*", count=500):
            value = await source.get(key)
            if value is None:
                continue
            existing = await mongo_store.get(key)
            if existing is not None:
                if existing != value:
                    raise RuntimeError(
                        f"Refusing to overwrite a different MongoDB value for {key!r}. "
                        "Resolve the conflict and rerun."
                    )
                skipped += 1
                continue
            pttl = await source.pttl(key)
            if pttl == -2 or pttl == 0:
                continue
            ttl = ceil(pttl / 1000) if pttl > 0 else None
            await mongo_store.set(key, value, ttl=ttl)
            copied += 1
            if (copied + skipped) % 100 == 0:
                print(f"Processed {copied + skipped} keys (copied={copied}, already-present={skipped})")
        print(f"Migration complete: copied={copied}, already-present={skipped}")
    finally:
        await source.aclose()
        await mongo_store.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"Migration stopped: {type(exc).__name__}: {exc}", file=sys.stderr)
        raise
