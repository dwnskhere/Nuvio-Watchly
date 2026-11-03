"""One-time migration utility placeholder.

Do not run against production until the source Redis key inventory and target
MongoDB shard mapping have been reviewed. The application intentionally does not
auto-import legacy Redis data during startup.
"""

raise SystemExit(
    "Redis import is not enabled in this release. Back up the source Redis instance "
    "and use the migration procedure documented in docs/MONGODB_SHARDING.md."
)
