# MongoDB sharding and storage

Watchly uses MongoDB as its durable storage layer. User records and user-scoped cache entries are placed on a configured data shard; a control database keeps the user-to-shard mapping, shard registry, provider identity index, token aliases, allocation locks, and migration jobs.

## Configuration

Use semicolons to separate MongoDB URLs. MongoDB replica-set URIs commonly contain commas, so commas are not a safe list separator.

```env
MONGODB_URLS=mongodb+srv://user:password@cluster-a.example.net/?retryWrites=true&w=majority
MONGODB_DATABASE_NAME=watchly
MONGODB_CONTROL_DATABASE_NAME=watchly_control
MONGODB_MAX_STORAGE_BYTES=536870912
MONGODB_ALLOCATION_THRESHOLD=0.80
MONGODB_NEW_USER_RESERVE_BYTES=1048576
```

Optional settings:

- `MONGODB_URL`: single-shard compatibility alias.
- `MONGODB_CONTROL_URL`: dedicated control-plane cluster; defaults to the first configured shard.
- `MONGODB_SHARD_IDS`: stable semicolon-separated IDs, one per configured URL.
- `MONGODB_MAX_POOL_SIZE`, `MONGODB_SERVER_SELECTION_TIMEOUT_MS`, `MONGODB_CONNECT_TIMEOUT_MS`: connection tuning.

URLs are assigned in list order. Existing user mappings are stable; new accounts fill the first shard below the configured threshold, then spill to the next shard. If no shard is eligible, account creation fails closed rather than silently writing beyond the configured quota.

## Data layout

Each data shard contains:

- `users`: encrypted credential/settings payloads keyed by opaque Watchly token.
- `cache_entries`: user-owned cache values, including profiles, libraries, catalog rows, and refresh locks.

The control database contains:

- `user_shard_map`: authoritative account-to-shard assignment.
- `shard_registry`: configured shard IDs and latest measured usage.
- `identity_index` and `token_aliases`: provider identity and merged-token lookup.
- `allocation_locks`: cross-dyno allocation lock.
- `migration_jobs`: migration audit and restart recovery records.
- `global_cache`: cache keys that are not user-scoped.

The current capacity measurement uses MongoDB `dbStats` dataSize + indexSize for Watchly's configured data database. It is an application-level guard, not an Atlas billing/quota API. Keep one Watchly data database per Atlas cluster if relying on this estimate, set the configured capacity conservatively, and monitor Atlas metrics as well.

## Existing-user migration

New-user allocation is automatic. Existing users stay on their assigned shard unless an administrator explicitly invokes `mongo_store.migrate_user(token, target_shard_id)`.

Migration freezes writes for the account, copies its user record and cache entries, verifies the destination copy, switches the central mapping, and then removes the source copy. Reads during migration continue to resolve to the source. If a dyno restarts mid-migration, startup recovery rolls back an unfinished copy or completes source cleanup when the mapping had already switched.

Automatic migrations are intentionally disabled. Before enabling them, add operator authentication, migration telemetry, rate limits, and load testing with realistic user/cache sizes.

## Redis transition

The runtime no longer requires a Redis server. Existing Redis deployments are not automatically imported by this change. Back up the old Redis database, preserve the exact existing TOKEN_SALT, and put the old instance into maintenance/read-only mode so records stop changing during the import. Run the one-time importer, compare source and destination key counts, then switch production traffic. Do not point the new app at an empty MongoDB database and assume existing Redis accounts will appear automatically.


## Live integration test

The default CI run skips live MongoDB tests. To enable the two-cluster integration test, add a GitHub Actions repository secret named `MONGODB_TEST_URLS` containing two disposable MongoDB URIs separated by a semicolon. The test creates uniquely named temporary data/control databases, exercises account allocation and migration, and drops those databases afterward.

Optionally set `MONGODB_TEST_CONTROL_URL` to a separate disposable control-plane URI. Allow GitHub Actions runner access in the test clusters' network access lists. Never use production cluster credentials for this test.
