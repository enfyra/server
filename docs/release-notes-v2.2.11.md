# Enfyra Server v2.2.11

## Features

- Added `$transaction.run()` to dynamic script contexts so repository operations can execute atomically on SQL databases and within MongoDB transaction scopes.
- Added structured `SCHEMA_INDEX_OVER_UNIQUE_FIELD` conflict details with the overlapping index and unique constraints.
- Added projected table `definition` metadata, including virtual timestamps and relations, for metadata consumers.

## Operational Flow

- Before: Dynamic scripts had no context-level transaction boundary for coordinated repository writes.
- After: Scripts can wrap repository work in `$transaction.run(async () => { ... })`; nested calls reuse the active boundary and abort signals cancel the running operation.

- Before: `GET /metadata` performed a full runtime metadata projection, while unavailable metadata could surface as a not-found response.
- After: `GET /metadata` returns instance information (`dbType` and `enfyraVersion`), and `GET /metadata/:name` returns a 503 while runtime metadata is unavailable.

## Bug Fixes

- Fixed schema migration validation to report every unique constraint that overlaps a requested index.
- Fixed metadata access projection to return a serializable root-admin table response with generated field definitions.
- Improved Redis admin and runtime-monitor overview reporting and reduced redundant SQL pool coordinator state.
- Updated `package.json` version to `2.2.11`.
