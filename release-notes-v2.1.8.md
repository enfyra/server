# Enfyra Server v2.1.8

## Features

- Added `@enfyra/kernel@1.0.11` as the published query and executor runtime dependency in `package.json`, replacing the embedded `src/kernel` source tree so server builds consume the same packaged kernel used in production.
- Added SQL physical schema contract utilities in `src/engines/knex/utils/sql-physical-schema-contract.ts` so table creation, schema diffs, indexes, foreign keys, and junction tables use one physical naming and relation contract.
- Added Mongo physical schema contract and migration services in `src/engines/mongo/utils/mongo-physical-schema-contract.ts` and `src/engines/mongo/services/mongo-physical-migration.service.ts` so Mongo schema updates can be planned and recovered consistently.
- Added Mongo saga snapshot support in `src/engines/mongo/services/mongo-saga-snapshot.service.ts` so failed Mongo schema migrations can restore metadata and data state more reliably.
- Added Redis runtime cache and runtime monitor improvements in `src/modules/admin/services/runtime-monitor.service.ts` and `src/modules/admin/services/redis-admin.service.ts` so operators can inspect cluster health, Redis memory, queues, and cache state with clearer diagnostics.
- Added `.internal/suites/schema-flow-matrix.js` to exercise add, update, delete, inverse, owning, and mixed schema mutation flows through the public API.

## Bug Fixes

- Fixed `src/engines/knex/utils/migration/relation-changes.ts` so deleting or updating an inverse many-to-many relation does not drop the owning side junction table.
- Fixed `src/modules/table-management/services/sql-table-metadata-writer.service.ts` to preserve `mappedById` and physical junction metadata when inverse relations are renamed or updated without remapping.
- Fixed `src/engines/knex/entity-manager.ts` so PostgreSQL dynamic inserts return generated IDs for user tables with multiple underscores in their names instead of mistaking them for junction tables.
- Fixed `src/modules/table-management/utils/relation-target-id.util.ts` and SQL table metadata services to normalize numeric relation target table IDs received as strings before resolving target metadata.
- Fixed `src/modules/dynamic-api/services/dynamic.service.ts` so script execution HTTP errors keep the original message separate from structured `details`, preventing responses and logs from collapsing to `{"scriptId":"(batch execution)"}`.
- Fixed `src/domain/exceptions/filters/global-exception.filter.ts` to log script errors with `errorCode` and `details` so batch execution failures are traceable through request logs.
- Fixed script execution diagnostics through `@enfyra/kernel@1.0.11` so handler errors include phase, line, column, and a focused code frame in structured error details.
- Fixed SQL relation DDL generation to preserve configured `onDelete`, physical FK names, referenced columns, and junction column metadata across create and update flows.
- Fixed Mongo relation handling and physical migration recovery paths in `src/engines/mongo/services/mongo-relation-manager.service.ts`, `mongo-schema-migration.service.ts`, and `mongo.service.ts` so relation writes and rollback behavior stay aligned with metadata.
- Updated `package.json` version to `2.1.8`.
