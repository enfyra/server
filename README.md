# Enfyra Backend

[Enfyra](https://demo.enfyra.io/login) is the open-source backend platform.  
We’re building the flexibility backend framework that automatically generates APIs from your database. You create tables through a visual interface, and Enfyra instantly provides REST & GraphQL APIs for them - no coding required. It's like having a backend developer that never sleeps.

## Documentation

For full documentation, visit [docs](https://github.com/enfyra/documents)

To see how to contribute, visit [Contributing](https://github.com/enfyra/server/blob/main/CONTRIBUTING.md)

## Community & Support

- [Community Forum](https://github.com/orgs/enfyra/discussions)
- [GitHub Issues](https://github.com/enfyra/server/issues)
- [Discord](https://discord.gg/DH5sXtFVWM)

## How it works
**Architecture**

Enfyra is a self-hosted and locally developed, easy-to-install. Cloud coming soon.

- **Query Engine**: high-performance engine for filtering, joins, aggregates, and search directly through your API.
- **Realtime**: push updates to clients when rows change using websockets.
- **REST/GraphQL API**: automatically generated from your schema.
- **Auth Service**: JWT-based authentication API for sign-ups, logins, and session management.
- **Storage**: RESTful API for managing files and permissions.
- **Functions**: run server-side code close to your data.

## Database Migrations

Enfyra defines the current system schema in TypeScript and keeps manual schema
and data migrations in JSON.

### Schema Target (`src/data/snapshot.ts`)

Define your database schema (tables, columns, relations):

```ts
snapshot
  .table('my_table', { description: 'My custom table' })
  .columns({
    id: col.int().primary().generated().notNull(),
    name: col.varchar().notNull(),
  })
  .relations({});
```

`tsc` compiles this source to `dist/data/snapshot.js`. Run schema migration:

```bash
yarn tsx scripts/init-db.ts
```

### Schema Migration (`src/data/snapshot-migration.ts`)

`snapshot-migration.ts` records non-additive schema changes. New tables,
columns, and relations require only their target definitions in `snapshot.ts`.
Updating, renaming, or deleting an existing table, column, relation, or physical
schema contract must be declared manually in `snapshot-migration.ts`.

`snapshot.ts` is the complete current system target. Migration does not infer
non-additive changes from live database differences: live state is used only to
validate the manual declaration, detect conflicts, resume idempotently, and
attest the target. Undeclared user metadata is preserved.

Snapshot migrations are forward-only. Updates synchronize the complete declared metadata and physical contract across supported databases. Deletions remove all affected metadata and physical state, including fields, foreign keys, constraints, indexes, junction storage, and tables or collections. After a database completes a migration for a newer Enfyra version, running an older Enfyra version against that database is unsupported.

```json
{
  "tables": [
    {
      "_unique": { "name": { "_eq": "users" } },
      "columnsToModify": [
        { "from": { "name": "email" }, "to": { "name": "userEmail" } }
      ],
      "columnsToRemove": ["deprecated_field"],
      "relationsToModify": [
        { "from": { "propertyName": "oldRelation" }, "to": { "propertyName": "newRelation" } }
      ],
      "relationsToRemove": ["deprecated_relation"]
    }
  ],
  "tablesToDrop": ["old_table_name"]
}
```

**Operations:**

| Field | Description | Data Loss Risk |
|-------|-------------|----------------|
| `columnsToModify` | Rename or change column properties | Low (rename preserves data) |
| `columnsToRemove` | Remove columns | **HIGH** |
| `relationsToModify` | Rename or change relation properties | Low |
| `relationsToRemove` | Remove relations (drops FK column) | **HIGH** |
| `tablesToDrop` | Drop entire tables | **HIGH** |

**Flow:**
1. Bootstrap artifacts are loaded and validated into one immutable definition.
2. Live metadata is inspected and an execution plan is prepared.
3. The production coordinator applies physical and metadata operations.
4. Healing and final attestation use the same compiled definition.

#### Usage Examples

**1. Rename a column (preserves data)**

```json
{
  "tables": [{
    "_unique": { "name": { "_eq": "users" } },
    "columnsToModify": [
      { "from": { "name": "email" }, "to": { "name": "userEmail" } }
    ]
  }]
}
```

Result: Column `email` renamed to `userEmail`, data preserved.

**2. Change column properties**

```json
{
  "tables": [{
    "_unique": { "name": { "_eq": "users" } },
    "columnsToModify": [
      {
        "from": { "name": "status", "isNullable": true },
        "to": { "name": "status", "isNullable": false }
      }
    ]
  }]
}
```

Result: Column `status` becomes NOT NULL.

**3. Remove deprecated column (⚠️ data loss)**

```json
{
  "tables": [{
    "_unique": { "name": { "_eq": "users" } },
    "columnsToRemove": ["old_legacy_field"]
  }]
}
```

Result: Column `old_legacy_field` dropped, all data in this column lost.

**4. Rename a relation (preserves FK data)**

```json
{
  "tables": [{
    "_unique": { "name": { "_eq": "orders" } },
    "relationsToModify": [
      { "from": { "propertyName": "approvedBy" }, "to": { "propertyName": "approver" } }
    ]
  }]
}
```

Result: FK column `approvedById` renamed to `approverId`, data preserved.

**5. Remove a relation (⚠️ FK data loss)**

```json
{
  "tables": [{
    "_unique": { "name": { "_eq": "orders" } },
    "relationsToRemove": ["legacyRelation"]
  }]
}
```

Result: FK column dropped, all FK references lost.

**6. Drop entire table (⚠️ all data lost)**

```json
{
  "tablesToDrop": ["deprecated_table", "legacy_data"]
}
```

Result: Tables completely removed from database.

#### When to Use

| Scenario | File to Modify |
|----------|---------------|
| Add new table | `src/data/snapshot.ts` |
| Add new column | `src/data/snapshot.ts` |
| Add new relation | `src/data/snapshot.ts` |
| Rename column | `src/data/snapshot-migration.ts` |
| Remove column | `src/data/snapshot-migration.ts` |
| Rename relation | `src/data/snapshot-migration.ts` |
| Remove relation | `src/data/snapshot-migration.ts` |
| Drop table | `src/data/snapshot-migration.ts` |

#### How It Works

`scripts/init-db.ts`, `scripts/init-db-sql.ts`, and `scripts/init-db-mongo.ts`
delegate to the same production initialization coordinator. They do not run a
separate physical diff or schema-sync pipeline.

### Destructive schema changes via API (confirm-hash)

When changing schema through the API (e.g. updating or deleting a row in `enfyra_table`), destructive changes are protected by a confirm-hash challenge.

- The server returns **422** with `code = "SCHEMA_CONFIRM_REQUIRED"` and `details` including:
  - `requiredConfirmHash`
  - `confirmToken` (short-lived)
  - `confirmTtlMs`
  - `removedColumns`, `removedRelationsCount` (when applicable)
- To proceed, resend the same request with:
  - `x-schema-confirm-hash: <requiredConfirmHash>`
  - `x-schema-confirm-token: <confirmToken>`

Example flow:

```bash
# 1) Attempt destructive update
curl -X PATCH "http://localhost:1105/api/enfyra_table/<id>" \
  -H "Content-Type: application/json" \
  -d '{"columns":[{"name":"id","type":"int"}]}' 

# 2) Server responds 422 with details.requiredConfirmHash + details.confirmToken
# 3) Retry with headers
curl -X PATCH "http://localhost:1105/api/enfyra_table/<id>" \
  -H "Content-Type: application/json" \
  -H "x-schema-confirm-hash: <requiredConfirmHash>" \
  -H "x-schema-confirm-token: <confirmToken>" \
  -d '{"columns":[{"name":"id","type":"int"}]}'
```

For a limited transition period, the legacy `schemaConfirm` phrase may also be accepted, but the UI uses confirm-hash by default.

### Data Migration (`src/data/data-migration.ts`)

Migrate existing data when the system is already initialized:

```json
{
  "_deletedTables": ["deprecated_table"],
  "enfyra_role": [
    {
      "name": "Admin",
      "description": "Updated admin description",
      "_unique": { "name": { "_eq": "Admin" } }
    }
  ]
}
```

- `_deletedRecords`: Delete specific records by filter (safe way to remove seeded routes/menus/etc. across versions)
- `_deletedTables`: Array of table names to delete all data from
- Table entries: Data to migrate, using `_unique` to identify existing records

#### Delete specific records (`_deletedRecords`)

Use `_deletedRecords` when you need to remove a small set of rows (e.g. remove an old seeded `enfyra_route`) without wiping the entire table.

```json
{
  "_deletedRecords": [
    { "table": "enfyra_route", "filter": { "path": { "_eq": "/old-route" } } },
    { "table": "enfyra_menu", "filter": { "path": { "_eq": "/old-menu" } } }
  ]
}
```

### Migration Flow

1. **Bootstrap or Upgrade** (`enfyra_setting` is absent or `isInit = false`):
   - The compiled snapshot module, migration, default data, and data migration files are loaded once into an immutable bootstrap definition and validated before schema mutation.
   - Live metadata is checked against every manual non-additive declaration and converted into an execution plan.
   - Manual non-additive schema migrations run from `snapshot-migration.ts`.
   - Missing additive schema is provisioned from `snapshot.ts`.
   - Additive target repair, derived-contract repair, and explicit one-time repair run as separate phases while preserving undeclared user metadata.
   - Default data is provisioned from `default-data.ts`.
   - Intentional record updates/deletes run from `data-migration.ts` and their target values are attested.
   - Final schema and data attestation run after every write.
   - The system sets `isInit = true` only after the full bootstrap path completes.

2. **Normal Start** (`isInit = true`):
   - Snapshot sync, metadata/data migrations, default-data provision, and schema healing are skipped.
   - To apply bootstrap data changes to an existing database, the owning upgrade or provision workflow must set `isInit = false` before the server starts.

### Supported Databases

- **SQL**: MySQL, PostgreSQL, MariaDB
- **NoSQL**: MongoDB
