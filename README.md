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

Enfyra supports schema and data migrations through JSON configuration files.

### Schema Migration (`data/snapshot.json`)

Define your database schema (tables, columns, relations):

```json
{
  "my_table": {
    "name": "my_table",
    "description": "My custom table",
    "columns": [
      { "name": "id", "type": "int", "isPrimary": true, "isGenerated": true },
      { "name": "name", "type": "varchar", "isNullable": false }
    ],
    "relations": []
  }
}
```

Run schema migration:
```bash
npx ts-node scripts/init-db.ts
```

### Schema Migration (`data/snapshot-migration.json`)

For dangerous operations (remove, modify/rename). Adding is handled automatically by `snapshot.json`.

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
1. Physical DB changes (init-db script or app bootstrap)
2. Metadata updates (provision service)
3. Both read from same `snapshot-migration.json` → consistent

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
| Add new table | `snapshot.json` |
| Add new column | `snapshot.json` |
| Add new relation | `snapshot.json` |
| Rename column | `snapshot-migration.json` |
| Remove column | `snapshot-migration.json` |
| Rename relation | `snapshot-migration.json` |
| Remove relation | `snapshot-migration.json` |
| Drop table | `snapshot-migration.json` |

#### How It Works

```
┌─────────────────────────────────────┐
│     snapshot-migration.json         │
│         (single source)             │
└──────────────┬──────────────────────┘
               │
       ┌───────┴───────┐
       ▼               ▼
┌──────────────┐ ┌──────────────────┐
│  init-db.ts  │ │  provision service│
│              │ │  (app bootstrap)  │
│ Physical DB  │ │   Metadata DB     │
│  (tables,    │ │  (table_def,      │
│   columns,   │ │   column_def,     │
│   FKs)       │ │   relation_def)   │
└──────────────┘ └──────────────────┘
       │               │
       └───────┬───────┘
               ▼
         ✅ Consistent
```

Both physical DB and metadata are updated from the same source, ensuring consistency.

### Destructive schema changes via API (confirm-hash)

When changing schema through the API (e.g. updating or deleting a row in `table_definition`), destructive changes are protected by a confirm-hash challenge.

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
curl -X PATCH "http://localhost:1105/api/table_definition/<id>" \
  -H "Content-Type: application/json" \
  -d '{"columns":[{"name":"id","type":"int"}]}' 

# 2) Server responds 422 with details.requiredConfirmHash + details.confirmToken
# 3) Retry with headers
curl -X PATCH "http://localhost:1105/api/table_definition/<id>" \
  -H "Content-Type: application/json" \
  -H "x-schema-confirm-hash: <requiredConfirmHash>" \
  -H "x-schema-confirm-token: <confirmToken>" \
  -d '{"columns":[{"name":"id","type":"int"}]}'
```

For a limited transition period, the legacy `schemaConfirm` phrase may also be accepted, but the UI uses confirm-hash by default.

### Data Migration (`data/data-migration.json`)

Migrate existing data when the system is already initialized:

```json
{
  "_deletedTables": ["deprecated_table"],
  "role_definition": [
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

Use `_deletedRecords` when you need to remove a small set of rows (e.g. remove an old seeded `route_definition`) without wiping the entire table.

```json
{
  "_deletedRecords": [
    { "table": "route_definition", "filter": { "path": { "_eq": "/old-route" } } },
    { "table": "menu_definition", "filter": { "path": { "_eq": "/old-menu" } } }
  ]
}
```

### Migration Flow

1. **First Init** (`isInit = false`):
   - Schema is created from `snapshot.json`
   - Default data is inserted from `default-data.json`
   - System sets `isInit = true`

2. **Subsequent Starts** (`isInit = true`):
   - Schema is synced from `snapshot.json` (auto-add new columns/relations)
   - Schema migrations run from `snapshot-migration.json` (remove/modify)
   - Data migrations run from `data-migration.json`

### Supported Databases

- **SQL**: MySQL, PostgreSQL, MariaDB
- **NoSQL**: MongoDB
