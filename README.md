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

### Deleting Tables (`data/snapshot-migration.json`)

To delete tables that are no longer needed:

```json
{
  "tables": [],
  "deletedTables": ["old_table_name", "deprecated_table"]
}
```

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

- `_deletedTables`: Array of table names to delete all data from
- Table entries: Data to migrate, using `_unique` to identify existing records

### Migration Flow

1. **First Init** (`isInit = false`):
   - Schema is created from `snapshot.json`
   - Default data is inserted from `default-data.json`
   - System sets `isInit = true`

2. **Subsequent Starts** (`isInit = true`):
   - Schema is synced from `snapshot.json`
   - Data migrations run from `data-migration.json`
   - Tables in `snapshot-migration.json` → `deletedTables` are dropped

### Supported Databases

- **SQL**: MySQL, PostgreSQL, MariaDB
- **NoSQL**: MongoDB
