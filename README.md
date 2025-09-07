# Enfyra Backend - API-First Dynamic Platform

## License

This project is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)** - a strong copyleft license that ensures all modifications and derivatives remain open source.

For details, see [LICENSE](./LICENSE).

## Overview

Enfyra Backend is an API-first platform that enables dynamic creation and management of API endpoints, database schemas, and business logic through configuration. The system is built on NestJS with TypeScript and supports both MySQL and PostgreSQL databases.

## Quick Start

```bash
# Install dependencies
npm install

# Setup environment
cp env_example .env
# Edit .env with your database credentials

# Start the server
npm run start
```

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                      │
└─────────────────────┬───────────────────────────────────────┘
                      │ HTTP/GraphQL
┌─────────────────────▼───────────────────────────────────────┐
│                    API Gateway Layer                        │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   REST API      │  │   GraphQL API   │  │   WebSocket │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                    Middleware Layer                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Route Detection │  │ Parse Query     │  │ Auth/Guard   │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                    Dynamic Layer                            │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Dynamic Service │  │ Dynamic Repo    │  │ Query Engine │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                    Handler Execution Layer                  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │ Handler Executor│  │ Executor Pool   │  │ Child Process│ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────┬───────────────────────────────────────┘
                      │
┌─────────────────────▼───────────────────────────────────────┐
│                    Data Layer                               │
│  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   TypeORM       │  │   MySQL         │  │   Redis      │ │
│  └─────────────────┘  └─────────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Documentation

### 📚 Core Documentation

- **[Architecture](./docs/ARCHITECTURE.md)** - Detailed layer-by-layer architecture
- **[API Reference](./docs/API.md)** - REST and GraphQL API documentation
- **[Error Handling](./docs/ERROR_HANDLING.md)** - Error handling architecture and custom exceptions
- **[Authentication](./docs/AUTH.md)** - JWT authentication and authorization
- **[Database](./docs/DATABASE.md)** - MySQL and PostgreSQL support and configuration

### 👥 User Guides

- **[User Guide](./docs/USER_GUIDE.md)** - End-user guide for using the system (no coding required)
- **[Admin Guide](./docs/ADMIN_GUIDE.md)** - System administration and deployment guide

### 🔧 Development

- **[Development Guide](./docs/DEVELOPMENT.md)** - Setup, testing, and development workflow

## Key Features

- ✅ **Dynamic Schema** → Auto-generate CRUD & GraphQL APIs
- ✅ **Custom Logic** → Override with JavaScript/TypeScript handlers
- ✅ **Dynamic REST + GraphQL** → Full API coverage
- ✅ **Multi-instance Sync** → Auto-sync between instances
- ✅ **Permission Control** → Per-route and per-query permissions
- ✅ **Snapshot & Restore** → Backup and restore schemas

## Technology Stack

- **Framework**: NestJS with TypeScript
- **Database**: MySQL 8.0+ / PostgreSQL 12+ with TypeORM
- **Cache**: Redis
- **Authentication**: JWT
- **API**: REST + GraphQL
- **Process Management**: PM2

## Environment Variables

```bash
# Database Configuration
# Choose one: MySQL or PostgreSQL
DB_TYPE=mysql                    # mysql | mariadb | postgres
DB_HOST=localhost
DB_PORT=3306                     # 3306 for MySQL, 5432 for PostgreSQL
DB_USERNAME=root
DB_PASSWORD=1234
DB_NAME=enfyra

# Redis
REDIS_URI=redis://localhost:6379
DEFAULT_TTL=5

# RabbitMQ (optional)
RABBITMQ_USERNAME=root
RABBITMQ_PASSWORD=1234

# Application Settings
NODE_NAME=my_enfyra
PORT=1105

# Authentication
SECRET_KEY=my_secret
SALT_ROUNDS=10
ACCESS_TOKEN_EXP=15m
REFRESH_TOKEN_NO_REMEMBER_EXP=1d
REFRESH_TOKEN_REMEMBER_EXP=7d
```

## Quick API Examples

### Create a Table

**REST API:**

```http
POST /table_definition
Content-Type: application/json

{
  "name": "posts",
  "columns": [
    {"name": "id", "type": "int", "isPrimary": true, "isAutoIncrement": true},
    {"name": "title", "type": "varchar", "length": 255},
    {"name": "content", "type": "text"}
  ]
}
```

### Query with Filters

**REST API:**

```http
GET /posts?filter[title][_contains]=hello&sort=-createdAt&page=1&limit=10
```

### GraphQL Query

```graphql
query {
  posts {
    data {
      id
      title
      content
      createdAt
      updatedAt
    }
  }
}
```

**Note**: GraphQL schema is automatically generated and reloaded when tables are created or modified through the `table_definition` API.

## Contributing

Please read [DEVELOPMENT.md](./docs/DEVELOPMENT.md) for details on development workflow and contribution guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

_Documentation last updated: August 5, 2025_
