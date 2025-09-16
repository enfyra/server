# Contributing to Enfyra

## Quick Start

```bash
# Clone
git clone https://github.com/[your-username]/enfyra-be.git
cd enfyra-be

# Install
npm install

# Configure
cp env_example .env
# Edit .env with your DB credentials

# Run
npm run start:dev
```

## Required Setup

- Node.js 18+
- MySQL/PostgreSQL + Redis
- Port 1105 (default)

## Project Structure

```
src/
├── core/               # Auth, DB, Exceptions
├── infrastructure/     # Query Engine, Redis, Handler Executor
├── modules/           # Dynamic API, GraphQL, Schema Management
└── shared/            # Guards, Decorators, Utils
```

## Key APIs

### Core Endpoints
- `POST /auth/login` - JWT authentication
- `POST /auth/refresh-token` - Token renewal
- `GET /me` - Current user

### Dynamic System
- `POST /table_definition` - Create tables → Auto-generate APIs
- `POST /route_definition` - Define custom routes with handlers
- `POST /package_definition` - Install npm packages at runtime
- `*` - All undefined routes handled by Dynamic API

### GraphQL
- `/graphql` - Auto-generated schema from tables

## Development Commands

```bash
npm run start:dev       # Development with hot-reload
npm run build          # Production build
npm run format         # Prettier
npm run lint           # ESLint
```

## Database

### Schema Management
1. Tables created via API → TypeORM entities auto-generated
2. Located in `src/core/database/entities/`
3. GraphQL schema auto-reloads

### Migrations
```bash
npm run migration:generate -- -n Name
npm run migration:run
```

## Environment Variables

```bash
# Database
DB_TYPE=mysql          # mysql|postgres|mariadb
DB_HOST=localhost
DB_PORT=3306
DB_USERNAME=root
DB_PASSWORD=1234
DB_NAME=enfyra

# Redis
REDIS_URI=redis://localhost:6379

# App
PORT=1105
NODE_NAME=my_enfyra
SECRET_KEY=your_secret
```

## How It Works

### Dynamic API Flow
1. Request hits undefined route
2. System checks `route_definition` table
3. Executes custom handler or default CRUD
4. Handler runs in sandboxed child process

### Handler Context
```javascript
// Available in custom handlers
$ctx = {
  $body,          // Request body
  $params,        // Route params
  $query,         // Query params
  $user,          // Current user
  $repos: {       // TypeORM repositories
    main,         // Primary table
    [table]       // Any table
  },
  $helpers: {     // Utilities
    $jwt,
    $bcrypt,
    autoSlug
  }
}
```

### Query Engine
```javascript
// Advanced filtering
GET /posts?filter[title][_like]=%typescript%&filter[views][_gt]=100&sort=-createdAt&page=1&limit=10
```

## Git Workflow

1. Fork → Branch → Commit → PR
2. Use conventional commits: `feat:`, `fix:`, `docs:`
3. Run `npm run lint` before committing

## Security Features

- JWT with refresh tokens
- Role-based permissions per route
- SQL injection protection
- Sandboxed handler execution
- System table protection

## Performance

- Redis caching with SWR pattern
- Connection pooling
- Query optimization
- Hot-reload in development

## License

Elastic License 2.0 - Contributions will be under same license.