# Enfyra Backend - Claude AI Development Guide

## Project Overview

Enfyra is an open-source backend platform that automatically generates REST and GraphQL APIs from database schemas. Users create tables through a visual interface, and Enfyra instantly provides APIs for them - no coding required.

**Technology Stack**: Node.js, NestJS, TypeScript, MySQL/PostgreSQL/MongoDB, Redis, GraphQL, JWT Auth

## Quick Start

### Prerequisites
- Node.js 18+
- MySQL/PostgreSQL + Redis (required)
- MongoDB (optional, for specific use cases)
- Port 1105 (default)

### Setup Commands
```bash
# Clone and install
git clone <repository>
cd enfyra/server
yarn install  # ALWAYS use yarn (never npm)

# Configure environment
cp env_example .env
# Edit .env with your database credentials

# Initialize database
yarn build

# Development
yarn start:dev  # Hot reload development
```

### Environment Configuration
Key environment variables (see `env_example` for complete list):
- `DB_TYPE`: mysql | postgres | mariadb | mongodb
- `DB_HOST`, `DB_PORT`, `DB_USERNAME`, `DB_PASSWORD`, `DB_NAME`
- `REDIS_URI`: Redis connection string
- `PORT`: 1105 (default)
- `SECRET_KEY`: JWT secret
- `NODE_ENV`: development | production | test

## Architecture Overview

### Directory Structure
```
src/
├── core/                    # Core system functionality
│   ├── auth/               # JWT authentication, guards, strategies
│   ├── bootstrap/          # System initialization and processors
│   └── exceptions/         # Global exception handling
├── infrastructure/         # Low-level services and utilities
│   ├── cache/              # Redis caching services
│   ├── knex/               # Database connection and schema management
│   ├── mongo/              # MongoDB integration
│   ├── query-builder/      # Dynamic query construction
│   ├── query-engine/       # Advanced filtering and querying
│   ├── swagger/            # API documentation generation
│   └── middleware/         # Custom middleware
├── modules/                # Business logic modules
│   ├── admin/              # Admin functionality
│   ├── ai-agent/           # AI agent services
│   ├── dynamic-api/        # Auto-generated API handling
│   ├── file-management/    # File upload and storage
│   ├── graphql/            # GraphQL endpoint and resolvers
│   ├── me/                 # User profile management
│   ├── package-management/ # NPM package runtime installation
│   └── table-management/   # Dynamic table creation/management
└── shared/                 # Common utilities and decorators
    ├── common/             # Shared services
    ├── guards/             # Route protection guards
    ├── interceptors/       # Request/response interceptors
    ├── middleware/         # Request middleware
    └── utils/              # Helper utilities
```

### Core Concepts

#### Dynamic API System
- **Route Detection**: `RouteDetectMiddleware` intercepts all undefined routes
- **Dynamic Context**: `TDynamicContext` provides request context to handlers
- **Handler Execution**: Custom handlers run in sandboxed child processes
- **Auto CRUD**: Automatic CRUD operations for defined tables

#### Database Schema Management
- Tables created via `table_definition` API
- Auto-generated TypeORM entities in runtime
- Schema stored in database, not code files
- Support for MySQL, PostgreSQL, MariaDB, and MongoDB

#### Query Engine
Advanced filtering capabilities:
```javascript
GET /posts?filter[title][_like]=%typescript%&filter[views][_gt]=100&sort=-createdAt&page=1&limit=10
```

#### Handler Context
Available in custom route handlers:
```javascript
$ctx = {
  $body,          // Request body
  $params,        // Route parameters
  $query,         // Query parameters
  $user,          // Current authenticated user
  $repos: {       // TypeORM repositories
    main,         // Primary table repository
    [tableName]   // Any table repository by name
  },
  $helpers: {     // Utility functions
    $jwt,          // JWT utilities
    $bcrypt,       // Password hashing
    autoSlug       // Auto-generate slugs
  }
}
```

## Development Workflow

### Build & Test Commands
```bash
yarn build              # Production build (copies data/ to dist/)
yarn start:dev          # Development with hot reload
yarn start:debug        # Debug mode with watch
yarn start:prod         # Production mode
yarn format             # Prettier code formatting
yarn lint               # ESLint with auto-fix
yarn test               # Run unit tests
yarn test:cov           # Run tests with coverage
yarn test:e2e           # End-to-end tests
```

### Database Migrations
```bash
yarn migration:generate -- -n MigrationName
yarn migration:run
yarn migration:revert
```

### Package Scripts Details
- **build**: `nest build && cp -r data dist/` - Builds and copies static data
- **postinstall**: Automatically installs sharp if not present
- **typeorm**: TypeORM CLI access for database operations

## Key Services & Modules

### Core Authentication
- `JwtAuthGuard`: Global JWT authentication
- `RoleGuard`: Role-based access control
- `JwtStrategy`: Passport JWT strategy implementation

### Dynamic API Processing
- `DynamicController`: Handles all undefined routes (`@All('*splat')`)
- `DynamicService`: Executes route handlers and manages responses
- `RouteDetectMiddleware`: Detects and configures dynamic routes
- `NotFoundDetectGuard`: Throws 404 for undefined routes

### AI Agent Module
- `AiAgentService`: Main AI conversation handler
- `LLMService`: Integration with multiple LLM providers (OpenAI, Anthropic, etc.)
- `ConversationService`: Manages conversation state
- Streaming responses with SSE (Server-Sent Events)

### Cache & Performance
- `RouteCacheService`: Redis-based response caching
- `MetadataCacheService`: Table schema caching
- SWR (Stale-While-Revalidate) caching pattern
- Connection pooling for databases

## Testing Structure

### Test Organization
```
test/
├── unit/           # Unit tests for individual services
├── integration/    # Integration tests between modules
├── builders/       # Entity builder tests
├── infrastructure/ # Infrastructure component tests
└── stress/         # Performance and load tests
```

### Test Configuration
- Jest as test runner
- Coverage collection from `src/**/*.(t|j)s`
- Excludes DTOs, interfaces, and spec files from coverage
- Node.js test environment

## Code Standards & Conventions

### ESLint & Prettier
- Prettier: Single quotes, trailing commas
- ESLint: TypeScript rules with relaxed strictness
- No explicit return types required
- `any` type allowed for flexibility

### Special Rules (from .cursorrules)
- **CRITICAL**: Never use `git revert`
- Always ask before making changes to existing code
- Use existing Knex helper services, never direct Knex
- Remove non-critical comments (Vietnamese comments in codebase)
- Use `yarn` exclusively (never npm)
- All services must be Global and exported from their modules
- Never run `start:dev` (owner handles this)
- Sleep 1 second after build to ensure compilation completion
- Do not create unnecessary markdown files

### TypeScript Configuration
- Target: ES2022
- Strict null checks disabled
- Decorators enabled for NestJS
- CommonJS modules
- Incremental compilation enabled

## Important Files & Configuration

### Key Configuration Files
- `.env`: Environment variables (copy from `env_example`)
- `tsconfig.json`: TypeScript compilation settings
- `nest-cli.json`: NestJS CLI configuration with asset copying
- `package.json`: Dependencies and scripts (see resolutions for package fixes)

### Critical Services
- `scripts/init-db.ts`: Database initialization script
- `src/main.ts`: Application bootstrap with performance logging
- `src/app.module.ts`: Main module with global providers and middleware

### Git Workflow
- Main branch: `main`
- Current branch: `release/v1.2.0-beta-3`
- Conventional commits: `feat:`, `fix:`, `refactor:`, `chore:`
- Elastic License 2.0

## Security Features

- JWT authentication with refresh tokens
- Role-based permissions per route
- SQL injection protection via query builders
- Sandboxed handler execution in child processes
- System table protection
- CORS configuration
- Request validation and transformation pipes

## Performance Considerations

- Redis caching with intelligent invalidation
- Database connection pooling
- Query optimization and indexing
- Cold start performance tracking
- Streaming responses for AI interactions
- Handler execution in isolated processes

## API Endpoints

### Core Authentication
- `POST /auth/login` - User authentication
- `POST /auth/refresh-token` - Token renewal
- `GET /me` - Current user profile

### Dynamic Table Management
- `POST /table_definition` - Create new tables
- `POST /route_definition` - Define custom routes with handlers
- `POST /package_definition` - Install NPM packages at runtime

### GraphQL
- `/graphql` - Auto-generated schema from database tables
- Yoga GraphQL server integration

### File Management
- File upload endpoints with storage provider support
- S3 and Google Cloud Storage integration

## Development Tips

1. **Always use yarn** - Never npm (per project rules)
2. **Check existing helpers** - Don't recreate utilities in `/infrastructure/helpers`
3. **Global services only** - Ensure all services are exported globally
4. **Test after changes** - Run `yarn test` and `yarn lint` before committing
5. **Monitor performance** - Check cold start logs for optimization opportunities
6. **Use caching wisely** - Leverage Redis services for frequently accessed data
7. **Security first** - Never expose system tables or sensitive operations
8. **Handler sandboxing** - Custom handlers run in isolated processes