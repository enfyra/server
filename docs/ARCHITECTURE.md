# Architecture Documentation

## Overview

Enfyra Backend follows a layered architecture pattern with clear separation of concerns. Each layer has specific responsibilities and communicates with adjacent layers through well-defined interfaces.

## Layer Architecture

### 1. API Gateway Layer

#### REST API (`src/dynamic/dynamic.controller.ts`)

- **Purpose**: Entry point for all dynamic REST API routes
- **Functionality**:
  - Receive HTTP requests from clients
  - Route to dynamic service based on URL patterns
  - Handle HTTP methods (GET, POST, PUT, DELETE, PATCH)
- **Key Features**:
  - Dynamic routing based on table definitions
  - Support for query parameters, filters, sorting, pagination
  - Automatic response formatting

#### GraphQL API (`src/graphql/dynamic.resolver.ts`)

- **Purpose**: Provide GraphQL interface for dynamic data
- **Functionality**:
  - Dynamic schema generation from table definitions
  - Query and mutation operations
  - Real-time subscriptions (if needed)
- **Key Features**:
  - Auto-generated types from database schema
  - Support for complex queries with relations
  - Type-safe operations

### 2. Middleware Layer

#### Route Detection (`src/middleware/route-detect.middleware.ts`)

- **Purpose**: Identify matched routes and prepare dynamic context
- **Functionality**:
  - Parse URLs to determine table names
  - Create `$ctx` object with request information
  - Inject context into request object
- **Key Variables**:
  ```typescript
  $ctx = {
    table: string,           // Table name
    action: string,          // CRUD action
    id?: string,            // ID for single record operations
    params: object,         // URL parameters
    query: object,          // Query parameters
    body: object,           // Request body
    user?: object,          // Authenticated user
    route: object           // Route definition
  }
  ```

#### Parse Query (`src/middleware/parse-query.middleware.ts`)

- **Purpose**: Parse and validate query parameters
- **Functionality**:
  - Convert string values to appropriate types
  - Parse JSON strings to objects
  - Validate query parameter formats
- **Supported Formats**:
  - `filter[field][operator]=value`
  - `sort[field]=direction`
  - `page=number&limit=number`
  - `fields=field1,field2`
  - `include=relation1,relation2`

#### Authentication & Authorization

- **JWT Auth Guard** (`src/guard/jwt-auth.guard.ts`): Verify JWT tokens
- **Role Guard** (`src/guard/role.guard.ts`): Check user permissions
- **Schema Lock Guard** (`src/guard/schema-lock.guard.ts`): Prevent schema changes during operations

### 3. Dynamic Layer

#### Dynamic Service (`src/dynamic/dynamic.service.ts`)

- **Purpose**: Orchestrator for handling dynamic requests
- **Functionality**:
  - Validate route definitions
  - Execute pre/post hooks
  - Route to appropriate handlers
  - Handle timeouts and errors
- **Key Methods**:
  ```typescript
  async executeHandler(ctx: DynamicContext): Promise<any>
  async executeHook(hook: HookDefinition, ctx: DynamicContext): Promise<void>
  private validateRoute(route: RouteDefinition): boolean
  ```

#### Dynamic Repository (`src/dynamic-repo/dynamic-repo.service.ts`)

- **Purpose**: CRUD operations for dynamic tables
- **Functionality**:
  - Create, Read, Update, Delete records
  - Handle relations and joins
  - Support for bulk operations
- **Key Methods**:
  ```typescript
  async create(tableName: string, data: any): Promise<any>
  async findOne(tableName: string, id: string): Promise<any>
  async findMany(tableName: string, options: QueryOptions): Promise<any>
  async update(tableName: string, id: string, data: any): Promise<any>
  async delete(tableName: string, id: string): Promise<any>
  ```

#### Query Engine (`src/query-engine/query-engine.service.ts`)

- **Purpose**: Handle complex queries with filters, sorting, pagination
- **Functionality**:
  - Build SQL queries from filter objects
  - Handle relations and joins
  - Support for aggregations
  - Optimize query performance
- **Supported Operators**:
  ```typescript
  _eq,
    _neq,
    _gt,
    _gte,
    _lt,
    _lte,
    _in,
    _not_in,
    _between,
    _not,
    _is_null,
    _contains,
    _starts_with,
    _ends_with;
  ```

### 4. Handler Execution Layer

#### Handler Executor (`src/handler-executor/handler-executor.service.ts`)

- **Purpose**: Execute user-defined JavaScript code in isolated environment
- **Functionality**:
  - Spawn child processes for code execution
  - Handle inter-process communication
  - Manage timeouts and resource cleanup
  - Provide sandboxed environment
- **Key Features**:
  - Isolated execution environment
  - Timeout management
  - Error handling and propagation
  - Resource cleanup

#### Executor Pool (`src/handler-executor/executor-pool.service.ts`)

- **Purpose**: Manage pool of child processes
- **Functionality**:
  - Reuse child processes for performance optimization
  - Handle process lifecycle
  - Load balancing
- **Pool Management**:
  ```typescript
  async acquire(): Promise<ChildProcess>
  async release(child: ChildProcess): void
  async destroy(): Promise<void>
  ```

#### Runner (`src/handler-executor/runner.ts`)

- **Purpose**: Child process script executor
- **Functionality**:
  - Execute user code in isolated context
  - Handle communication with parent process
  - Provide runtime environment
- **Context Injection**:
  ```typescript
  // Available in user scripts
  $ctx: DynamicContext
  $db: Database connection
  $req: HTTP request
  $res: HTTP response
  ```

### 5. Data Layer

#### Data Source (`src/data-source/data-source.service.ts`)

- **Purpose**: Manage database connections and schema
- **Functionality**:
  - Dynamic schema loading
  - Connection pooling
  - Migration management
  - Entity registration
- **Key Methods**:
  ```typescript
  async reloadDataSource(): Promise<void>
  async getRepository(tableName: string): Promise<Repository>
  async executeQuery(sql: string, params: any[]): Promise<any>
  ```

#### Table Service (`src/table/table.service.ts`)

- **Purpose**: CRUD operations for table definitions
- **Functionality**:
  - Create/drop tables dynamically
  - Manage columns and relations
  - Handle foreign key constraints
  - Schema validation
- **Key Methods**:
  ```typescript
  async create(definition: TableDefinition): Promise<any>
  async update(id: string, definition: TableDefinition): Promise<any>
  async delete(id: string): Promise<any>
  async getSchema(tableName: string): Promise<any>
  ```

## Data Flow

### Request Flow

1. **Client Request** → API Gateway Layer
2. **Route Detection** → Parse URL and create context
3. **Authentication** → Verify JWT and permissions
4. **Query Parsing** → Parse and validate parameters
5. **Dynamic Service** → Route to appropriate handler
6. **Handler Execution** → Execute custom logic or default CRUD
7. **Query Engine** → Build and execute database queries
8. **Response** → Format and return data

### Error Flow

1. **Exception Thrown** → Any layer can throw exceptions
2. **Global Exception Filter** → Catch and format errors
3. **Logging** → Structured logging with correlation ID
4. **Response** → Standardized error response

## Component Interactions

### Service Dependencies

```
DynamicController
    ↓
DynamicService
    ↓
HandlerExecutorService
    ↓
ExecutorPoolService
    ↓
ChildProcess (Runner)
```

### Data Flow Dependencies

```
QueryEngine
    ↓
DataSourceService
    ↓
TypeORM Repository
    ↓
MySQL/PostgreSQL Database
```

## Design Patterns

### 1. Dependency Injection

- All services use NestJS DI container
- Loose coupling between components
- Easy testing and mocking

### 2. Factory Pattern

- Dynamic entity creation
- Repository factory for different table types
- Handler factory for different script types

### 3. Strategy Pattern

- Different query strategies
- Different authentication strategies
- Different caching strategies

### 4. Observer Pattern

- Redis Pub/Sub for inter-service communication
- Event-driven architecture
- Schema change notifications
- Automatic schema synchronization via `syncAll`

## Scalability Considerations

### Horizontal Scaling

- Stateless services
- Redis for shared state
- Database connection pooling
- Load balancer ready

### Vertical Scaling

- Child process pooling
- Memory management
- Query optimization
- Caching strategies

## Security Architecture

### Authentication Flow

1. JWT token validation
2. User context injection
3. Role-based access control
4. Resource-level permissions

### Data Protection

- Input validation and sanitization
- SQL injection prevention
- XSS protection
- Rate limiting

## Monitoring & Observability

### Logging

- Structured logging with correlation IDs
- Request/response logging
- Error tracking
- Performance metrics

### Health Checks

- Database connectivity
- Redis connectivity
- Service status
- Resource usage


