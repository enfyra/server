# Development Guide

## Setup

### Prerequisites

- Node.js 18+
- MySQL 8.0+ or PostgreSQL 12+
- Redis 6.0+
- Git

### Installation

```bash
# Clone repository
git clone <repository-url>
cd enfyra_be

# Install dependencies
npm install

# Copy environment file
cp env_example .env

# Edit environment variables
nano .env
```

### Environment Configuration

```bash
# Database Configuration
# Choose one: MySQL or PostgreSQL
DB_TYPE=mysql                    # or postgres
DB_HOST=localhost
DB_PORT=3306                     # 3306 for MySQL, 5432 for PostgreSQL
DB_USERNAME=root
DB_PASSWORD=your_password
DB_NAME=enfyra_cms

# PostgreSQL specific (if using PostgreSQL)
# DB_SSL=true                    # Enable SSL for PostgreSQL
# DB_SSL_REJECT_UNAUTHORIZED=false

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379

# JWT
SECRET_KEY=your-secret-key
ACCESS_TOKEN_EXP=15m
REFRESH_TOKEN_NO_REMEMBER_EXP=1d
REFRESH_TOKEN_REMEMBER_EXP=7d

# Server
PORT=1105
NODE_ENV=development
```

### Database Setup

#### MySQL Setup

```bash
# Create MySQL database
mysql -u root -p
CREATE DATABASE enfyra_cms;
```

#### PostgreSQL Setup

```bash
# Create PostgreSQL database
psql -U postgres
CREATE DATABASE enfyra_cms;
```

### Start Development Server

```bash
# Start in development mode
npm run start:dev

# Start in production mode
npm run start
```

## Project Structure

```
src/
├── auth/                 # Authentication & authorization
├── auto/                 # Code generation utilities
├── bootstrap/            # Initial setup and data
├── common/               # Shared utilities and services
├── data-source/          # Database connection management
├── database/             # Database configuration
├── decorators/           # Custom decorators
├── dynamic/              # Dynamic API layer
├── dynamic-repo/         # Dynamic repository layer
├── entities/             # TypeORM entities
├── exceptions/           # Custom exception classes
├── error-handling/       # Error handling system
├── graphql/              # GraphQL implementation
├── guard/                # Authentication guards
├── handler-executor/     # Script execution system
├── interceptors/         # Request/response interceptors
├── middleware/           # Custom middleware
├── query-engine/         # Query processing engine
├── redis/                # Redis services
├── schema/               # Schema management
├── sql/                  # SQL utilities
├── table/                # Table management
├── test/                 # Test files
├── utils/                # Utility functions
├── validator/            # Custom validators
├── app.module.ts         # Main application module
└── main.ts              # Application entry point
```

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/new-feature

# Make changes
# ... code changes ...

# Run tests
npm run test

# Run linting
npm run lint

# Commit changes
git add .
git commit -m "feat: add new feature"

# Push branch
git push origin feature/new-feature
```

### 2. Testing

```bash
# Run all tests
npm run test

# Run tests in watch mode
npm run test:watch

# Run specific test file
npm run test src/test/auth.service.spec.ts

# Run tests with coverage
npm run test:cov
```

### 3. Code Quality

```bash
# Run linting
npm run lint

# Fix linting issues
npm run lint:fix

# Run type checking
npm run build
```

## API Development

### Creating New Endpoints

1. **Define Table Schema**

```typescript
// Create table definition
const tableDef = {
  name: 'products',
  columns: [
    { name: 'id', type: 'int', isPrimary: true, isAutoIncrement: true },
    { name: 'name', type: 'varchar', length: 255 },
    { name: 'price', type: 'decimal', precision: 10, scale: 2 },
  ],
};
```

2. **Add Custom Handler (Optional)**

```javascript
// Custom logic for GET /products
if ($ctx.$user.role !== 'admin') {
  return await $ctx.$repos.products.find({
    where: { isPublic: { _eq: true } },
  });
}
```

3. **Test Endpoint**

**REST API:**

```http
GET /posts
```

**GraphQL:**

```graphql
query {
  posts {
    data {
      id
      title
      content
    }
  }
}
```

### Adding Custom Logic

1. **REST Handler**

```javascript
// Custom POST /products
if ($ctx.$body.price < 0) {
  throw new Error('Price cannot be negative');
}

const product = await $ctx.$repos.products.create($ctx.$body);
return { success: true, data: product };
```

2. **GraphQL Handler**

```javascript
// Custom products query
const products = await $ctx.$repos.products.find({
  where: $ctx.$args.filter,
  sort: $ctx.$args.sort,
  page: $ctx.$args.page,
  limit: $ctx.$args.limit,
});

return {
  data: products.data,
  meta: products.meta,
};
```

## Database Development

### Creating Tables

```typescript
// Using Table Service
const tableService = this.moduleRef.get(TableService);

const tableDefinition = {
  name: 'users',
  columns: [
    {
      name: 'id',
      type: 'int',
      isPrimary: true,
      isAutoIncrement: true,
    },
    {
      name: 'email',
      type: 'varchar',
      length: 255,
      isUnique: true,
      isNullable: false,
    },
    {
      name: 'password',
      type: 'varchar',
      length: 255,
      isNullable: false,
    },
  ],
};

await tableService.create(tableDefinition);
```

### Adding Relations

```typescript
const tableDefinition = {
  name: 'posts',
  columns: [
    { name: 'id', type: 'int', isPrimary: true, isAutoIncrement: true },
    { name: 'title', type: 'varchar', length: 255 },
    { name: 'authorId', type: 'int', isNullable: false },
  ],
  relations: [
    {
      name: 'author',
      type: 'many-to-one',
      targetTable: 'users',
      foreignKey: 'authorId',
    },
  ],
};
```

### Running Migrations

```bash
# Generate migration
npm run migration:generate -- src/migrations/CreateUsersTable

# Run migrations
npm run migration:run

# Revert migration
npm run migration:revert
```

## Testing

### Unit Tests

```typescript
// src/test/auth.service.spec.ts
import { Test, TestingModule } from '@nestjs/testing';
import { AuthService } from '../auth/auth.service';

describe('AuthService', () => {
  let service: AuthService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [AuthService],
    }).compile();

    service = module.get<AuthService>(AuthService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });

  it('should validate user credentials', async () => {
    const result = await service.validateUser('test@example.com', 'password');
    expect(result).toBeDefined();
  });
});
```

### Integration Tests

```typescript
// src/test/api.integration.spec.ts
import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { AppModule } from '../app.module';

describe('API Integration', () => {
  let app: INestApplication;

  beforeEach(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  });

  it('/posts (GET)', () => {
    return request(app.getHttpServer()).get('/posts').expect(200);
  });
});
```

### E2E Tests

```typescript
// test/app.e2e-spec.ts
import { Test, TestingModule } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import * as request from 'supertest';
import { AppModule } from './../src/app.module';

describe('AppController (e2e)', () => {
  let app: INestApplication;

  beforeEach(async () => {
    const moduleFixture: TestingModule = await Test.createTestingModule({
      imports: [AppModule],
    }).compile();

    app = moduleFixture.createNestApplication();
    await app.init();
  });

  it('/ (GET)', () => {
    return request(app.getHttpServer()).get('/').expect(200);
  });
});
```

## Debugging

### Logging

```typescript
import { Logger } from '@nestjs/common';

export class MyService {
  private readonly logger = new Logger(MyService.name);

  async someMethod() {
    this.logger.log('Starting operation');
    this.logger.debug('Debug information');
    this.logger.warn('Warning message');
    this.logger.error('Error occurred', error.stack);
  }
}
```

### Debug Mode

```bash
# Start with debug logging
DEBUG=* npm run start:dev

# Start with specific debug namespace
DEBUG=enfyra:* npm run start:dev
```

### Using Debugger

```bash
# Start with debugger
npm run start:debug

# Attach debugger in VS Code
# Add breakpoints in code
# Use F5 to start debugging
```

## Performance Optimization

### Query Optimization

```typescript
// Use specific fields
const users = await this.userRepository.find({
  select: ['id', 'name', 'email'],
  where: { isActive: true },
});

// Use relations efficiently
const posts = await this.postRepository.find({
  relations: ['author'],
  where: { published: true },
});
```

### Caching

```typescript
// Use Redis cache
@Injectable()
export class CacheService {
  constructor(private redisService: RedisService) {}

  async get(key: string) {
    return await this.redisService.get(key);
  }

  async set(key: string, value: any, ttl?: number) {
    await this.redisService.set(key, JSON.stringify(value), ttl);
  }
}
```

### Connection Pooling

#### MySQL Configuration

```typescript
// Configure MySQL connection pool
const dataSource = new DataSource({
  type: 'mysql',
  host: 'localhost',
  port: 3306,
  username: 'root',
  password: 'password',
  database: 'enfyra_cms',
  extra: {
    connectionLimit: 10,
    acquireTimeout: 60000,
    timeout: 60000,
  },
});
```

#### PostgreSQL Configuration

```typescript
// Configure PostgreSQL connection pool
const dataSource = new DataSource({
  type: 'postgres',
  host: 'localhost',
  port: 5432,
  username: 'postgres',
  password: 'password',
  database: 'enfyra_cms',
  ssl:
    process.env.DB_SSL === 'true'
      ? {
          rejectUnauthorized:
            process.env.DB_SSL_REJECT_UNAUTHORIZED !== 'false',
        }
      : false,
  extra: {
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
  },
});
```

## Code Style

### TypeScript Guidelines

```typescript
// Use interfaces for object shapes
interface User {
  id: number;
  email: string;
  name: string;
}

// Use enums for constants
enum UserRole {
  ADMIN = 'admin',
  USER = 'user',
  MODERATOR = 'moderator',
}

// Use async/await consistently
async function fetchUser(id: number): Promise<User> {
  const user = await this.userRepository.findOne({ where: { id } });
  if (!user) {
    throw new ResourceNotFoundException('User', id.toString());
  }
  return user;
}
```

### Naming Conventions

- **Files**: kebab-case (`user-service.ts`)
- **Classes**: PascalCase (`UserService`)
- **Methods**: camelCase (`getUserById`)
- **Constants**: UPPER_SNAKE_CASE (`MAX_RETRY_COUNT`)
- **Interfaces**: PascalCase with `I` prefix (`IUserService`)

### Documentation

```typescript
/**
 * Service for managing user operations
 */
@Injectable()
export class UserService {
  /**
   * Find user by ID
   * @param id User ID
   * @returns User object or null if not found
   */
  async findById(id: number): Promise<User | null> {
    return await this.userRepository.findOne({ where: { id } });
  }
}
```

## Deployment

### Development Deployment

```bash
# Build application
npm run build

# Start with PM2
pm2 start ecosystem.config.js --env development
```

### Production Deployment

```bash
# Build for production
npm run build:prod

# Start with PM2
pm2 start ecosystem.config.js --env production
```


## Troubleshooting

### Common Issues

1. **Port already in use**

```bash
# Find process using port
lsof -i :1105

# Kill process
kill -9 <PID>
```

2. **Database connection failed**

```bash
# Check MySQL service
sudo systemctl status mysql

# Check PostgreSQL service
sudo systemctl status postgresql

# Check credentials in .env
# Verify database exists
```

3. **Redis connection failed**

```bash
# Check Redis service
sudo systemctl status redis

# Test Redis connection
redis-cli ping
```

### Performance Issues

1. **Slow queries**

```bash
# Enable slow query log
SET GLOBAL slow_query_log = 'ON';
SET GLOBAL long_query_time = 2;
```

2. **Memory leaks**

```bash
# Monitor memory usage
pm2 monit

# Check for memory leaks
node --inspect npm run start
```

### Debug Commands

```bash
# Check application status
pm2 status

# View logs
pm2 logs enfyra-backend

# Monitor resources
pm2 monit

# Restart application
pm2 restart enfyra-backend
```
