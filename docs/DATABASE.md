# Database Documentation

## Overview

Enfyra Backend supports both MySQL and PostgreSQL databases, providing flexibility for different deployment environments and preferences. The system automatically adapts to the chosen database type and handles database-specific features appropriately.

## Supported Databases

### MySQL

- **Version**: 8.0+
- **Port**: 3306 (default)
- **Driver**: `mysql2`
- **Features**: Full support for all Enfyra features

### PostgreSQL

- **Version**: 12+
- **Port**: 5432 (default)
- **Driver**: `pg`
- **Features**: Full support for all Enfyra features

## Configuration

### Environment Variables

```bash
# Database Type Selection
DB_TYPE=mysql                    # or postgres

# Common Database Settings
DB_HOST=localhost
DB_PORT=3306                     # 3306 for MySQL, 5432 for PostgreSQL
DB_USERNAME=root                 # or postgres for PostgreSQL
DB_PASSWORD=your_password
DB_NAME=enfyra_cms

# PostgreSQL Specific Settings
DB_SSL=true                      # Enable SSL for PostgreSQL (optional)
DB_SSL_REJECT_UNAUTHORIZED=false # SSL certificate validation (optional)
```

### TypeORM Configuration

#### MySQL Configuration

```typescript
// src/data-source/data-source.ts
const dataSource = new DataSource({
  type: 'mysql',
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT),
  username: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  entities: [__dirname + '/../**/*.entity{.ts,.js}'],
  synchronize: process.env.NODE_ENV === 'development',
  logging: process.env.NODE_ENV === 'development',
  extra: {
    connectionLimit: 10,
    acquireTimeout: 60000,
    timeout: 60000,
  },
});
```

#### PostgreSQL Configuration

```typescript
// src/data-source/data-source.ts
const dataSource = new DataSource({
  type: 'postgres',
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT),
  username: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  entities: [__dirname + '/../**/*.entity{.ts,.js}'],
  synchronize: process.env.NODE_ENV === 'development',
  logging: process.env.NODE_ENV === 'development',
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

## Database Setup

### MySQL Setup

#### Installation

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install mysql-server

# macOS
brew install mysql

# Start MySQL service
sudo systemctl start mysql
sudo systemctl enable mysql
```

#### Create Database

```bash
# Access MySQL
mysql -u root -p

# Create database
CREATE DATABASE enfyra_cms CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

# Create user (optional)
CREATE USER 'enfyra'@'localhost' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON enfyra_cms.* TO 'enfyra'@'localhost';
FLUSH PRIVILEGES;
```

### PostgreSQL Setup

#### Installation

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql postgresql-contrib

# macOS
brew install postgresql

# Start PostgreSQL service
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

#### Create Database

```bash
# Switch to postgres user
sudo -u postgres psql

# Create database
CREATE DATABASE enfyra_cms;

# Create user (optional)
CREATE USER enfyra WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE enfyra_cms TO enfyra;
```

## Database Features

### Dynamic Schema Management

Both MySQL and PostgreSQL support dynamic schema creation and modification:

```typescript
// Create table dynamically
const tableDefinition = {
  name: 'products',
  columns: [
    {
      name: 'id',
      type: 'int',
      isPrimary: true,
      isAutoIncrement: true,
    },
    {
      name: 'name',
      type: 'varchar',
      length: 255,
      isNullable: false,
    },
    {
      name: 'price',
      type: 'decimal',
      precision: 10,
      scale: 2,
    },
  ],
};

await this.tableService.create(tableDefinition);
```

### Data Types Mapping

| Enfyra Type | MySQL          | PostgreSQL     |
| ----------- | -------------- | -------------- |
| `int`       | `INT`          | `INTEGER`      |
| `bigint`    | `BIGINT`       | `BIGINT`       |
| `varchar`   | `VARCHAR(n)`   | `VARCHAR(n)`   |
| `text`      | `TEXT`         | `TEXT`         |
| `decimal`   | `DECIMAL(p,s)` | `DECIMAL(p,s)` |
| `boolean`   | `BOOLEAN`      | `BOOLEAN`      |
| `datetime`  | `DATETIME`     | `TIMESTAMP`    |
| `date`      | `DATE`         | `DATE`         |
| `json`      | `JSON`         | `JSONB`        |

### Relations Support

Both databases support all relation types:

```typescript
// One-to-Many
{
  name: 'author',
  type: 'many-to-one',
  targetTable: 'users',
  foreignKey: 'authorId'
}

// Many-to-Many
{
  name: 'tags',
  type: 'many-to-many',
  targetTable: 'tags',
  joinTable: 'post_tags'
}
```

## Query Engine Compatibility

### Filter Operators

All filter operators work consistently across both databases:

```typescript
// These work the same in MySQL and PostgreSQL
filter: {
  title: { _contains: 'hello' },
  price: { _between: "100,500" },
  status: { _in: ['active', 'pending'] },
  deletedAt: { _is_null: true }
}
```

### SQL Generation

The Query Engine automatically generates database-specific SQL:

#### MySQL Example

```sql
SELECT * FROM products
WHERE title LIKE '%hello%'
  AND price BETWEEN 100 AND 500
  AND status IN ('active', 'pending')
```

#### PostgreSQL Example

```sql
SELECT * FROM products
WHERE title ILIKE '%hello%'
  AND price BETWEEN 100 AND 500
  AND status = ANY(ARRAY['active', 'pending'])
```

## Performance Considerations

### MySQL Optimizations

```sql
-- Create indexes for better performance
CREATE INDEX idx_products_title ON products(title);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_status ON products(status);

-- Optimize table
OPTIMIZE TABLE products;
```

### PostgreSQL Optimizations

```sql
-- Create indexes for better performance
CREATE INDEX idx_products_title ON products USING gin(to_tsvector('english', title));
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_status ON products(status);

-- Analyze table for query planner
ANALYZE products;
```

### Connection Pooling

#### MySQL Pool Settings

```typescript
extra: {
  connectionLimit: 10,
  acquireTimeout: 60000,
  timeout: 60000,
  queueLimit: 0,
}
```

#### PostgreSQL Pool Settings

```typescript
extra: {
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
  maxUses: 7500,
}
```

## Migration Support

### TypeORM Migrations

Both databases support TypeORM migrations:

```bash
# Generate migration
npm run migration:generate -- src/migrations/CreateProductsTable

# Run migrations
npm run migration:run

# Revert migration
npm run migration:revert
```

### Database-Specific Migrations

```typescript
// MySQL migration
export class CreateProductsTable1234567890123 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      ) ENGINE=InnoDB
    `);
  }
}

// PostgreSQL migration
export class CreateProductsTable1234567890123 implements MigrationInterface {
  async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`
      CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        price DECIMAL(10,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
  }
}
```

## Backup and Recovery

### MySQL Backup

```bash
# Create backup
mysqldump -u root -p enfyra_cms > backup.sql

# Restore backup
mysql -u root -p enfyra_cms < backup.sql
```

### PostgreSQL Backup

```bash
# Create backup
pg_dump -U postgres enfyra_cms > backup.sql

# Restore backup
psql -U postgres enfyra_cms < backup.sql
```

## Monitoring and Maintenance

### MySQL Monitoring

```sql
-- Check slow queries
SHOW VARIABLES LIKE 'slow_query_log';
SHOW VARIABLES LIKE 'long_query_time';

-- Check table sizes
SELECT
  table_name,
  ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)'
FROM information_schema.tables
WHERE table_schema = 'enfyra_cms';
```

### PostgreSQL Monitoring

```sql
-- Check slow queries
SELECT query, mean_time, calls
FROM pg_stat_statements
ORDER BY mean_time DESC
LIMIT 10;

-- Check table sizes
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public';
```

## Troubleshooting

### Common MySQL Issues

1. **Connection refused**

```bash
# Check MySQL service
sudo systemctl status mysql

# Check port
sudo netstat -tlnp | grep 3306
```

2. **Access denied**

```sql
-- Reset root password
ALTER USER 'root'@'localhost' IDENTIFIED BY 'new_password';
FLUSH PRIVILEGES;
```

### Common PostgreSQL Issues

1. **Connection refused**

```bash
# Check PostgreSQL service
sudo systemctl status postgresql

# Check port
sudo netstat -tlnp | grep 5432
```

2. **Authentication failed**

```bash
# Edit pg_hba.conf
sudo nano /etc/postgresql/*/main/pg_hba.conf

# Restart PostgreSQL
sudo systemctl restart postgresql
```

## Best Practices

### General Database Practices

1. **Use connection pooling** to manage database connections efficiently
2. **Create appropriate indexes** for frequently queried columns
3. **Regular backups** to prevent data loss
4. **Monitor performance** using database-specific tools
5. **Use transactions** for data consistency

### MySQL Specific

1. **Use InnoDB engine** for ACID compliance
2. **Configure innodb_buffer_pool_size** appropriately
3. **Enable slow query log** for performance monitoring
4. **Use utf8mb4** character set for full Unicode support

### PostgreSQL Specific

1. **Use JSONB** for JSON data (better performance than JSON)
2. **Configure shared_buffers** appropriately
3. **Use pg_stat_statements** for query monitoring
4. **Enable SSL** for production deployments

## Switching Between Databases

To switch between MySQL and PostgreSQL:

1. **Export data from current database** using native tools (mysqldump or pg_dump)
2. **Update environment variables** for the new database type and connection details
3. **Import data to new database** (manual transformation required due to SQL dialect differences)

Note: Data transformation between MySQL and PostgreSQL requires careful handling of data types and SQL syntax differences.
