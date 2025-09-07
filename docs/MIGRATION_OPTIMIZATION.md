# Migration Optimization Documentation

## Overview

This document describes the migration optimization logic that converts TypeORM's default DROP/ADD column operations into more efficient and data-preserving operations.

## Problem Statement

By default, TypeORM generates migrations that:

1. **DROP COLUMN** old_column
2. **ADD COLUMN** new_column

This approach **loses data** when renaming columns and is inefficient for type changes.

## Solution

Our optimization logic detects DROP/ADD patterns and converts them to:

- **Column renames**: `RENAME COLUMN` (preserves data)
- **Type changes**: `MODIFY COLUMN` / `ALTER COLUMN` (more efficient)

## Use Cases & Test Cases

### ✅ Supported Use Cases

#### 1. Column Rename (MySQL)

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `old_name`;
ALTER TABLE `users` ADD `new_name` varchar(255);
```

**Output:**

```sql
ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`;
```

#### 2. Column Rename (PostgreSQL)

**Input:**

```sql
ALTER TABLE "users" DROP COLUMN "old_name";
ALTER TABLE "users" ADD "new_name" varchar(255);
```

**Output:**

```sql
ALTER TABLE "users" RENAME COLUMN "old_name" TO "new_name";
```

#### 3. Column Type Change (MySQL)

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `email`;
ALTER TABLE `users` ADD `email` varchar(500);
```

**Output:**

```sql
ALTER TABLE `users` MODIFY COLUMN `email` varchar(500);
```

#### 4. Column Type Change (PostgreSQL)

**Input:**

```sql
ALTER TABLE "users" DROP COLUMN "email";
ALTER TABLE "users" ADD "email" varchar(500);
```

**Output:**

```sql
ALTER TABLE "users" ALTER COLUMN "email" TYPE varchar(500);
```

#### 5. Complex Column Definitions

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `email`;
ALTER TABLE `users` ADD `email` varchar(255) NOT NULL DEFAULT 'test@example.com';
```

**Output:**

```sql
ALTER TABLE `users` MODIFY COLUMN `email` varchar(255) NOT NULL DEFAULT 'test@example.com';
```

#### 6. Multiple DROP/ADD Pairs

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `old_name`;
ALTER TABLE `users` ADD `new_name` varchar(255);
ALTER TABLE `users` DROP COLUMN `old_email`;
ALTER TABLE `users` ADD `new_email` varchar(500);
```

**Output:**

```sql
ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`;
ALTER TABLE `users` RENAME COLUMN `old_email` TO `new_email`;
```

#### 7. Mixed Quotes in Names

**Input:**

```sql
ALTER TABLE "users" DROP COLUMN `old_name`;
ALTER TABLE "users" ADD `new_name` varchar(255);
```

**Output:**

```sql
ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`;
```

### ❌ Unsupported Use Cases (Fallback to Original)

#### 1. Non-consecutive DROP/ADD

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `old_name`;
ALTER TABLE `posts` ADD `title` varchar(255);
ALTER TABLE `users` ADD `new_name` varchar(255);
```

**Output:** (No optimization - uses original queries)

#### 2. Different Tables

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `old_name`;
ALTER TABLE `posts` ADD `new_name` varchar(255);
```

**Output:** (No optimization - uses original queries)

#### 3. Unsupported Database Types

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN `old_name`;
ALTER TABLE `users` ADD `new_name` varchar(255);
```

**Output:** (No optimization for SQLite, etc. - uses original queries)

#### 4. Invalid Column Names

**Input:**

```sql
ALTER TABLE `users` DROP COLUMN ``;
ALTER TABLE `users` ADD `` varchar(255);
```

**Output:** (No optimization - uses original queries)

#### 5. Malformed Queries

**Input:**

```sql
INVALID SQL QUERY;
ALTER TABLE `users` ADD `new_name` varchar(255);
```

**Output:** (No optimization - uses original queries)

## Edge Cases Handled

### 1. Empty Queries Array

- **Input:** `[]`
- **Output:** `[]`
- **Behavior:** Returns empty array

### 2. Single Query

- **Input:** `[{ query: 'ALTER TABLE users ADD new_column varchar(255)' }]`
- **Output:** Same as input
- **Behavior:** No optimization needed

### 3. Malformed SQL

- **Input:** `[{ query: 'INVALID SQL' }]`
- **Output:** Same as input
- **Behavior:** Gracefully handles invalid SQL

### 4. Missing Column Names

- **Input:** `[{ query: 'ALTER TABLE users DROP COLUMN ' }]`
- **Output:** Same as input
- **Behavior:** Skips optimization for invalid names

## Database Support

### ✅ Fully Supported

- **MySQL 8.0+**: `RENAME COLUMN`, `MODIFY COLUMN`
- **PostgreSQL**: `RENAME COLUMN`, `ALTER COLUMN TYPE`

### ⚠️ Partially Supported

- **MySQL < 8.0**: Falls back to DROP/ADD for renames
- **SQLite**: Falls back to DROP/ADD
- **Other databases**: Falls back to DROP/ADD

## Implementation Details

### Regex Patterns

```typescript
// Drop column pattern
/ALTER TABLE [`"]?([^`"\s]+)[`"]?\s+DROP COLUMN [`"]?([^`"\s]+)[`"]?/i

// Add column pattern
/ALTER TABLE [`"]?([^`"\s]+)[`"]?\s+ADD [`"]?([^`"\s]+)[`"]?\s+(.+)/i
```

### Optimization Logic

1. **Detect DROP/ADD pairs** in consecutive queries
2. **Validate table names** match
3. **Check column names**:
   - Same name = Type change
   - Different names = Rename
4. **Generate appropriate SQL** based on database type
5. **Handle unsupported cases** with fallback

### Error Handling

- **Invalid names**: Skip optimization
- **Unsupported DB**: Use original queries
- **Malformed SQL**: Graceful fallback
- **Non-consecutive**: No optimization

## Testing

Run the test suite:

```bash
npm test -- --testPathPattern=migration-helper.spec.ts
```

### Test Coverage

- ✅ Column renames (MySQL/PostgreSQL)
- ✅ Type changes (MySQL/PostgreSQL)
- ✅ Complex definitions
- ✅ Multiple operations
- ✅ Edge cases
- ✅ Error handling
- ✅ Database compatibility

## Migration Examples

### Before Optimization

```sql
-- TypeORM default (loses data)
ALTER TABLE `users` DROP COLUMN `old_name`;
ALTER TABLE `users` ADD `new_name` varchar(255);
```

### After Optimization

```sql
-- Our optimization (preserves data)
ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`;
```

## Benefits

1. **Data Preservation**: Renames don't lose data
2. **Performance**: Type changes are more efficient
3. **Safety**: Graceful fallback for unsupported cases
4. **Compatibility**: Works with multiple database types
5. **Maintainability**: Clear, testable logic

## Future Enhancements

1. **Support for more databases** (SQLite, Oracle, etc.)
2. **Index preservation** during renames
3. **Constraint handling** (foreign keys, unique constraints)
4. **Data validation** before optimization
5. **Rollback strategy** improvements
