# User Guide

## Overview

Enfyra Backend is an API-first platform that allows you to create and manage API endpoints, database schemas, and business logic without writing code. The system automatically generates REST API and GraphQL API based on your configuration.

## Quick Start

### 1. Login to System

**REST API:**

```http
POST /auth/login
Content-Type: application/json

{
  "email": "enfyra@admin.com",
  "password": "1234"
}
```

**Result:**

```json
{
  "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "refreshToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expTime": 1754378861000,
  "statusCode": 201,
  "message": "Success"
}
```

### 2. Use Token for Other APIs

**REST API:**

```http
GET /posts
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

## Create Data Tables

### Create Simple Table

**REST API:**

```http
POST /table_definition
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "name": "posts",
  "columns": [
    {
      "name": "id",
      "type": "int",
      "isPrimary": true,
      "isAutoIncrement": true
    },
    {
      "name": "title",
      "type": "varchar",
      "length": 255,
      "isNullable": false
    },
    {
      "name": "content",
      "type": "text"
    },
    {
      "name": "createdAt",
      "type": "datetime"
    }
  ]
}
```

### Create Table with Relations

**REST API:**

1. Create categories table:

```http
POST /table_definition
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "name": "categories",
  "columns": [
    {
      "name": "id",
      "type": "int",
      "isPrimary": true,
      "isAutoIncrement": true
    },
    {
      "name": "name",
      "type": "varchar",
      "length": 255
    }
  ]
}
```

2. Create posts table with relation:

```http
POST /table_definition
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "name": "posts",
  "columns": [
    {
      "name": "id",
      "type": "int",
      "isPrimary": true,
      "isAutoIncrement": true
    },
    {
      "name": "title",
      "type": "varchar",
      "length": 255
    },
    {
      "name": "content",
      "type": "text"
    },
    {
      "name": "categoryId",
      "type": "int"
    }
  ],
  "relations": [
    {
      "name": "category",
      "type": "many-to-one",
      "targetTable": "categories",
      "foreignKey": "categoryId"
    }
  ]
}
```

## Data Operations

### 1. Create Data (CREATE)

**REST API:**

```http
POST /posts
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "title": "My First Post",
  "content": "This is the content of my first post"
}
```

### 2. Read Data (READ)

#### Get All Posts

**REST API:**

```http
GET /posts
Authorization: Bearer <your-token>
```

#### Get Post by ID

**REST API:**

```http
GET /posts/1
Authorization: Bearer <your-token>
```

#### Get Post with Relations

**REST API:**

```http
GET /posts?include=category
Authorization: Bearer <your-token>
```

### 3. Update Data (UPDATE)

**REST API:**

```http
PATCH /posts/1
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "title": "Updated Post Title"
}
```

### 4. Delete Data (DELETE)

**REST API:**

```http
DELETE /posts/1
Authorization: Bearer <your-token>
```

## Filter and Search Data

### Basic Filter Operators

#### Exact Search

**REST API:**

```http
GET /posts?filter[title][_eq]=Hello World
Authorization: Bearer <your-token>
```

#### Contains Search

**REST API:**

```http
GET /posts?filter[title][_contains]=Hello
Authorization: Bearer <your-token>
```

#### Range Search

**REST API:**

```http
GET /posts?filter[id][_between]=1,10
```http
GET /posts?filter[createdAt][_between]=2023-01-01,2023-12-31
Authorization: Bearer <your-token>
```Authorization: Bearer <your-token>
```

#### List Search

**REST API:**

```http
GET /posts?filter[title][_in]=Hello World,Test Post,Another Post
Authorization: Bearer <your-token>
```

### Combine Multiple Conditions

**REST API:**

```http
GET /posts?filter[id][_gt]=1&filter[title][_contains]=Hello
Authorization: Bearer <your-token>
```

## Sort Data

### Sort by Single Column

**REST API:**

```http
GET /posts?sort=createdAt
Authorization: Bearer <your-token>
```

```http
GET /posts?sort=-createdAt
Authorization: Bearer <your-token>
```

### Sort by Multiple Columns

**REST API:**

```http
GET /posts?sort=categoryId,-createdAt
Authorization: Bearer <your-token>
```

## Pagination

### Basic Pagination

**REST API:**

```http
GET /posts?page=1&limit=10
Authorization: Bearer <your-token>
```

```http
GET /posts?page=2&limit=10
Authorization: Bearer <your-token>
```

### Get All Data

**REST API:**

```http
GET /posts?limit=0
Authorization: Bearer <your-token>
```

## Select Specific Fields

**REST API:**

```http
GET /posts?fields=id,title
Authorization: Bearer <your-token>
```

```http
GET /posts?fields=id,title,content,createdAt
Authorization: Bearer <your-token>
```

## Using GraphQL

### Basic Query

**GraphQL:**

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

### Query with Filter

**GraphQL:**

```graphql
query {
  posts(filter: { title: { _contains: "hello" } }) {
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

### Query with Relations

**GraphQL:**

```graphql
query {
  posts {
    data {
      id
      title
      content
      createdAt
      updatedAt
      user {
        id
        email
      }
    }
  }
}
```

**Note**: GraphQL schema includes all fields including timestamp fields (createdAt, updatedAt) that are automatically generated by TypeORM.

**Note**: GraphQL schema is automatically generated and reloaded when tables are created or modified through the `table_definition` API. The system runs `syncAll` internally to update the schema.

## Add Sample Data

### Create Multiple Posts at Once

**REST API:**

```http
POST /posts
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "title": "Post 1",
  "content": "Content for post 1"
}
```

```http
POST /posts
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "title": "Post 2",
  "content": "Content for post 2"
}
```

### Create Data from JSON File

**REST API:**

```http
POST /posts
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "title": "Getting Started with Enfyra",
  "content": "Learn how to use the Enfyra platform for building dynamic APIs"
}
```

```http
POST /posts
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "title": "Advanced Filtering Techniques",
  "content": "Explore advanced filtering and querying capabilities"
}
```

## Table Management

### View Table List

**REST API:**

```http
GET /table_definition
Authorization: Bearer <your-token>
```

### View Table Structure

**REST API:**

```http
GET /table_definition/11
Authorization: Bearer <your-token>
```

### Update Table Structure

```bash
# Add new column to table
curl -X PATCH http://localhost:1105/table_definition/11 \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "columns": [
      {
        "name": "id",
        "type": "int",
        "isPrimary": true,
        "isAutoIncrement": true
      },
      {
        "name": "title",
        "type": "varchar",
        "length": 255
      },
      {
        "name": "content",
        "type": "text"
      },
      {
        "name": "author",
        "type": "varchar",
        "length": 100
      }
    ]
  }'
```

### Delete Table

**REST API:**

```http
DELETE /table_definition/11
Authorization: Bearer <your-token>
```

**Warning**: This will permanently delete the table and all its data.

## Statistics and Reports

### Get Record Count

**REST API:**

```http
GET /posts?meta=totalCount&limit=0
Authorization: Bearer <your-token>
```

### Filter by Count Condition

**REST API:**

```http
GET /posts?filter[count.comments.id][_gt]=2
Authorization: Bearer <your-token>
```

### Filter by Aggregate Conditions

**REST API:**

```http
GET /posts?filter[count.comments.id][_gt]=2
Authorization: Bearer <your-token>
```

```http
GET /posts?filter[sum.comments.id][_gt]=100
Authorization: Bearer <your-token>
```

**Note**:

- Use `meta=totalCount` to get record counts
- Use `aggregate[count/sum/avg]` in filters to filter by aggregate conditions of related records
- Aggregate functions work with relations, not direct table fields

## Common Error Handling

### Authentication Error

```json
{
  "success": false,
  "message": "Unauthorized",
  "statusCode": 401,
  "error": {
    "code": "UNAUTHORIZED",
    "message": "Invalid or expired token"
  }
}
```

**How to fix:**

- Check if token is correct
- Check if token has expired
- Login again to get new token

### Not Found Error

```json
{
  "success": false,
  "message": "Resource not found",
  "statusCode": 404,
  "error": {
    "code": "NOT_FOUND",
    "message": "Post with id 999 not found"
  }
}
```

**How to fix:**

- Check if ID exists
- Check if table name is correct

### Validation Error

```json
{
  "success": false,
  "message": "Validation failed",
  "statusCode": 400,
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "Title cannot be empty"
  }
}
```

**How to fix:**

- Check data format
- Check required fields
- Check length limits

## Usage Tips

### 1. Use jq to process JSON

**Install jq:**

```bash
# macOS: brew install jq
# Ubuntu: sudo apt install jq
```

**Process JSON responses:**

```bash
# Get only post titles
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:1105/posts | jq '.data[].title'

# Filter posts by title containing "test"
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:1105/posts | jq '.data[] | select(.title | contains("test"))'
```

### 2. Create aliases for common commands

**Add to ~/.bashrc or ~/.zshrc:**

```bash
alias enfyra-login='curl -X POST http://localhost:1105/auth/login -H "Content-Type: application/json" -d '"'"'{"email": "enfyra@admin.com", "password": "1234"}'"'"' | jq -r ".accessToken"'

alias enfyra-posts='curl -H "Authorization: Bearer $(enfyra-login)" http://localhost:1105/posts'
```

### 3. Use scripts for automation

**script.sh:**

```bash
#!/bin/bash

# Login and get token
TOKEN=$(curl -s -X POST http://localhost:1105/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "enfyra@admin.com", "password": "1234"}' | \
  jq -r '.accessToken')

# Create post
curl -X POST http://localhost:1105/posts \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "title": "New Post",
    "content": "Created by script"
  }'

# Get post list
curl -H "Authorization: Bearer $TOKEN" \
  http://localhost:1105/posts | jq '.data'
```

## Real Examples

### Blog Management System

**1. Create users table:**

```http
POST /table_definition
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "name": "users",
  "columns": [
    {"name": "id", "type": "int", "isPrimary": true, "isAutoIncrement": true},
    {"name": "email", "type": "varchar", "length": 255},
    {"name": "name", "type": "varchar", "length": 255}
  ]
}
```

**2. Create comments table:**

```http
POST /table_definition
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "name": "comments",
  "columns": [
    {"name": "id", "type": "int", "isPrimary": true, "isAutoIncrement": true},
    {"name": "content", "type": "text"},
    {"name": "postId", "type": "int"},
    {"name": "userId", "type": "int"}
  ],
  "relations": [
    {
        "name": "post",
        "type": "many-to-one",
        "targetTable": "posts",
        "foreignKey": "postId"
      },
      {
        "name": "user",
        "type": "many-to-one",
        "targetTable": "users",
        "foreignKey": "userId"
      }
    ]
  }
}
```

**3. Add Sample Data:**

```http
POST /users
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "email": "john@example.com",
  "name": "John Doe"
}
```

```http
POST /comments
Authorization: Bearer <your-token>
Content-Type: application/json

{
  "content": "Great post!",
  "postId": 1,
  "userId": 1
}
```

**4. Query data:**

```http
GET /posts?include=comments
Authorization: Bearer <your-token>
```

```http
GET /posts?filter[count.comments.id][_gt]=0
Authorization: Bearer <your-token>
```

## Support

If you encounter problems using the system:

1. **Check logs**: View server logs to find errors
2. **Check connection**: Ensure server is running
3. **Check permissions**: Ensure you have access rights
4. **Contact admin**: If still cannot resolve

---

_This guide was last updated: August 2025_
