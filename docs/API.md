# API Documentation

## Overview

Enfyra Backend provides both REST and GraphQL APIs for dynamic data operations. All APIs are automatically generated based on table definitions and can be customized with JavaScript handlers.

## REST API

### Base URL

```
http://localhost:1105
```

### Authentication

All API endpoints require JWT authentication. Include the token in the Authorization header:

```
Authorization: Bearer <your-jwt-token>
```

### Default Endpoints

#### List Records

```http
GET /{table_name}
```

**Query Parameters:**

- `filter[field][operator]=value` - Filter records
- `sort=field|-field` - Sort records (prefix with `-` for descending)
- `page=number` - Page number (default: 1)
- `limit=number` - Records per page (default: 10, 0 for all)
- `fields=field1,field2` - Select specific fields
- `include=relation1,relation2` - Include related data

**Example:**

```bash
curl "http://localhost:1105/posts?filter[title][_contains]=hello&sort=-createdAt&page=1&limit=10"
```

#### Get Single Record

```http
GET /{table_name}/{id}
```

**Example:**

```bash
curl "http://localhost:1105/posts/1"
```

#### Create Record

```http
POST /{table_name}
```

**Example:**

```http
POST /posts
Content-Type: application/json

{
  "title": "Hello World",
  "content": "This is my first post"
}
```

#### Update Record

```http
PATCH /{table_name}/{id}
```

**Example:**

```http
PATCH /posts/1
Content-Type: application/json

{
  "title": "Updated Title"
}
```

#### Delete Record

```http
DELETE /{table_name}/{id}
```

**Example:**

```http
DELETE /posts/1
```

### Filter Operators

The Enfyra API uses MongoDB-like operators for filtering. Below is a quick reference:

| Operator       | Description           | Example                               |
| -------------- | --------------------- | ------------------------------------- |
| `_eq`          | Equal                 | `filter[status][_eq]=published`       |
| `_neq`         | Not equal             | `filter[status][_neq]=draft`          |
| `_gt`          | Greater than          | `filter[price][_gt]=100`              |
| `_gte`         | Greater than or equal | `filter[price][_gte]=100`             |
| `_lt`          | Less than             | `filter[price][_lt]=500`              |
| `_lte`         | Less than or equal    | `filter[price][_lte]=500`             |
| `_in`          | In array              | `filter[category][_in]=tech,business` |
| `_not_in`      | Not in array          | `filter[category][_not_in]=tech`      |
| `_between`     | Between values        | `filter[price][_between]=100,500`     |
| `_is_null`     | Is null               | `filter[deletedAt][_is_null]=true`    |
| `_contains`    | Contains text         | `filter[title][_contains]=hello`      |
| `_starts_with` | Starts with           | `filter[title][_starts_with]=hello`   |
| `_ends_with`   | Ends with             | `filter[title][_ends_with]=world`     |
| `_and`         | AND logic             | `filter[_and][0][age][_gte]=18`       |
| `_or`          | OR logic              | `filter[_or][0][role][_eq]=admin`     |
| `_not`         | NOT logic             | `filter[_not][status][_eq]=draft`     |

**Note**: For `_between` operator, you can use either:
- Comma-separated string: `filter[price][_between]=100,500`
- Array format (in JSON body): `{ "price": { "_between": [100, 500] } }`

For comprehensive documentation on query operators, complex filters, aggregations, and SQL equivalents, see [Query Engine Documentation](./QUERY_ENGINE.md).

### Response Format

#### Success Response

```json
{
  "data": [
    {
      "id": 1,
      "title": "Hello World",
      "content": "This is my first post",
      "createdAt": "2025-08-05T03:54:42.610Z",
      "updatedAt": "2025-08-05T03:54:42.610Z"
    }
  ],
  "meta": {
    "totalCount": 4
  },
  "statusCode": 200,
  "message": "Success"
}
```

**Note**: The `meta` object is only included when `meta` parameter is specified in the request.

#### Error Response

```json
{
  "success": false,
  "message": "Error message",
  "statusCode": 400,
  "error": {
    "code": "ERROR_CODE",
    "message": "Error message",
    "details": null,
    "timestamp": "2025-08-05T03:54:42.610Z",
    "path": "/api/endpoint",
    "method": "GET",
    "correlationId": "req_1754366082608_f1ts2w7za"
  }
}
```

## GraphQL API

### Endpoint

```
http://localhost:1105/graphql
```

### Authentication

Include JWT token in HTTP headers:

```
Authorization: Bearer <your-jwt-token>
```

### Schema

GraphQL schema is automatically generated from table definitions. Each table becomes a type with corresponding queries and mutations.

### Queries

#### List Records

```graphql
query {
  posts(
    filter: { title: { _contains: "hello" } }
    sort: { createdAt: DESC }
    page: 1
    limit: 10
  ) {
    data {
      id
      title
      content
      createdAt
      updatedAt
    }
    meta {
      totalCount
      page
      limit
      totalPages
    }
  }
}
```

#### Get Single Record

```graphql
query {
  post(id: "1") {
    id
    title
    content
    createdAt
    updatedAt
  }
}
```

#### With Relations

```graphql
query {
  posts {
    data {
      id
      title
      content
      author {
        id
        name
        email
      }
      comments {
        id
        content
        user {
          name
        }
      }
    }
  }
}
```

### Mutations

#### Create Record

```graphql
mutation {
  createPost(
    input: { title: "Hello World", content: "This is my first post" }
  ) {
    id
    title
    content
    createdAt
  }
}
```

#### Update Record

```graphql
mutation {
  updatePost(id: "1", input: { title: "Updated Title" }) {
    id
    title
    content
    updatedAt
  }
}
```

#### Delete Record

```graphql
mutation {
  deletePost(id: "1") {
    success
    message
  }
}
```

### Filter Operators (GraphQL)

GraphQL uses the same operators as the REST API. Here are some examples:

```graphql
filter: {
  # Comparison operators
  status: { _eq: "published" }
  age: { _gte: 18 }
  price: { _between: [100, 500] }  # Can also use "100,500"
  
  # Text search
  title: { _contains: "hello" }
  email: { _starts_with: "admin" }
  
  # Array operators
  category: { _in: ["tech", "business"] }
  status: { _not_in: ["deleted", "suspended"] }
  
  # Null checks
  deletedAt: { _is_null: true }
  
  # Logical operators
  _and: [
    { status: { _eq: "active" } }
    { role: { _neq: "guest" } }
  ]
  _or: [
    { priority: { _eq: "high" } }
    { dueDate: { _lt: "2024-01-01" } }
  ]
  
  # Relation filters
  author: {
    name: { _contains: "John" }
  }
  
  # Aggregation filters
  posts: {
    _count: { _gt: 5 }
  }
}
```

For detailed documentation, see [Query Engine Documentation](./QUERY_ENGINE.md).

## Table Management API

### Create Table

```http
POST /table_definition
```

**Example:**

```http
POST /table_definition
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
      "type": "text",
      "isNullable": true
    },
    {
      "name": "authorId",
      "type": "int",
      "isNullable": false
    }
  ],
  "relations": [
    {
      "name": "author",
      "type": "many-to-one",
      "targetTable": "users",
      "foreignKey": "authorId"
    }
  ]
}
```

### Update Table

```http
PATCH /table_definition/{id}
```

### Delete Table

```http
DELETE /table_definition/{id}
```

### List Tables

```http
GET /table_definition
```

## Custom Handlers

### REST Handler Example

```javascript
// Custom logic for GET /posts
if ($ctx.$user.role !== 'admin') {
  return await $ctx.$repos.posts.find({
    where: {
      authorId: { _eq: $ctx.$user.id },
    },
  });
}

return await $ctx.$repos.posts.find({
  where: $ctx.$args.filter,
});
```

### GraphQL Handler Example

```javascript
// Custom logic for posts query
if ($ctx.$user.role !== 'admin') {
  return await $ctx.$repos.posts.find({
    where: {
      authorId: { _eq: $ctx.$user.id },
    },
  });
}

return await $ctx.$repos.posts.find({
  where: $ctx.$args.filter,
});
```

## Rate Limiting

API endpoints are rate-limited to prevent abuse:

- 100 requests per minute per IP
- 1000 requests per hour per user

## Pagination

### Cursor-based Pagination

```http
GET /posts?cursor=eyJpZCI6MTB9&limit=10
```

### Offset-based Pagination

```http
GET /posts?page=1&limit=10
```

## Field Selection

### REST API

```http
GET /posts?fields=id,title,createdAt
```

### GraphQL

```graphql
query {
  posts {
    data {
      id
      title
      createdAt
    }
  }
}
```

## Aggregate Filters

Aggregate filters allow you to filter records based on aggregate conditions of related records.

### Count Filter

Filter based on the count of related records:

```http
# Users with more than 5 posts
GET /users?filter[posts][_count][_gt]=5

# Users with no posts
GET /users?filter[posts][_count][_eq]=0
```

### Sum Filter

Filter based on the sum of a field in related records:

```http
# Users whose orders total more than $1000
GET /users?filter[orders][_sum][total][_gt]=1000
```

### Average Filter

Filter based on the average of a field in related records:

```http
# Products with average rating >= 4.5
GET /products?filter[reviews][_avg][rating][_gte]=4.5
```

### Min/Max Filter

Filter based on minimum or maximum values in related records:

```http
# Users whose minimum order is at least $50
GET /users?filter[orders][_min][total][_gte]=50

# Products whose maximum price variant is less than $100
GET /products?filter[variants][_max][price][_lt]=100
```

**Note**: Aggregate filters work with relations. The format follows the pattern:
- Count: `filter[relation][_count][operator]=value`
- Sum/Avg/Min/Max: `filter[relation][_aggregate][field][operator]=value`

For more examples and detailed documentation, see [Query Engine Documentation](./QUERY_ENGINE.md).

## Error Codes

| Code                     | Description              |
| ------------------------ | ------------------------ |
| `UNAUTHORIZED`           | Authentication required  |
| `FORBIDDEN`              | Insufficient permissions |
| `NOT_FOUND`              | Resource not found       |
| `VALIDATION_ERROR`       | Invalid input data       |
| `BUSINESS_LOGIC_ERROR`   | Business rule violation  |
| `SCRIPT_EXECUTION_ERROR` | Handler script error     |
| `SCRIPT_TIMEOUT_ERROR`   | Handler script timeout   |
| `INTERNAL_SERVER_ERROR`  | Unexpected server error  |

## Schema Synchronization

When you create or modify tables through the `table_definition` API, the system automatically:

1. **Pulls metadata** from the database
2. **Generates TypeScript entities**
3. **Creates and runs migrations**
4. **Reloads the DataSource** with new entities
5. **Reloads GraphQL schema** with new types
6. **Creates a backup** of the current schema

This process is handled by the `syncAll` method in `MetadataSyncService`.

## Raw API Examples

### JavaScript/Fetch

```javascript
// Login to get token
const loginResponse = await fetch('http://localhost:1105/auth/login', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    email: 'enfyra@admin.com',
    password: '1234'
  })
});

const { accessToken } = await loginResponse.json();

// List posts
const postsResponse = await fetch(
  'http://localhost:1105/posts?filter[title][_contains]=hello&sort=-createdAt&page=1&limit=10',
  {
    headers: {
      'Authorization': `Bearer ${accessToken}`
    }
  }
);
const posts = await postsResponse.json();

// Create post
const createResponse = await fetch('http://localhost:1105/posts', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': `Bearer ${accessToken}`
  },
  body: JSON.stringify({
    title: 'Hello World',
    content: 'This is my first post'
  })
});
const newPost = await createResponse.json();
```

### cURL Examples

```bash
# Login to get token
TOKEN=$(curl -X POST http://localhost:1105/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email": "enfyra@admin.com", "password": "1234"}' \
  | jq -r '.accessToken')

# List posts
curl -H "Authorization: Bearer $TOKEN" \
  "http://localhost:1105/posts?filter[title][_contains]=hello&sort=-createdAt&page=1&limit=10"

# Create post
curl -X POST http://localhost:1105/posts \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "title": "Hello World",
    "content": "This is my first post"
  }'
```
