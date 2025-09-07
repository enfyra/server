# Query Engine Documentation

## Overview

The Enfyra Query Engine provides a powerful and flexible way to query data using MongoDB-like operators through REST and GraphQL APIs. It features:

✨ **Automatic Smart Joins**: The engine automatically creates optimal JOIN queries when you filter or select fields from related tables - no manual JOIN needed!

🚀 **Optimized Performance**: All queries are translated into efficient SQL with proper indexing and join strategies.

🔗 **Deep Relations**: Support for nested data loading with independent query control at each level.

## Table of Contents

- [Auto-Join Feature](#auto-join-feature)
- [REST API Usage](#rest-api-usage)
- [GraphQL API Usage](#graphql-api-usage)  
- [Query Parameters](#query-parameters)
- [Filter Operations](#filter-operations)
- [Deep Relations & Use Cases](#deep-relations--use-cases)
- [Complex Examples](#complex-examples)
- [Performance Tips](#performance-tips)

## Auto-Join Feature

The Query Engine automatically creates JOIN queries when you reference related tables. No manual JOIN configuration needed!

### Automatic JOIN on Field Selection

```http
# Select user with role name - AUTO JOIN with roles table
GET /users?fields=id,name,email,role.name,role.permissions

# Select post with author details - AUTO JOIN with users table
GET /posts?fields=title,content,author.name,author.email
```

### Automatic JOIN on Filtering

```http
# Find posts by author name - AUTO JOIN with users table
GET /posts?filter={"author":{"name":{"_contains":"john"}}}

# Find users with admin role - AUTO JOIN with roles table  
GET /users?filter={"role":{"name":{"_eq":"admin"}}}

# Complex multi-level JOIN - AUTO JOIN users -> posts -> comments
GET /users?filter={"posts":{"comments":{"approved":{"_eq":true}}}}
```

### Automatic JOIN on Sorting

```http
# Sort posts by author name - AUTO JOIN with users table
GET /posts?sort=author.name,-createdAt

# Sort orders by customer email - AUTO JOIN with customers table
GET /orders?sort=customer.email,orderDate
```

The engine optimizes these JOINs automatically:
- Uses LEFT JOIN for optional relations
- Uses INNER JOIN when filtering requires the relation to exist
- Avoids duplicate JOINs when same relation is used multiple times
- Creates proper join conditions based on foreign keys

## REST API Usage

All parameters are passed via query string (URL parameters), even for complex queries.

### Basic Queries

```http
# Get all users with basic pagination
GET /users?fields=id,name,email&limit=10&page=1

# Filter users by status
GET /users?filter={"status":{"_eq":"active"}}

# Sort users by creation date (descending)
GET /users?sort=-createdAt&limit=20

# Multiple sort fields
GET /users?sort=name,-createdAt&limit=20
```

### Complex Queries with Filters

Complex filters are passed as URL-encoded JSON in the query string:

```http
# Filter with AND conditions
GET /users?filter={"_and":[{"status":{"_eq":"active"}},{"age":{"_gte":18}}]}&fields=id,name,email&sort=-createdAt

# Filter with OR conditions
GET /users?filter={"_or":[{"role":{"_eq":"admin"}},{"role":{"_eq":"moderator"}}]}

# Nested relation filters
GET /posts?filter={"author":{"name":{"_contains":"john"}},"status":{"_eq":"published"}}&fields=id,title,author.name

# Filter with multiple operators
GET /products?filter={"price":{"_gte":100,"_lte":500},"category":{"_in":["electronics","computers"]}}
```

### URL Encoding

Since complex filters are JSON objects, they must be URL-encoded:

```javascript
// JavaScript example
const filter = {
  "_and": [
    {"status": {"_eq": "active"}},
    {"age": {"_gte": 18}}
  ]
};

const url = `/users?filter=${encodeURIComponent(JSON.stringify(filter))}`;
// Result: /users?filter=%7B%22_and%22%3A%5B%7B%22status%22%3A%7B%22_eq%22%3A%22active%22%7D%7D%2C%7B%22age%22%3A%7B%22_gte%22%3A18%7D%7D%5D%7D
```


## GraphQL API Usage

GraphQL queries also use the same query parameters structure:

```graphql
query {
  users(
    filter: "{\"status\":{\"_eq\":\"active\"}}"
    fields: "id,name,email,role.name"
    sort: "name,-createdAt"
    page: 1
    limit: 10
  ) {
    data
    meta
  }
}
```

Or with GraphQL variables:

```graphql
query GetUsers($filter: String, $fields: String, $sort: String, $page: Int, $limit: Int) {
  users(filter: $filter, fields: $fields, sort: $sort, page: $page, limit: $limit) {
    data
    meta
  }
}
```

Variables:
```json
{
  "filter": "{\"status\":{\"_eq\":\"active\"},\"age\":{\"_gte\":18}}",
  "fields": "id,name,email",
  "sort": "-createdAt",
  "page": 1,
  "limit": 20
}
```

## Query Parameters

All query parameters are passed as URL query strings:

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `filter` | JSON string | Filter conditions | `filter={"status":{"_eq":"active"}}` |
| `fields` | string | Comma-separated field list | `fields=id,name,email` |
| `sort` | string | Comma-separated sort fields (- for DESC) | `sort=name,-createdAt` |
| `page` | number | Page number (1-based, default: 1) | `page=2` |
| `limit` | number | Records per page (default: 10, use 0 for all) | `limit=20` or `limit=0` |
| `meta` | string | Meta information to include | `meta=totalCount,filterCount` |
| `deep` | JSON string | Deep relation options | `deep={"posts":{"limit":5}}` |

## Comparison Operators

### Equal (_eq)

**Usage:** Find records where field equals a value

**REST API:**
```http
GET /users?filter[status][_eq]=active
```

**JavaScript/GraphQL:**
```javascript
filter: { status: { _eq: 'active' } }
```

**SQL Equivalent:**
```sql
WHERE status = 'active'
```

### Not Equal (_neq)

**Usage:** Find records where field does not equal a value

**REST API:**
```http
GET /users?filter[status][_neq]=deleted
```

**JavaScript/GraphQL:**
```javascript
filter: { status: { _neq: 'deleted' } }
```

**SQL Equivalent:**
```sql
WHERE status != 'deleted'
```

### Greater Than (_gt)

**Usage:** Find records where field is greater than a value

**REST API:**
```http
GET /users?filter[age][_gt]=18
```

**JavaScript/GraphQL:**
```javascript
filter: { age: { _gt: 18 } }
```

**SQL Equivalent:**
```sql
WHERE age > 18
```

### Greater Than or Equal (_gte)

**Usage:** Find records where field is greater than or equal to a value

**REST API:**
```http
GET /users?filter[age][_gte]=18
```

**JavaScript/GraphQL:**
```javascript
filter: { age: { _gte: 18 } }
```

**SQL Equivalent:**
```sql
WHERE age >= 18
```

### Less Than (_lt)

**Usage:** Find records where field is less than a value

**REST API:**
```http
GET /products?filter[price][_lt]=100
```

**JavaScript/GraphQL:**
```javascript
filter: { price: { _lt: 100 } }
```

**SQL Equivalent:**
```sql
WHERE price < 100
```

### Less Than or Equal (_lte)

**Usage:** Find records where field is less than or equal to a value

**REST API:**
```http
GET /products?filter[price][_lte]=100
```

**JavaScript/GraphQL:**
```javascript
filter: { price: { _lte: 100 } }
```

**SQL Equivalent:**
```sql
WHERE price <= 100
```

### Between (_between)

**Usage:** Find records where field is between two values (inclusive)

**REST API:**
```http
# Numeric values
GET /products?filter[price][_between]=100,500

# Date values
GET /orders?filter[createdAt][_between]=2024-01-01,2024-12-31
```

**JavaScript/GraphQL:**
```javascript
// Array format
filter: { price: { _between: [100, 500] } }

// String format (comma-separated)
filter: { price: { _between: '100,500' } }

// Date example
filter: { createdAt: { _between: ['2024-01-01', '2024-12-31'] } }
```

**SQL Equivalent:**
```sql
WHERE price BETWEEN 100 AND 500
WHERE createdAt BETWEEN '2024-01-01' AND '2024-12-31'
```

## Text Search Operators

### Contains (_contains)

**Usage:** Find records where field contains a substring (case-insensitive)

**REST API:**
```http
GET /posts?filter[title][_contains]=javascript
```

**JavaScript/GraphQL:**
```javascript
filter: { title: { _contains: 'javascript' } }
```

**SQL Equivalent:**
```sql
-- MySQL
WHERE LOWER(UNACCENT(title)) LIKE '%javascript%'

-- SQLite
WHERE title LIKE '%javascript%'
```

### Starts With (_starts_with)

**Usage:** Find records where field starts with a value

**REST API:**
```http
GET /users?filter[email][_starts_with]=admin
```

**JavaScript/GraphQL:**
```javascript
filter: { email: { _starts_with: 'admin' } }
```

**SQL Equivalent:**
```sql
WHERE email LIKE 'admin%'
```

### Ends With (_ends_with)

**Usage:** Find records where field ends with a value

**REST API:**
```http
GET /files?filter[filename][_ends_with]=.pdf
```

**JavaScript/GraphQL:**
```javascript
filter: { filename: { _ends_with: '.pdf' } }
```

**SQL Equivalent:**
```sql
WHERE filename LIKE '%.pdf'
```

## Logical Operators

### AND (_and)

**Usage:** Combine multiple conditions with AND logic

**REST API:**
```http
GET /users?filter[_and][0][status][_eq]=active&filter[_and][1][age][_gte]=18&filter[_and][2][role][_neq]=guest

# Or using JSON in POST body
POST /users/search
Content-Type: application/json
{
  "filter": {
    "_and": [
      { "status": { "_eq": "active" } },
      { "age": { "_gte": 18 } },
      { "role": { "_neq": "guest" } }
    ]
  }
}
```

**JavaScript/GraphQL:**
```javascript
filter: {
  _and: [
    { status: { _eq: 'active' } },
    { age: { _gte: 18 } },
    { role: { _neq: 'guest' } }
  ]
}
```

**SQL Equivalent:**
```sql
WHERE status = 'active' AND age >= 18 AND role != 'guest'
```

### OR (_or)

**Usage:** Combine multiple conditions with OR logic

**REST API:**
```http
GET /users?filter[_or][0][role][_eq]=admin&filter[_or][1][role][_eq]=moderator

# Or using JSON in POST body
POST /users/search
Content-Type: application/json
{
  "filter": {
    "_or": [
      { "role": { "_eq": "admin" } },
      { "role": { "_eq": "moderator" } }
    ]
  }
}
```

**JavaScript/GraphQL:**
```javascript
filter: {
  _or: [
    { role: { _eq: 'admin' } },
    { role: { _eq: 'moderator' } }
  ]
}
```

**SQL Equivalent:**
```sql
WHERE role = 'admin' OR role = 'moderator'
```

### NOT (_not)

**Usage:** Negate a condition or group of conditions

**REST API:**
```http
GET /users?filter[_not][status][_eq]=deleted

# Complex NOT with JSON
POST /users/search
Content-Type: application/json
{
  "filter": {
    "_not": {
      "_and": [
        { "status": { "_eq": "deleted" } },
        { "role": { "_eq": "guest" } }
      ]
    }
  }
}
```

**JavaScript/GraphQL:**
```javascript
filter: {
  _not: {
    status: { _eq: 'deleted' }
  }
}
```

**SQL Equivalent:**
```sql
WHERE NOT (status = 'deleted')
```

## Array Operators

### In (_in)

**Usage:** Find records where field value is in a list

**REST API:**
```http
GET /posts?filter[category][_in]=tech,business,science
```

**JavaScript/GraphQL:**
```javascript
filter: { category: { _in: ['tech', 'business', 'science'] } }
```

**SQL Equivalent:**
```sql
WHERE category IN ('tech', 'business', 'science')
```

### Not In (_not_in)

**Usage:** Find records where field value is not in a list

**REST API:**
```http
GET /users?filter[status][_not_in]=deleted,suspended
```

**JavaScript/GraphQL:**
```javascript
filter: { status: { _not_in: ['deleted', 'suspended'] } }
```

**SQL Equivalent:**
```sql
WHERE status NOT IN ('deleted', 'suspended')
```

## Null Checks

### Is Null (_is_null)

**Usage:** Check if field is null or not null

**REST API:**
```http
# Find records where deletedAt is null
GET /posts?filter[deletedAt][_is_null]=true

# Find records where deletedAt is not null
GET /posts?filter[deletedAt][_is_null]=false
```

**JavaScript/GraphQL:**
```javascript
// Find records where deletedAt is null
filter: { deletedAt: { _is_null: true } }

// Find records where deletedAt is not null
filter: { deletedAt: { _is_null: false } }
```

**SQL Equivalent:**
```sql
WHERE deletedAt IS NULL
WHERE deletedAt IS NOT NULL
```

## Aggregation Operators

### Count (_count)

**Usage:** Filter based on count of related records

**REST API:**
```http
# Users with more than 5 posts
GET /users?filter[posts][_count][_gt]=5

# Users with exactly 0 posts
GET /users?filter[posts][_count][_eq]=0
```

**JavaScript/GraphQL:**
```javascript
// Users with more than 5 posts
filter: {
  posts: {
    _count: { _gt: 5 }
  }
}

// Users with exactly 0 posts
filter: {
  posts: {
    _count: { _eq: 0 }
  }
}
```

**SQL Equivalent:**
```sql
WHERE (SELECT COUNT(*) FROM posts WHERE posts.userId = users.id) > 5
```

### Sum (_sum)

**Usage:** Filter based on sum of a field in related records

**REST API:**
```http
# Users whose orders total more than $1000
GET /users?filter[orders][_sum][total][_gt]=1000
```

**JavaScript/GraphQL:**
```javascript
// Users whose orders total more than $1000
filter: {
  orders: {
    _sum: {
      total: { _gt: 1000 }
    }
  }
}
```

**SQL Equivalent:**
```sql
WHERE (SELECT SUM(total) FROM orders WHERE orders.userId = users.id) > 1000
```

### Average (_avg)

**Usage:** Filter based on average of a field in related records

**REST API:**
```http
# Products with average rating >= 4.5
GET /products?filter[reviews][_avg][rating][_gte]=4.5
```

**JavaScript/GraphQL:**
```javascript
// Products with average rating >= 4.5
filter: {
  reviews: {
    _avg: {
      rating: { _gte: 4.5 }
    }
  }
}
```

**SQL Equivalent:**
```sql
WHERE (SELECT AVG(rating) FROM reviews WHERE reviews.productId = products.id) >= 4.5
```

### Min/Max (_min, _max)

**Usage:** Filter based on minimum or maximum value in related records

**REST API:**
```http
# Users whose minimum order is at least $50
GET /users?filter[orders][_min][total][_gte]=50

# Products whose maximum price is less than $100
GET /products?filter[variants][_max][price][_lt]=100
```

**JavaScript/GraphQL:**
```javascript
// Users whose minimum order is at least $50
filter: {
  orders: {
    _min: {
      total: { _gte: 50 }
    }
  }
}

// Products whose maximum price is less than $100
filter: {
  variants: {
    _max: {
      price: { _lt: 100 }
    }
  }
}
```

**SQL Equivalent:**
```sql
WHERE (SELECT MIN(total) FROM orders WHERE orders.userId = users.id) >= 50
WHERE (SELECT MAX(price) FROM variants WHERE variants.productId = products.id) < 100
```

## Relations and Joins

### Simple Relation Filter

**Usage:** Filter based on related record fields

**REST API:**
```http
# Find posts where author's name contains 'John'
GET /posts?filter[author][name][_contains]=John
```

**JavaScript/GraphQL:**
```javascript
// Find posts where author's name contains 'John'
filter: {
  author: {
    name: { _contains: 'John' }
  }
}
```

**SQL Equivalent:**
```sql
SELECT posts.* FROM posts
INNER JOIN users ON posts.authorId = users.id
WHERE users.name LIKE '%John%'
```

### Nested Relation Filter

**Usage:** Filter through multiple levels of relations

**REST API:**
```http
# Find users who have posts with comments containing 'excellent'
GET /users?filter[posts][comments][content][_contains]=excellent
```

**JavaScript/GraphQL:**
```javascript
// Find users who have posts with comments containing 'excellent'
filter: {
  posts: {
    comments: {
      content: { _contains: 'excellent' }
    }
  }
}
```

**SQL Equivalent:**
```sql
SELECT DISTINCT users.* FROM users
INNER JOIN posts ON posts.authorId = users.id
INNER JOIN comments ON comments.postId = posts.id
WHERE comments.content LIKE '%excellent%'
```

## Sorting

### Basic Sorting

**REST API:**
```http
# Ascending sort
GET /products?sort=createdAt,name

# Descending sort (prefix with -)
GET /products?sort=-createdAt,-price

# Mixed sorting
GET /products?sort=category,-price,name
```

**JavaScript/GraphQL:**
```javascript
// Ascending sort
sort: ['createdAt', 'name']

// Descending sort (prefix with -)
sort: ['-createdAt', '-price']

// Mixed sorting
sort: ['category', '-price', 'name']
```

**SQL Equivalent:**
```sql
ORDER BY createdAt ASC, name ASC
ORDER BY createdAt DESC, price DESC
ORDER BY category ASC, price DESC, name ASC
```

### Sorting by Related Fields

**REST API:**
```http
# Sort posts by author name
GET /posts?sort=author.name,-createdAt
```

**JavaScript/GraphQL:**
```javascript
// Sort posts by author name
sort: ['author.name', '-createdAt']
```

**SQL Equivalent:**
```sql
SELECT posts.* FROM posts
LEFT JOIN users ON posts.authorId = users.id
ORDER BY users.name ASC, posts.createdAt DESC
```

## Pagination

### Offset-based Pagination

**REST API:**
```http
GET /users?page=2&limit=20
```

**GraphQL:**
```graphql
query {
  users(page: 2, limit: 20) {
    data {
      id
      name
      email
    }
    meta {
      totalCount
      page
      limit
    }
  }
}
```

**JavaScript:**
```javascript
{
  tableName: 'users',
  page: 2,
  limit: 20
}
```

**SQL Equivalent:**
```sql
SELECT * FROM users
LIMIT 20 OFFSET 20
```

### Cursor-based Pagination

**REST API:**
```http
GET /users?cursor=eyJpZCI6MTAwfQ==&limit=20
```

**GraphQL:**
```graphql
query {
  users(cursor: "eyJpZCI6MTAwfQ==", limit: 20) {
    data {
      id
      name
      email
    }
    meta {
      nextCursor
      hasMore
    }
  }
}
```

**JavaScript:**
```javascript
{
  tableName: 'users',
  cursor: 'eyJpZCI6MTAwfQ==',  // base64 encoded cursor
  limit: 20
}
```

## Field Selection

### Select Specific Fields

**REST API:**
```http
# String format
GET /users?fields=id,name,email

# With related fields
GET /users?fields=id,name,posts.title,posts.createdAt
```

**GraphQL:**
```graphql
query {
  users {
    data {
      id
      name
      email
      posts {
        title
        createdAt
      }
    }
  }
}
```

**JavaScript:**
```javascript
// String format
fields: 'id,name,email'

// Array format
fields: ['id', 'name', 'email']

// With related fields
fields: 'id,name,posts.title,posts.createdAt'
```

**SQL Equivalent:**
```sql
SELECT id, name, email FROM users
```

### Select All Fields

**REST API:**
```http
GET /users?fields=*
```

**GraphQL:**
```graphql
query {
  users {
    data {
      id
      name
      email
      createdAt
      updatedAt
      # ... all fields
    }
  }
}
```

**JavaScript:**
```javascript
fields: '*'
```

## Deep Relations & Use Cases

The `deep` parameter enables loading nested relations with independent query control at each level. Each level is a complete query environment with all parameters (fields, filter, sort, limit, page, meta, deep).

### When to Use Deep vs Auto-Join

**Use Auto-Join (default) when:**
- You need a few fields from a related table
- You're filtering/sorting by related fields
- Relations are one-to-one or many-to-one
- You want to avoid N+1 queries

**Use Deep when:**
- You need to load collections (one-to-many) with specific limits
- You want to paginate related records independently
- You need different filters at each relation level
- You need nested collections (e.g., posts → comments)

**⚠️ Important N+1 Warning**: Deep queries execute separate queries for each parent record. Use Deep only when necessary!

### Use Case 1: Blog Dashboard
Load authors with their recent popular posts and top comments:

```http
GET /authors?limit=20&fields=id,name,avatar&deep={"posts":{"fields":"id,title,views,createdAt","filter":{"status":{"_eq":"published"},"views":{"_gt":1000}},"sort":"-views","limit":5,"deep":{"comments":{"fields":"id,content,likes","filter":{"spam":{"_eq":false}},"sort":"-likes","limit":3}}}}
```

**Query breakdown:**
- 1 query: Get 20 authors (explicit limit=20)
- 20 queries: Get top 5 posts per author  
- ~100 queries: Get top 3 comments per post (assuming 5 posts × 20 authors)
- Total: ~121 queries (controlled by explicit root limit)

**Result structure:**
```json
{
  "data": [
    {
      "id": 1,
      "name": "John Doe",
      "avatar": "...",
      "posts": [
        {
          "id": 101,
          "title": "Popular Post",
          "views": 5000,
          "comments": [
            {"id": 1001, "content": "Great!", "likes": 50},
            {"id": 1002, "content": "Helpful", "likes": 30}
          ]
        }
        // ... up to 5 posts
      ]
    }
  ]
}
```

### Use Case 2: Detail Page (Good use of Deep)
Load single customer detail with recent orders and items - N+1 is acceptable for single record:

```http
GET /customers/123?fields=*&deep={"orders":{"fields":"id,orderNumber,total,status,createdAt","filter":{"createdAt":{"_gte":"2024-01-01"}},"sort":"-createdAt","limit":10,"deep":{"items":{"fields":"*,product.name,product.image","limit":100}}}}
```

This is OK because:
- Only 1 customer record (1 query)
- Orders for that customer (1 query for all orders with limit 10)
- Items for each order (up to 10 queries, one per order)
- Total: ~12 queries for complete detail view

Note: Each level of deep executes 1 query per parent record, not 1 query total

### Use Case 3: Social Media Feed
Load user with friends and their recent activities:

```http
GET /users/me?fields=id,name&deep={"friends":{"fields":"id,name,avatar","filter":{"status":{"_eq":"active"}},"limit":20,"deep":{"posts":{"fields":"id,content,createdAt,likesCount","filter":{"createdAt":{"_gte":"2024-08-01"}},"sort":"-createdAt","limit":3}}}}
```

### Use Case 4: Admin Dashboard - User Management
Load users with their roles, permissions, and activity logs:

```http
GET /users?fields=id,email,lastLogin&filter={"status":{"_eq":"active"}}&deep={"role":{"fields":"*,permissions.*"},"activityLogs":{"fields":"action,timestamp,ipAddress","sort":"-timestamp","limit":10},"sessions":{"fields":"device,lastActivity","filter":{"active":{"_eq":true}}}}
```

### Performance Consideration

**⚠️ N+1 Query Warning**: Deep queries create N+1 query problems:
- Fetching 10 users with deep posts: 1 query for users + 10 queries for posts = 11 total
- Fetching 100 products with deep reviews: 1 query for products + 100 queries for reviews = 101 total
- Nested deep multiplies: 10 users → posts → comments = 1 + 10 + (posts per user × 10) queries!

**Why N+1 is Unavoidable for Pagination:**

When you need independent limits per parent record, N+1 queries are the only solution:

```javascript
// ❌ This won't work with _in approach:
// Want: "5 recent posts per user" for 10 users

// SQL with _in would be:
// SELECT * FROM posts WHERE userId IN (1,2,3...10) ORDER BY createdAt DESC LIMIT 50

// Problem: Might return 50 posts from user #1, 0 posts from others
// Cannot guarantee 5 posts per user with single query
```

**The only solution is N+1:**
```http
# ✅ This guarantees 5 posts per user:
GET /users?deep={"posts":{"limit":5,"sort":"-createdAt"}}

# Executes:
# 1 query: SELECT * FROM users  
# N queries: SELECT * FROM posts WHERE userId = ? ORDER BY createdAt DESC LIMIT 5
```

**Best Practices:**
1. Use Auto-Join instead of Deep when you don't need per-record limits
2. Deep is acceptable when viewing a single detail record (e.g., `/users/123`)
3. Deep is necessary when you need "top N records per parent"
4. Always set `limit` on collections to control data size
5. **⚠️ CRITICAL: Always limit root level records when using Deep!**

```javascript
// ✅ DEFAULT: Only 10 users due to default limit=10
GET /users?deep={"posts":{"limit":5}}
// Result: 10 users × 5 posts = ~15 queries (safe)

// ❌ DANGER: Getting ALL users with limit=0 
GET /users?limit=0&deep={"posts":{"limit":5}}
// Result: ALL users × 5 posts = potentially thousands of queries!

// ✅ EXPLICIT: Control exact number of root records
GET /users?limit=20&deep={"posts":{"limit":5}}
// Result: 20 users × 5 posts = ~25 queries (controlled)
```

```javascript
// ❌ Bad - might load thousands of records
deep: { posts: { fields: "*" } }

// ✅ Good - controlled data loading
deep: { posts: { fields: "id,title", limit: 20 } }
```

## Complex Examples

### Example 1: E-commerce Product Search

**REST API:**
```http
# Find active products in specific categories with good ratings
POST /products/search
Content-Type: application/json

{
  "filter": {
    "_and": [
      { "status": { "_eq": "active" } },
      { "category": { "_in": ["electronics", "computers"] } },
      { "price": { "_between": [100, 1000] } },
      {
        "reviews": {
          "_avg": {
            "rating": { "_gte": 4.0 }
          }
        }
      },
      {
        "reviews": {
          "_count": { "_gte": 10 }
        }
      }
    ]
  },
  "fields": "id,name,price,category",
  "sort": ["-reviews.rating", "price"],
  "page": 1,
  "limit": 20,
  "meta": "totalCount,filterCount"
}
```

**GraphQL:**
```graphql
query {
  products(
    filter: {
      _and: [
        { status: { _eq: "active" } }
        { category: { _in: ["electronics", "computers"] } }
        { price: { _between: [100, 1000] } }
        { reviews: { _avg: { rating: { _gte: 4.0 } } } }
        { reviews: { _count: { _gte: 10 } } }
      ]
    }
    sort: ["-reviews.rating", "price"]
    page: 1
    limit: 20
  ) {
    data {
      id
      name
      price
      category
    }
    meta {
      totalCount
      filterCount
    }
  }
}
```

**JavaScript:**
```javascript
// Find active products in specific categories with good ratings
const products = await queryEngine.find({
  tableName: 'products',
  filter: {
    _and: [
      { status: { _eq: 'active' } },
      { category: { _in: ['electronics', 'computers'] } },
      { price: { _between: [100, 1000] } },
      {
        reviews: {
          _avg: {
            rating: { _gte: 4.0 }
          }
        }
      },
      {
        reviews: {
          _count: { _gte: 10 }  // At least 10 reviews
        }
      }
    ]
  },
  fields: 'id,name,price,category',
  sort: ['-reviews.rating', 'price'],
  page: 1,
  limit: 20,
  meta: 'totalCount,filterCount'
});
```

### Example 2: Blog Post Query with Author and Comments

**REST API:**
```http
# Find recent published posts with their authors and approved comments
POST /posts/search
Content-Type: application/json

{
  "filter": {
    "_and": [
      { "status": { "_eq": "published" } },
      { "publishedAt": { "_lte": "2024-08-05T10:00:00.000Z" } },
      {
        "_or": [
          { "tags": { "_contains": "javascript" } },
          { "tags": { "_contains": "typescript" } }
        ]
      }
    ]
  },
  "fields": "id,title,excerpt,publishedAt,author.name,author.avatar",
  "deep": {
    "comments": {
      "fields": ["id", "content", "createdAt", "user.name"],
      "filter": { "approved": { "_eq": true } },
      "sort": ["-createdAt"],
      "limit": 5
    }
  },
  "sort": ["-publishedAt"],
  "limit": 10
}
```

**GraphQL:**
```graphql
query {
  posts(
    filter: {
      _and: [
        { status: { _eq: "published" } }
        { publishedAt: { _lte: "2024-08-05T10:00:00.000Z" } }
        {
          _or: [
            { tags: { _contains: "javascript" } }
            { tags: { _contains: "typescript" } }
          ]
        }
      ]
    }
    sort: ["-publishedAt"]
    limit: 10
  ) {
    data {
      id
      title
      excerpt
      publishedAt
      author {
        name
        avatar
      }
      comments(filter: { approved: { _eq: true } }, sort: ["-createdAt"], limit: 5) {
        id
        content
        createdAt
        user {
          name
        }
      }
    }
  }
}
```

**JavaScript:**
```javascript
// Find recent published posts with their authors and approved comments
const posts = await queryEngine.find({
  tableName: 'posts',
  filter: {
    _and: [
      { status: { _eq: 'published' } },
      { publishedAt: { _lte: new Date() } },
      {
        _or: [
          { tags: { _contains: 'javascript' } },
          { tags: { _contains: 'typescript' } }
        ]
      }
    ]
  },
  fields: 'id,title,excerpt,publishedAt,author.name,author.avatar',
  deep: {
    comments: {
      fields: ['id', 'content', 'createdAt', 'user.name'],
      filter: { approved: { _eq: true } },
      sort: ['-createdAt'],
      limit: 5
    }
  },
  sort: ['-publishedAt'],
  limit: 10
});
```

### Example 3: User Activity Report

```javascript
// Find active users with recent activity
const activeUsers = await queryEngine.find({
  tableName: 'users',
  filter: {
    _and: [
      { status: { _eq: 'active' } },
      { lastLoginAt: { _gte: '2024-01-01' } },
      {
        _or: [
          {
            posts: {
              _count: { _gte: 5 }
            }
          },
          {
            comments: {
              _count: { _gte: 20 }
            }
          }
        ]
      }
    ]
  },
  fields: 'id,name,email,lastLoginAt',
  sort: ['-lastLoginAt'],
  limit: 50,
  meta: '*'  // Include all meta information
});
```

### Example 4: Complex Date Range Query

```javascript
// Find orders from last month with high value
const lastMonth = new Date();
lastMonth.setMonth(lastMonth.getMonth() - 1);
const startOfLastMonth = new Date(lastMonth.getFullYear(), lastMonth.getMonth(), 1);
const endOfLastMonth = new Date(lastMonth.getFullYear(), lastMonth.getMonth() + 1, 0);

const orders = await queryEngine.find({
  tableName: 'orders',
  filter: {
    _and: [
      { createdAt: { _between: [startOfLastMonth, endOfLastMonth] } },
      { total: { _gte: 500 } },
      { status: { _not_in: ['cancelled', 'refunded'] } },
      {
        customer: {
          vipStatus: { _eq: true }
        }
      }
    ]
  },
  fields: 'id,orderNumber,total,createdAt,customer.name,customer.email',
  deep: {
    items: {
      fields: ['productId', 'quantity', 'price'],
      deep: {
        product: {
          fields: ['name', 'sku']
        }
      }
    }
  },
  sort: ['-total'],
  limit: 100
});
```

## Performance Tips

1. **Use Field Selection**: Only select fields you need to reduce data transfer
2. **Add Indexes**: Ensure database indexes exist for frequently filtered fields
3. **Limit Deep Relations**: Avoid deeply nested queries when possible
4. **Use Aggregations Wisely**: Aggregation queries can be expensive on large datasets
5. **Paginate Results**: Always use pagination for large result sets

## Error Handling

The Query Engine validates input and provides clear error messages:

```javascript
try {
  const result = await queryEngine.find({
    tableName: 'users',
    filter: { age: { _between: '18,65' } }
  });
} catch (error) {
  console.error(error.message);
  // e.g., "_between operator requires valid numeric values for field type int"
}
```

## TypeScript Support

The Query Engine is fully typed. Example interfaces:

```typescript
interface QueryOptions {
  tableName: string;
  filter?: FilterCondition;
  fields?: string | string[];
  sort?: string[];
  page?: number;
  limit?: number;
  meta?: string;
  deep?: DeepRelationOptions;
}

interface FilterCondition {
  [field: string]: 
    | any // Direct value (implicit _eq)
    | { [operator: string]: any }
    | FilterCondition; // Nested conditions
}
```