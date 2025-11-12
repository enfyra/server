# Nested Relations: Querying with Relations

## Overview
Enfyra supports powerful nested querying for relations. Instead of making multiple queries to different tables, you can fetch and filter data across relations in a single query using **nested fields** and **nested filters**.

## Key Concepts

### When to Use Nested Queries
✅ **Use nested queries when:**
- You need data from related tables (e.g., "route with its roles")
- You want to filter by related table fields (e.g., "routes that have role Admin")
- You need multiple levels of relations (e.g., "route → permissions → role")

❌ **Don't make separate queries when:**
- You can use nested fields to get all data in one query
- Relations are already available through the table structure

## 1. Nested Field Selection

### Syntax: Dot Notation
Use `relation.field` or `relation.*` to select fields from related tables.

**Format:**
```
fields="baseField1,baseField2,relation.field1,relation.field2"
```

### Examples

**Example 1: Get route with role names (One-to-Many)**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": { "id": { "_eq": 20 } },
  "fields": "id,path,roles.name,roles.id"
}
```
Returns:
```json
{
  "data": [{
    "id": 20,
    "path": "/api/users",
    "roles": [
      { "id": 1, "name": "Admin" },
      { "id": 2, "name": "User" }
    ]
  }]
}
```

**Example 2: Get all fields from a relation using wildcard**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": { "id": { "_eq": 20 } },
  "fields": "id,path,roles.*"
}
```
Returns all fields from the `roles` relation.

**Example 3: Multiple relations at once**
```json
{
  "table": "route_definition",
  "operation": "find",
  "fields": "id,path,roles.name,handlers.method.name,hooks.name"
}
```

**Example 4: Deep nesting (3+ levels)**
```json
{
  "table": "route_definition",
  "operation": "find",
  "fields": "id,path,routePermissions.role.name,routePermissions.role.permissions.*"
}
```

## 2. Nested Filtering

### Syntax: Object Notation
Use nested objects where the relation name is the key, containing filter conditions.

**Format:**
```json
{
  "relationName": {
    "field": { "operator": "value" }
  }
}
```

### Examples

**Example 1: Find routes that have Admin role**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "roles": {
      "name": { "_eq": "Admin" }
    }
  },
  "fields": "id,path"
}
```

**Example 2: Find routes with specific role ID**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "roles": {
      "id": { "_eq": 5 }
    }
  },
  "fields": "id,path,roles.name"
}
```

**Example 3: Complex nested filter with multiple conditions**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "_and": [
      { "path": { "_starts_with": "/api" } },
      {
        "roles": {
          "name": { "_in": ["Admin", "Moderator"] }
        }
      }
    ]
  },
  "fields": "id,path,roles.*"
}
```

**Example 4: Filter with OR on nested relations**
```json
{
  "table": "user_definition",
  "operation": "find",
  "where": {
    "_or": [
      { "roles": { "name": { "_eq": "Admin" } } },
      { "roles": { "name": { "_eq": "Moderator" } } }
    ]
  },
  "fields": "id,name,roles.name"
}
```

**Example 5: Deep nested filter (2+ levels)**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "routePermissions": {
      "role": {
        "name": { "_eq": "Admin" }
      }
    }
  },
  "fields": "id,path"
}
```

## 3. Combining Nested Fields and Filters

You can use nested fields and nested filters together in the same query.

**Example: Find routes with Admin role and show role details**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "roles": {
      "name": { "_eq": "Admin" }
    }
  },
  "fields": "id,path,method,roles.id,roles.name,roles.description",
  "limit": 0
}
```

## 4. Common Use Cases

### Use Case 1: "What roles can access route ID 20?"
**Single query solution:**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": { "id": { "_eq": 20 } },
  "fields": "id,path,roles.id,roles.name",
  "limit": 1
}
```

### Use Case 2: "List all routes accessible by role ID 5"
**Single query solution:**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "roles": {
      "id": { "_eq": 5 }
    }
  },
  "fields": "id,path,method,roles.name",
  "limit": 0
}
```

### Use Case 3: "Get user with their posts and comments"
**Single query solution:**
```json
{
  "table": "user_definition",
  "operation": "find",
  "where": { "id": { "_eq": 10 } },
  "fields": "id,name,posts.title,posts.createdAt,comments.content",
  "limit": 1
}
```

### Use Case 4: "Count routes that require Admin role"
**Single query solution:**
```json
{
  "table": "route_definition",
  "operation": "find",
  "where": {
    "roles": {
      "name": { "_eq": "Admin" }
    }
  },
  "fields": "id",
  "limit": 0
}
```
Then count the results with `result.count` or `result.data.length`.

## 5. Available Filter Operators

```
_eq           - Equal
_neq          - Not equal
_gt           - Greater than
_gte          - Greater than or equal
_lt           - Less than
_lte          - Less than or equal
_in           - In array [val1, val2, ...]
_not_in       - Not in array
_contains     - String contains (case-insensitive)
_starts_with  - String starts with
_ends_with    - String ends with
_between      - Between [min, max]
_is_null      - Is NULL (use true/false)
_is_not_null  - Is not NULL (use true/false)

_and          - Logical AND (array of conditions)
_or           - Logical OR (array of conditions)
_not          - Logical NOT
```

## 6. Performance Tips

1. **Select only needed fields**: Use specific fields instead of `*` when possible
   - ✅ `fields="id,name,roles.name"`
   - ❌ `fields="*,roles.*"` (if you only need name)

2. **Use limit=0 wisely**: Only use when you truly need all records
   - For counts: `fields="id", limit=0` is efficient
   - For large datasets: Use pagination with limit

3. **Avoid redundant queries**:
   - ❌ Bad: Query route → Query role_definition → Query route_permission
   - ✅ Good: Single query with nested fields

4. **Filter at database level**: Use nested filters instead of fetching all and filtering in code

## 7. Troubleshooting

**Problem**: "I don't know what relations are available"
**Solution**: Use `get_table_details` to see the `relations` field in the response.

**Problem**: "Nested field returns null"
**Solution**: The relation might not exist for that record, or the field name is incorrect. Check table details.

**Problem**: "Too many results"
**Solution**: Add filters and use appropriate `limit`. For "exists" checks, `limit=1` is efficient.

## 8. Quick Reference

| Task | Solution |
|------|----------|
| Get related data | Use nested fields: `relation.field` |
| Filter by relation | Use nested where: `{ relation: { field: { _eq: value } } }` |
| Get all relation fields | Use wildcard: `relation.*` |
| Multiple relations | Comma-separate: `rel1.field,rel2.field` |
| Deep nesting | Chain dots: `rel1.rel2.field` |
| Check relation exists | Filter: `{ relation: { id: { _is_not_null: true } } }` |

## Remember
- **Always prefer ONE nested query over multiple separate queries**
- Nested fields and filters work across all relation types (M2O, O2M, M2M, O2O)
- The system automatically handles JOINs and subqueries for you
- Check `get_table_details` first to understand available relations
