# Enfyra Relation Behavior

## Overview
Enfyra handles relations similar to TypeORM behavior. When creating or updating records, you can pass relation data as objects containing an `id` field. The system automatically determines whether to update existing records or create new ones based on whether the `id` field is present.

## Relation Types and Behavior

### Many-to-One (M2O) Relations

**Behavior:**
- If you pass an object with an `id` field: The system extracts the `id` and uses it as the foreign key value
- If you pass a number or string: The system uses it directly as the foreign key value
- If you pass `null`: The system sets the foreign key to `null`

**Example:**
```json
{
  "name": "Product",
  "category": { "id": 1 }  // Links to category with id=1
}
// OR
{
  "name": "Product",
  "category": 1  // Also links to category with id=1
}
// OR
{
  "name": "Product",
  "category": null  // No category
}
```

### One-to-One (O2O) Relations

**Behavior:**
- If you pass an object with an `id` field: The system sets the foreign key to point to that existing record
- If you pass an object without an `id` field: The system creates a new related entity and sets the foreign key to point to it (cascade create)
- Only the owner side (non-inverse) handles the foreign key. Inverse side is ignored

**Example:**
```json
{
  "name": "User",
  "profile": { "id": 5 }  // Links to existing profile
}
// OR
{
  "name": "User",
  "profile": { "bio": "Hello", "avatar": "url" }  // Creates new profile and links to it
}
```

### Many-to-Many (M2M) Relations

**Behavior:**
- Pass an array of objects/ids: The system extracts IDs and synchronizes the junction table
- Items in the array can be objects with `id` field or just IDs (numbers/strings)
- The system clears existing junction records and inserts new ones

**Example:**
```json
{
  "name": "Post",
  "tags": [
    { "id": 1 },
    { "id": 2 },
    3  // Can mix objects and IDs
  ]
}
```

### One-to-Many (O2M) Relations

**Behavior:**
- If an item has an `id` field: The system updates that item's foreign key to point to the parent (UPDATE operation)
- If an item does not have an `id` field: The system creates a new item with the foreign key pointing to the parent (CREATE operation)
- Items that are no longer in the array but were previously linked: The system sets their foreign key to `null` (removes from relation)

**Example:**
```json
{
  "name": "Order",
  "items": [
    { "id": 10, "quantity": 5 },  // Updates existing item id=10
    { "productId": 1, "quantity": 2 }  // Creates new item
  ]
}
// If items with id=11, id=12 were previously linked but not in this array,
// their orderId will be set to null
```

## Important Notes

1. **ID Field Naming:**
   - For SQL databases (MySQL, PostgreSQL, SQLite): Use `id` field
   - For MongoDB: Use `_id` field (not `id`)
   - Always check the database type using `get_hint` tool before performing operations

2. **Cascade Operations:**
   - O2O and O2M relations support cascade create (creating related entities automatically)
   - M2M relations are synchronized through junction tables
   - Updates to relations happen after the main record is created/updated

3. **Relation Property Names:**
   - Use the relation `propertyName` as defined in the table metadata
   - The system automatically transforms relation objects to foreign keys
   - Relation properties are removed from the data before insertion/update

4. **Best Practices:**
   - Always read hints using `get_hint` tool before performing database operations
   - Check the database type to know whether to use `id` or `_id`
   - Use `get_table_details` to understand relation structure before creating/updating records
   - For O2M relations, be aware that items not in the array will have their FK set to null




