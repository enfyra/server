import { InsertOptions, UpdateOptions, DeleteOptions, CountOptions, WhereCondition } from '../../shared/types/query-builder.types';

/**
 * MongoDB CRUD Functions
 * Pure functions for MongoDB database operations
 */

/**
 * Convert unified WHERE conditions to MongoDB filter
 */
function whereToMongoFilter(conditions: WhereCondition[]): any {
  const { ObjectId } = require('mongodb');
  const filter: any = {};

  for (const condition of conditions) {
    // MongoDB field name normalization:
    // - 'id' -> '_id' (MongoDB primary key)
    // - 'sourceTableId' -> 'sourceTable' (relation field in relation_definition)
    // - 'targetTableId' -> 'targetTable' (relation field in relation_definition)
    let fieldName = condition.field;
    let fieldValue = condition.value;

    if (fieldName === 'id') {
      fieldName = '_id';
      // Convert string ID to ObjectId for MongoDB
      if (typeof fieldValue === 'string' && fieldValue.length === 24) {
        fieldValue = new ObjectId(fieldValue);
      }
    } else if (fieldName === 'sourceTableId') {
      fieldName = 'sourceTable';
      // Convert string ID to ObjectId for MongoDB relation fields
      if (typeof fieldValue === 'string' && fieldValue.length === 24) {
        fieldValue = new ObjectId(fieldValue);
      }
    } else if (fieldName === 'targetTableId') {
      fieldName = 'targetTable';
      // Convert string ID to ObjectId for MongoDB relation fields
      if (typeof fieldValue === 'string' && fieldValue.length === 24) {
        fieldValue = new ObjectId(fieldValue);
      }
    }

    switch (condition.operator) {
      case '=':
        filter[fieldName] = fieldValue;
        break;
      case '!=':
        filter[fieldName] = { $ne: fieldValue };
        break;
      case '>':
        filter[fieldName] = { $gt: fieldValue };
        break;
      case '<':
        filter[fieldName] = { $lt: fieldValue };
        break;
      case '>=':
        filter[fieldName] = { $gte: fieldValue };
        break;
      case '<=':
        filter[fieldName] = { $lte: fieldValue };
        break;
      case 'like':
        filter[fieldName] = { $regex: fieldValue.replace(/%/g, '.*') };
        break;
      case 'in':
        filter[fieldName] = { $in: fieldValue };
        break;
      case 'not in':
        filter[fieldName] = { $nin: fieldValue };
        break;
      case 'is null':
        filter[fieldName] = null;
        break;
      case 'is not null':
        filter[fieldName] = { $ne: null };
        break;
    }
  }

  return filter;
}

/**
 * Insert records (one or multiple) - MongoDB
 */
export async function mongoInsert(
  options: InsertOptions,
  mongoService: any
): Promise<any> {
  const collection = mongoService.collection(options.table);
  if (Array.isArray(options.data)) {
    // Process nested relations for each record
    const processedData = await Promise.all(
      options.data.map((record: any) => mongoService.processNestedRelations(options.table, record))
    );

    // Apply timestamps hook
    const dataWithTimestamps = mongoService.applyTimestamps(processedData);
    const result = await collection.insertMany(dataWithTimestamps as any[]);
    return Object.values(result.insertedIds).map((id, idx) => ({
      id: id.toString(),
      ...(dataWithTimestamps as any[])[idx],
    }));
  } else {
    return mongoService.insertOne(options.table, options.data);
  }
}

/**
 * Update records - MongoDB
 */
export async function mongoUpdate(
  options: UpdateOptions,
  mongoService: any
): Promise<any> {
  // Process nested relations first
  const dataWithRelations = await mongoService.processNestedRelations(options.table, options.data);

  // Apply update timestamp
  const dataWithTimestamp = mongoService.applyUpdateTimestamp(dataWithRelations);

  const filter = whereToMongoFilter(options.where);
  const collection = mongoService.collection(options.table);
  await collection.updateMany(filter, { $set: dataWithTimestamp });
  return collection.find(filter).toArray();
}

/**
 * Delete records - MongoDB
 */
export async function mongoDelete(
  options: DeleteOptions,
  mongoService: any
): Promise<number> {
  const filter = whereToMongoFilter(options.where);
  const collection = mongoService.collection(options.table);
  const result = await collection.deleteMany(filter);
  return result.deletedCount;
}

/**
 * Count records - MongoDB
 */
export async function mongoCount(
  options: CountOptions,
  mongoService: any
): Promise<number> {
  const filter = options.where ? whereToMongoFilter(options.where) : {};
  return mongoService.count(options.table, filter);
}

/**
 * Execute transaction - MongoDB
 */
export async function mongoTransaction<T>(
  callback: (trx: any) => Promise<T>,
  mongoService: any
): Promise<T> {
  const session = mongoService.getClient().startSession();
  try {
    await session.startTransaction();
    const result = await callback(session);
    await session.commitTransaction();
    return result;
  } catch (error) {
    await session.abortTransaction();
    throw error;
  } finally {
    await session.endSession();
  }
}

/**
 * Find one by ID - MongoDB
 */
export async function mongoFindById(
  table: string,
  id: any,
  mongoService: any
): Promise<any> {
  return mongoService.findOne(table, { _id: id });
}

/**
 * Find one by conditions - MongoDB
 */
export async function mongoFindOneWhere(
  table: string,
  where: Record<string, any>,
  mongoService: any
): Promise<any> {
  // Normalize 'id' to '_id' and convert to ObjectId for MongoDB
  const { ObjectId } = require('mongodb');
  const normalizedWhere: any = {};

  for (const [key, value] of Object.entries(where)) {
    if (key === 'id' || key === '_id') {
      normalizedWhere._id = typeof value === 'string' ? new ObjectId(value) : value;
    } else {
      normalizedWhere[key] = value;
    }
  }

  return mongoService.findOne(table, normalizedWhere);
}

/**
 * Find many by conditions - MongoDB
 */
export async function mongoFindWhere(
  table: string,
  where: Record<string, any>,
  mongoService: any
): Promise<any[]> {
  // Normalize 'id' to '_id' and convert to ObjectId for MongoDB
  const { ObjectId } = require('mongodb');
  const normalizedWhere: any = {};

  for (const [key, value] of Object.entries(where)) {
    if (key === 'id' || key === '_id') {
      normalizedWhere._id = typeof value === 'string' ? new ObjectId(value) : value;
    } else {
      normalizedWhere[key] = value;
    }
  }

  const collection = mongoService.collection(table);
  const results = await collection.find(normalizedWhere).toArray();
  return results.map((doc: any) => mongoService['mapDocument'](doc));
}

/**
 * Insert one and return with ID - MongoDB
 */
export async function mongoInsertAndGet(
  table: string,
  data: any,
  mongoService: any
): Promise<any> {
  return mongoService.insertOne(table, data);
}

/**
 * Update by ID - MongoDB
 */
export async function mongoUpdateById(
  table: string,
  id: any,
  data: any,
  mongoService: any
): Promise<any> {
  return mongoService.updateOne(table, id, data);
}

/**
 * Delete by ID - MongoDB
 */
export async function mongoDeleteById(
  table: string,
  id: any,
  mongoService: any
): Promise<number> {
  const deleted = await mongoService.deleteOne(table, id);
  return deleted ? 1 : 0;
}

/**
 * Execute raw command - MongoDB
 */
export async function mongoRaw(
  query: string | any,
  mongoService: any
): Promise<any> {
  // MongoDB: execute command
  const db = mongoService.getDb();
  if (typeof query === 'string') {
    // If string, treat as simple ping or eval
    if (query.toLowerCase().includes('select 1')) {
      return db.command({ ping: 1 });
    }
    throw new Error('String queries not supported for MongoDB. Use db.command() object instead.');
  }
  return db.command(query);
}
