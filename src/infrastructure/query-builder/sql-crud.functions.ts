import { Knex } from 'knex';
import { InsertOptions, UpdateOptions, DeleteOptions, CountOptions, WhereCondition } from '../../shared/types/query-builder.types';

/**
 * SQL CRUD Functions
 * Pure functions for SQL database operations using Knex
 */

/**
 * Apply WHERE conditions to Knex query
 */
function applyWhereToKnex(query: any, conditions: WhereCondition[]): any {
  for (const condition of conditions) {
    switch (condition.operator) {
      case '=':
        query = query.where(condition.field, '=', condition.value);
        break;
      case '!=':
        query = query.where(condition.field, '!=', condition.value);
        break;
      case '>':
        query = query.where(condition.field, '>', condition.value);
        break;
      case '<':
        query = query.where(condition.field, '<', condition.value);
        break;
      case '>=':
        query = query.where(condition.field, '>=', condition.value);
        break;
      case '<=':
        query = query.where(condition.field, '<=', condition.value);
        break;
      case 'like':
        query = query.where(condition.field, 'like', condition.value);
        break;
      case 'in':
        query = query.whereIn(condition.field, condition.value);
        break;
      case 'not in':
        query = query.whereNotIn(condition.field, condition.value);
        break;
      case 'is null':
        query = query.whereNull(condition.field);
        break;
      case 'is not null':
        query = query.whereNotNull(condition.field);
        break;
    }
  }
  return query;
}

/**
 * Insert records (one or multiple) - SQL
 */
export async function sqlInsert(
  options: InsertOptions,
  knexService: any
): Promise<any> {
  // SQL: Use KnexService.insertWithCascade for automatic relation handling
  if (Array.isArray(options.data)) {
    // Handle multiple records
    const results = [];
    for (const record of options.data) {
      const result = await knexService.insertWithCascade(options.table, record);
      results.push(result);
    }
    return results;
  } else {
    // Handle single record
    return await knexService.insertWithCascade(options.table, options.data);
  }
}

/**
 * Update records - SQL
 */
export async function sqlUpdate(
  options: UpdateOptions,
  knexService: any
): Promise<any> {
  // SQL: Use KnexService.updateWithCascade for automatic relation handling
  const knex = knexService.getKnex();
  let query: any = knex(options.table);

  if (options.where.length > 0) {
    query = applyWhereToKnex(query, options.where);
  }

  // Get records to update first
  const recordsToUpdate = await query.clone();

  // Update each record with cascade
  for (const record of recordsToUpdate) {
    await knexService.updateWithCascade(options.table, record.id, options.data);
  }

  if (options.returning) {
    return query.returning(options.returning);
  }

  return { affected: recordsToUpdate.length };
}

/**
 * Delete records - SQL
 */
export async function sqlDelete(
  options: DeleteOptions,
  knexService: any
): Promise<number> {
  const knex = knexService.getKnex();
  let query: any = knex(options.table);

  if (options.where.length > 0) {
    query = applyWhereToKnex(query, options.where);
  }

  return query.delete();
}

/**
 * Count records - SQL
 */
export async function sqlCount(
  options: CountOptions,
  knexService: any
): Promise<number> {
  const knex = knexService.getKnex();
  let query: any = knex(options.table);

  if (options.where && options.where.length > 0) {
    query = applyWhereToKnex(query, options.where);
  }

  const result = await query.count('* as count').first();
  return Number(result?.count || 0);
}

/**
 * Execute transaction - SQL
 */
export async function sqlTransaction<T>(
  callback: (trx: any) => Promise<T>,
  knexService: any
): Promise<T> {
  const knex = knexService.getKnex();
  return knex.transaction(callback);
}

/**
 * Find one by ID - SQL
 */
export async function sqlFindById(
  table: string,
  id: any,
  knexService: any
): Promise<any> {
  const knex = knexService.getKnex();
  return knex(table).where('id', id).first();
}

/**
 * Find one by conditions - SQL
 */
export async function sqlFindOneWhere(
  table: string,
  where: Record<string, any>,
  knexService: any
): Promise<any> {
  const knex = knexService.getKnex();
  return knex(table).where(where).first();
}

/**
 * Find many by conditions - SQL
 */
export async function sqlFindWhere(
  table: string,
  where: Record<string, any>,
  knexService: any
): Promise<any[]> {
  const knex = knexService.getKnex();
  return knex(table).where(where);
}

/**
 * Insert one and return with ID - SQL
 */
export async function sqlInsertAndGet(
  table: string,
  data: any,
  knexService: any
): Promise<any> {
  // Use insertWithCascade for M2M/O2M relation handling
  const insertedId = await knexService.insertWithCascade(table, data);

  const knex = knexService.getKnex();
  const recordId = insertedId || data.id;

  // Query back the inserted record
  return knex(table).where('id', recordId).first();
}

/**
 * Update by ID - SQL
 */
export async function sqlUpdateById(
  table: string,
  id: any,
  data: any,
  knexService: any
): Promise<any> {
  // SQL: Use KnexService.updateWithCascade for automatic relation handling
  await knexService.updateWithCascade(table, id, data);
  const knex = knexService.getKnex();
  return knex(table).where('id', id).first();
}

/**
 * Delete by ID - SQL
 */
export async function sqlDeleteById(
  table: string,
  id: any,
  knexService: any
): Promise<number> {
  const knex = knexService.getKnex();
  return knex(table).where('id', id).delete();
}

/**
 * Execute raw query - SQL
 */
export async function sqlRaw(
  query: string | any,
  bindings: any | undefined,
  knexService: any
): Promise<any> {
  // SQL: execute raw query
  const knex = knexService.getKnex();
  return knex.raw(query, bindings);
}
