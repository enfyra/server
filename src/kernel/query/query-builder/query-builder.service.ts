import { KnexService } from '../../../engines/knex';
import { MongoService, normalizeMongoDocument } from '../../../engines/mongo';
import {
  DatabaseType,
  WhereCondition,
  InsertOptions,
  UpdateOptions,
  DeleteOptions,
  CountOptions,
  AggregateQuery,
} from '../../../shared/types/query-builder.types';
import { MongoQueryExecutor } from './executors/mongo-query-executor';
import { SqlQueryExecutor } from './executors/sql-query-executor';
import { QueryPlanner } from '../query-dsl/query-planner';
import {
  DatabaseConfigService,
  RuntimeMetricsCollectorService,
} from '../../../shared/services';
import type { QueryMetricContext } from '../../../shared/types';
import type { Cradle } from '../../../container';
import { DebugTrace } from '../../../shared/utils/debug-trace.util';
import { whereToMongoFilter } from './utils/mongo/filter-builder';
import { applyWhereToKnex as applyWhereToKnexComplete } from './utils/sql/sql-where-builder';
import { IQueryBuilder } from '../../../domain/shared/interfaces/query-builder.interface';

import { ObjectId } from 'mongodb';

export class QueryBuilderService implements IQueryBuilder {
  private readonly knexService: KnexService;
  private readonly mongoService: any;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly runtimeMetricsCollectorService?: RuntimeMetricsCollectorService;
  private readonly lazyRef: Cradle;
  private dbType: DatabaseType;
  private debugLog: any[] = [];

  constructor(deps: {
    knexService?: KnexService;
    mongoService?: MongoService;
    databaseConfigService: DatabaseConfigService;
    runtimeMetricsCollectorService?: RuntimeMetricsCollectorService;
    lazyRef: Cradle;
  }) {
    this.knexService = deps.knexService as KnexService;
    this.mongoService = deps.mongoService as MongoService;
    this.databaseConfigService = deps.databaseConfigService;
    this.runtimeMetricsCollectorService = deps.runtimeMetricsCollectorService;
    this.lazyRef = deps.lazyRef;
    this.dbType = this.databaseConfigService.getDbType();
  }

  async runWithPolicy<T>(
    policyCheck: (
      tableName: string,
      operation: 'create' | 'update' | 'delete',
      data: any,
    ) => Promise<void>,
    callback: () => Promise<T>,
  ): Promise<T> {
    if (this.knexService) {
      return this.knexService.runWithPolicy(policyCheck, callback);
    }
    if (this.mongoService) {
      return this.mongoService.runWithPolicy(policyCheck, callback);
    }
    return callback();
  }

  async runWithFieldPermissionCheck<T>(
    checker: (
      tableName: string,
      action: 'create' | 'update',
      data: any,
    ) => Promise<void>,
    callback: () => Promise<T>,
  ): Promise<T> {
    if (this.knexService) {
      return this.knexService.runWithFieldPermissionCheck(checker, callback);
    }
    if (this.mongoService) {
      return this.mongoService.runWithFieldPermissionCheck(checker, callback);
    }
    return callback();
  }

  async runWithTelemetryContext<T>(
    context: QueryMetricContext,
    callback: () => Promise<T>,
  ): Promise<T> {
    if (!this.runtimeMetricsCollectorService) return await callback();
    return await this.runtimeMetricsCollectorService.runWithQueryContext(
      context,
      callback,
    );
  }

  getDbType(): DatabaseType {
    return this.dbType;
  }

  private safeObjectId(value: any): any {
    if (!ObjectId) return value;
    if (typeof value === 'string') {
      try {
        return new ObjectId(value);
      } catch (err) {
        return value;
      }
    }
    return value;
  }

  private async getMetadataForQuery(): Promise<any> {
    const fallback = { tables: new Map(), tablesList: [] };
    const metadataCacheService = this.lazyRef.metadataCacheService;
    if (!metadataCacheService) return fallback;
    if (metadataCacheService.isLoaded?.()) {
      return (await metadataCacheService.getMetadata?.()) ?? fallback;
    }
    return metadataCacheService.getDirectMetadata?.() ?? fallback;
  }

  private async buildMongoFilter(
    where: WhereCondition[],
    table?: string,
  ): Promise<any> {
    const metadata = await this.getMetadataForQuery();
    const filter = whereToMongoFilter(metadata, where, table, this.dbType);
    if (filter._id !== undefined) {
      filter._id = this.safeObjectId(filter._id);
    }
    return filter;
  }

  private async applyWhereToKnex(
    query: any,
    conditions: WhereCondition[],
    table?: string,
  ): Promise<any> {
    const metadata = await this.getMetadataForQuery();
    return applyWhereToKnexComplete(
      query,
      conditions,
      table ?? '',
      metadata,
      this.dbType as 'postgres' | 'mysql' | 'sqlite',
    );
  }

  async insertWithOptions(options: InsertOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      const collection = this.mongoService.collection(options.table);
      if (Array.isArray(options.data)) {
        const processedData = await Promise.all(
          options.data.map((record) =>
            this.mongoService.processNestedRelations(options.table, record),
          ),
        );

        const dataWithTimestamps =
          this.mongoService.applyTimestamps(processedData);
        const result = await collection.insertMany(dataWithTimestamps as any[]);
        return Object.values(result.insertedIds).map((id, idx) => ({
          id: (id as any).toString(),
          ...(dataWithTimestamps as any[])[idx],
        }));
      } else {
        return this.mongoService.insertOne(options.table, options.data);
      }
    }

    if (Array.isArray(options.data)) {
      const results = [];
      for (const record of options.data) {
        const result = await this.knexService.insertWithCascade(
          options.table,
          record,
        );
        results.push(result);
      }
      return results;
    } else {
      return await this.knexService.insertWithCascade(
        options.table,
        options.data,
      );
    }
  }

  async select(options: {
    tableName: string;
    fields?: string | string[];
    filter?: any;
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string;
    deep?: Record<string, any>;
    debugMode?: boolean;
    debugLog?: any[];
    debugTrace?: DebugTrace;
    pipeline?: any[];
    aggregate?: AggregateQuery;
    maxQueryDepth?: number;
  }): Promise<any> {
    const selectStart = performance.now();
    const metadata = await this.getMetadataForQuery();

    const planStart = performance.now();
    const planner = new QueryPlanner();
    const plan = planner.plan({
      tableName: options.tableName,
      fields: options.fields,
      filter: options.filter,
      sort: options.sort,
      page: options.page,
      limit: options.limit,
      meta: options.meta,
      metadata,
      dbType: this.dbType as any,
    });
    const trace = options.debugTrace;
    if (trace) {
      trace.dur('qb_planner', planStart, { table: options.tableName });
      trace.setPlan(this.sanitizePlan(plan));
    }

    if (this.dbType === 'mongodb') {
      const executor = new MongoQueryExecutor(this.mongoService);
      return executor.execute({
        ...options,
        metadata,
        dbType: this.dbType,
        plan,
        aggregate: options.aggregate,
      });
    }

    const executor = new SqlQueryExecutor(
      this.knexService.getKnex(),
      this.dbType as 'postgres' | 'mysql' | 'sqlite',
      this.knexService,
      options.maxQueryDepth,
    );
    const result = await executor.execute({ ...options, metadata, plan });
    if (trace) {
      trace.dur('qb_total_select', selectStart, { table: options.tableName });
    }
    return result;
  }

  private sanitizePlan(plan: any): any {
    return {
      rawFields: plan.rawFields,
      hasRelationFilters: plan.hasRelationFilters,
      hasRelationSort: plan.hasRelationSort,
      joins: plan.joins?.length ?? 0,
      sortItems: plan.sortItems,
      limit: plan.limit,
      offset: plan.offset,
      page: plan.page,
      isSimpleQuery:
        plan.joins?.length === 0 &&
        !plan.hasRelationFilters &&
        !plan.hasRelationSort,
    };
  }

  async updateWithOptions(options: UpdateOptions): Promise<any> {
    if (this.dbType === 'mongodb') {
      const dataWithRelations = await this.mongoService.processNestedRelations(
        options.table,
        options.data,
      );
      const dataWithTimestamp =
        this.mongoService.applyUpdateTimestamp(dataWithRelations);

      const filter = await this.buildMongoFilter(options.where, options.table);

      const collection = this.mongoService.collection(options.table);
      await collection.updateMany(filter, { $set: dataWithTimestamp });
      const results = await collection.find(filter).toArray();
      return results;
    }

    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);

    if (options.where.length > 0) {
      query = await this.applyWhereToKnex(query, options.where, options.table);
    }

    const recordsToUpdate = await query;

    for (const record of recordsToUpdate) {
      await this.knexService.updateWithCascade(
        options.table,
        record.id,
        options.data,
      );
    }

    if (options.returning) {
      const returnQuery = knex(options.table);
      if (options.where.length > 0) {
        await this.applyWhereToKnex(returnQuery, options.where, options.table);
      }
      return await returnQuery.select(options.returning);
    }

    return { affected: recordsToUpdate.length };
  }

  async deleteWithOptions(options: DeleteOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      const filter = await this.buildMongoFilter(options.where, options.table);

      const collection = this.mongoService.collection(options.table);
      const result = await collection.deleteMany(filter);
      return result.deletedCount;
    }

    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);

    if (options.where.length > 0) {
      query = await this.applyWhereToKnex(query, options.where, options.table);
    }

    return await query.delete();
  }

  async count(options: CountOptions): Promise<number> {
    if (this.dbType === 'mongodb') {
      const filter = options.where
        ? await this.buildMongoFilter(options.where, options.table)
        : {};
      return this.mongoService.count(options.table, filter);
    }

    const knex = this.knexService.getKnex();
    let query: any = knex(options.table);

    if (options.where && options.where.length > 0) {
      query = await this.applyWhereToKnex(query, options.where, options.table);
    }

    const result = await query.count('* as count').first();
    return Number(result?.count || 0);
  }

  private whereToFilter(where: Record<string, any>): any {
    const filter: any = {};
    const isMongo = this.databaseConfigService.isMongoDb();
    for (const [key, value] of Object.entries(where)) {
      if (key === 'id' || key === '_id') {
        if (isMongo) {
          filter._id = { _eq: this.safeObjectId(value) };
        } else {
          filter.id = { _eq: value };
        }
      } else {
        filter[key] = { _eq: value };
      }
    }
    return filter;
  }

  private filterToWhere(filter: any): WhereCondition[] {
    const conditions: WhereCondition[] = [];

    for (const [field, value] of Object.entries(filter)) {
      if (
        typeof value === 'object' &&
        value !== null &&
        !Array.isArray(value)
      ) {
        const operatorEntry = Object.entries(value)[0];
        if (operatorEntry) {
          const [operator, operand] = operatorEntry;
          const sqlOperator = this.mapFilterOperatorToSql(operator);
          conditions.push({
            field,
            operator: sqlOperator,
            value: operand,
          } as WhereCondition);
        }
      }
    }

    return conditions;
  }

  private mapFilterOperatorToSql(op: string): string {
    const opMap: Record<string, string> = {
      _eq: '=',
      _neq: '!=',
      _gt: '>',
      _gte: '>=',
      _lt: '<',
      _lte: '<=',
      _in: 'in',
      _not_in: 'not in',
      _contains: 'like',
      _starts_with: 'like',
      _ends_with: 'like',
      _is_null: 'is null',
      _is_not_null: 'is not null',
    };
    return opMap[op] || '=';
  }

  async find(options: {
    table: string;
    filter?: any;
    where?: Record<string, any>;
    fields?: string | string[];
    sort?: string | string[];
    page?: number;
    limit?: number;
    meta?: string | string[];
    deep?: Record<string, any>;
    debugMode?: boolean;
    debugLog?: any[];
    debugTrace?: DebugTrace;
    pipeline?: any[];
    aggregate?: AggregateQuery;
    maxQueryDepth?: number;
  }): Promise<any> {
    return this.trackQueryMetric('find', options.table, async () => {
      const filter =
        options.filter ||
        (options.where ? this.whereToFilter(options.where) : undefined);
      return await this.select({
        tableName: options.table,
        filter,
        fields: options.fields,
        sort: options.sort,
        page: options.page,
        limit: options.limit,
        meta: options.meta as any,
        deep: options.deep,
        debugMode: options.debugMode,
        debugLog: options.debugLog,
        debugTrace: options.debugTrace,
        pipeline: options.pipeline,
        aggregate: options.aggregate,
        maxQueryDepth: options.maxQueryDepth,
      });
    });
  }

  async findOne(options: {
    table: string;
    filter?: any;
    where?: Record<string, any>;
    fields?: string | string[];
  }): Promise<any> {
    const result = await this.find({
      ...options,
      limit: 1,
    });
    return result?.data?.[0] ?? null;
  }

  async insert(table: string, data: Record<string, any>): Promise<any> {
    return this.trackQueryMetric('insert', table, async () => {
      if (this.dbType === 'mongodb') {
        const result = await this.mongoService.insertOne(table, data);
        return normalizeMongoDocument(result);
      }
      const insertedId = await this.knexService.insertWithCascade(table, data);
      const knex = this.knexService.getKnex();
      const recordId = insertedId || data.id;
      return await knex(table).where('id', recordId).first();
    });
  }

  async update(
    table: string,
    id: any,
    data: Record<string, any>,
  ): Promise<any> {
    return this.trackQueryMetric('update', table, async () => {
      if (id && typeof id === 'object' && 'where' in id) {
        return await this.updateWithOptions({ table, where: id.where, data });
      }

      if (this.dbType === 'mongodb') {
        const result = await this.mongoService.updateOne(table, id, data);
        return normalizeMongoDocument(result);
      }
      await this.knexService.updateWithCascade(table, id, data);
      const knex = this.knexService.getKnex();
      return await knex(table).where('id', id).first();
    });
  }

  async delete(table: string, id: any): Promise<boolean> {
    return this.trackQueryMetric('delete', table, async () => {
      if (id && typeof id === 'object' && 'where' in id) {
        const count = await this.deleteWithOptions({ table, where: id.where });
        return count > 0;
      }

      if (this.dbType === 'mongodb') {
        return await this.mongoService.deleteOne(table, id);
      }
      const knex = this.knexService.getKnex();
      const deleted = await knex(table).where('id', id).delete();
      return deleted > 0;
    });
  }

  async countRecords(table: string, filter?: any): Promise<number> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.count(table, filter || {});
    }
    const knex = this.knexService.getKnex();
    let query: any = knex(table);

    if (filter && Object.keys(filter).length > 0) {
      const where = this.filterToWhere(filter);
      query = await this.applyWhereToKnex(query, where, table);
    }

    const result = await query.count('* as count').first();
    return Number(result?.count || 0);
  }

  async transaction<T>(callback: (trx: any) => Promise<T>): Promise<T> {
    if (this.dbType === 'mongodb') {
      const session = this.mongoService.getClient().startSession();
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

    return this.knexService.transaction(callback);
  }

  async findById(table: string, id: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      const mongoId = this.safeObjectId(id);
      return this.mongoService.findOne(table, { _id: mongoId });
    }

    const knex = this.knexService.getKnex();
    return await knex(table).where('id', id).first();
  }

  async findOneWhere(table: string, where: Record<string, any>): Promise<any> {
    if (this.dbType === 'mongodb') {
      const normalizedWhere: any = {};

      for (const [key, value] of Object.entries(where)) {
        if (key === 'id' || key === '_id') {
          normalizedWhere._id = this.safeObjectId(value);
        } else {
          normalizedWhere[key] = value;
        }
      }

      return this.mongoService.findOne(table, normalizedWhere);
    }

    const knex = this.knexService.getKnex();
    return await knex(table).where(where).first();
  }

  async findWhere(table: string, where: Record<string, any>): Promise<any[]> {
    if (this.dbType === 'mongodb') {
      const normalizedWhere: any = {};

      for (const [key, value] of Object.entries(where)) {
        if (key === 'id' || key === '_id') {
          normalizedWhere._id = this.safeObjectId(value);
        } else {
          normalizedWhere[key] = value;
        }
      }

      const collection = this.mongoService.collection(table);
      const results = await collection.find(normalizedWhere).toArray();
      return results;
    }

    const knex = this.knexService.getKnex();
    return await knex(table).where(where);
  }

  async insertAndGet(table: string, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.insertOne(table, data);
    }

    const insertedId = await this.knexService.insertWithCascade(table, data);

    const knex = this.knexService.getKnex();
    const recordId = insertedId || data.id;
    return await knex(table).where('id', recordId).first();
  }

  async updateById(table: string, id: any, data: any): Promise<any> {
    if (this.dbType === 'mongodb') {
      return this.mongoService.updateOne(table, id, data);
    }

    await this.knexService.updateWithCascade(table, id, data);
    const knex = this.knexService.getKnex();
    return await knex(table).where('id', id).first();
  }

  async deleteById(table: string, id: any): Promise<number> {
    if (this.dbType === 'mongodb') {
      const deleted = await this.mongoService.deleteOne(table, id);
      return deleted ? 1 : 0;
    }

    const knex = this.knexService.getKnex();
    return knex(table).where('id', id).delete();
  }

  async raw(query: string | any, bindings?: any): Promise<any> {
    const table = this.dbType === 'mongodb' ? 'mongodb' : 'sql';
    return this.trackQueryMetric('raw', table, async () => {
      if (this.dbType === 'mongodb') {
        const db = this.mongoService.getDb();
        if (typeof query === 'string') {
          if (query.toLowerCase().includes('select 1')) {
            return await db.command({ ping: 1 });
          }
          throw new Error(
            'String queries not supported for MongoDB. Use db.command() object instead.',
          );
        }
        return await db.command(query);
      }

      const knex = this.knexService.getKnex();
      return await knex.raw(query, bindings);
    });
  }

  private async trackQueryMetric<T>(
    op: string,
    table: string | undefined,
    callback: () => Promise<T>,
  ): Promise<T> {
    if (this.runtimeMetricsCollectorService) {
      return await this.runtimeMetricsCollectorService.trackQuery(
        { op, table },
        callback,
      );
    }

    return await callback();
  }

  getConnection(): any {
    if (this.dbType === 'mongodb') {
      return this.mongoService.getDb();
    }
    return this.knexService.getKnex();
  }

  getKnex(): any {
    if (this.dbType === 'mongodb') {
      throw new Error(
        'getKnex() is not available for MongoDB. Use getConnection() or unified methods.',
      );
    }
    return this.knexService.getKnex();
  }

  getMongoDb(): any {
    if (this.dbType !== 'mongodb') {
      throw new Error(
        'getMongoDb() is not available for SQL. Use getConnection() or unified methods.',
      );
    }
    return this.mongoService.getDb();
  }

  getDatabaseType(): DatabaseType {
    return this.dbType;
  }

  isMongoDb(): boolean {
    return this.dbType === 'mongodb';
  }

  isSql(): boolean {
    return ['mysql', 'postgres', 'mariadb', 'sqlite'].includes(this.dbType);
  }

  getPkField(): string {
    return this.isMongoDb() ? '_id' : 'id';
  }

  getRecordId(record: any): any {
    return this.isMongoDb() ? record._id : record.id;
  }
}
