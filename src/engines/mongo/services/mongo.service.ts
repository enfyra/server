import {
  MongoClient,
  Db,
  Collection,
  Document,
  ObjectId,
  Long,
  ClientSession,
} from 'mongodb';
import { randomUUID } from 'crypto';
import { AsyncLocalStorage } from 'async_hooks';
import type { Cradle } from '../../../container';
import { Logger } from '../../../shared/logger';
import { EnvService, DatabaseConfigService } from '../../../shared/services';
import { MetadataCacheService } from '../../cache';
import { MongoRelationManagerService } from './mongo-relation-manager.service';
import type { MongoSagaSession } from './mongo-saga-session';
import { mongoTopologySupportsNativeTransactions } from '../utils/mongo-native-transaction-topology.util';
import { DatabaseException } from '../../../domain/exceptions';

export class MongoService {
  private client!: MongoClient;
  private db!: Db;
  private readonly logger = new Logger(MongoService.name);
  private readonly envService: EnvService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly mongoRelationManagerService: MongoRelationManagerService;
  private readonly lazyRef: Cradle;

  private readonly policyContext = new AsyncLocalStorage<{
    check: (
      tableName: string,
      operation: 'create' | 'update' | 'delete',
      data: any,
    ) => Promise<void>;
  }>();
  private readonly fieldPermissionContext = new AsyncLocalStorage<{
    check: (
      tableName: string,
      action: 'create' | 'update',
      data: any,
    ) => Promise<void>;
  }>();
  private nativeMultiDocSupported = false;
  private readonly nativeTxBundleAls = new AsyncLocalStorage<{
    session: ClientSession;
    logicalTxId: string;
  }>();
  private readonly appTxSessionAls = new AsyncLocalStorage<MongoSagaSession>();

  constructor(deps: {
    envService: EnvService;
    databaseConfigService: DatabaseConfigService;
    metadataCacheService: MetadataCacheService;
    mongoRelationManagerService: MongoRelationManagerService;
    lazyRef: Cradle;
  }) {
    this.envService = deps.envService;
    this.databaseConfigService = deps.databaseConfigService;
    this.metadataCacheService = deps.metadataCacheService;
    this.mongoRelationManagerService = deps.mongoRelationManagerService;
    this.lazyRef = deps.lazyRef;
  }

  async runWithPolicy<T>(
    policyCheck: (
      tableName: string,
      operation: 'create' | 'update' | 'delete',
      data: any,
    ) => Promise<void>,
    callback: () => Promise<T>,
  ): Promise<T> {
    return this.policyContext.run({ check: policyCheck }, callback);
  }

  private async checkPolicy(
    tableName: string,
    operation: 'create' | 'update' | 'delete',
    data: any,
  ): Promise<void> {
    const ctx = this.policyContext.getStore();
    if (ctx) {
      await ctx.check(tableName, operation, data);
    }
  }

  async runWithFieldPermissionCheck<T>(
    checker: (
      tableName: string,
      action: 'create' | 'update',
      data: any,
    ) => Promise<void>,
    callback: () => Promise<T>,
  ): Promise<T> {
    return this.fieldPermissionContext.run({ check: checker }, callback);
  }

  private async checkFieldPermission(
    tableName: string,
    action: 'create' | 'update',
    data: any,
  ): Promise<void> {
    const ctx = this.fieldPermissionContext.getStore();
    if (ctx) {
      await ctx.check(tableName, action, data);
    }
  }

  async init(): Promise<void> {
    if (!this.databaseConfigService.isMongoDb()) {
      return;
    }

    const uri = this.envService.get('DB_URI');

    if (!uri) {
      throw new Error('DB_URI is not defined in environment variables');
    }

    try {
      this.client = new MongoClient(uri);
      await this.client.connect();

      const dbName = this.extractDbName(uri);
      this.db = this.client.db(dbName);

      await this.refreshNativeTransactionCapability();
      await this.db.command({ ping: 1 });
      this.logger.log(`Connected to MongoDB: ${dbName}`);
    } catch (error) {
      this.logger.error('Failed to connect to MongoDB:', error);
      throw error;
    }
  }

  async onDestroy(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.logger.log('MongoDB connection closed');
    }
  }

  getDb(): Db {
    if (!this.db) {
      throw new Error('MongoDB is not initialized');
    }
    return this.db;
  }

  getClient(): MongoClient {
    if (!this.client) {
      throw new Error('MongoDB client is not initialized');
    }
    return this.client;
  }

  supportsNativeMultiDocumentTransactions(): boolean {
    return this.nativeMultiDocSupported;
  }

  getActiveSagaSession(): MongoSagaSession | undefined {
    return this.appTxSessionAls.getStore();
  }

  isInSaga(): boolean {
    return (
      this.nativeTxBundleAls.getStore() !== undefined ||
      this.appTxSessionAls.getStore() !== undefined
    );
  }

  getCurrentSagaId(): string | undefined {
    const n = this.nativeTxBundleAls.getStore();
    if (n) {
      return n.logicalTxId;
    }
    return this.appTxSessionAls.getStore()?.txId;
  }

  async runInSaga<T>(
    fn: () => Promise<T>,
    options?: { throwOnFailure?: boolean },
  ): Promise<{
    success: boolean;
    data?: T;
    error?: unknown;
    txId: string;
    rollbackResult?: unknown;
    stats?: unknown;
  }> {
    const throwOnFailure = options?.throwOnFailure !== false;
    if (this.nativeMultiDocSupported) {
      const logicalTxId = `tx-${randomUUID()}`;
      const session = this.client.startSession();
      try {
        let dataOut: T | undefined;
        await session.withTransaction(async () => {
          dataOut = await this.nativeTxBundleAls.run(
            { session, logicalTxId },
            fn,
          );
        });
        return { success: true, data: dataOut as T, txId: logicalTxId };
      } catch (error) {
        if (throwOnFailure) {
          throw error;
        }
        return { success: false, error, txId: logicalTxId };
      } finally {
        await session.endSession();
      }
    }
    const sagaCoordinator = this.lazyRef.mongoSagaCoordinator;
    if (!sagaCoordinator) {
      throw new Error(
        'MongoSagaCoordinator is required when native multi-document transactions are not available',
      );
    }
    const execResult = await sagaCoordinator.execute((tx) =>
      this.appTxSessionAls.run(tx, fn),
    );
    if (throwOnFailure && !execResult.success) {
      const err = execResult.error;
      if (err instanceof Error) {
        throw err;
      }
      throw new DatabaseException(
        typeof err === 'string' ? err : 'Application transaction failed',
        { txId: execResult.txId },
      );
    }
    return {
      success: execResult.success,
      data: execResult.data,
      error: execResult.error,
      txId: execResult.txId,
      rollbackResult: execResult.rollbackResult,
      stats: execResult.stats,
    };
  }

  private async refreshNativeTransactionCapability(): Promise<void> {
    if (!this.client || !this.db) {
      return;
    }
    if (process.env.MONGO_FORCE_APP_TRANSACTION === '1') {
      this.nativeMultiDocSupported = false;
      this.logger.log(
        'MongoDB: MONGO_FORCE_APP_TRANSACTION=1, using application-level transactions',
      );
      return;
    }
    try {
      const hello = await this.db.admin().command({ hello: 1 });
      if (mongoTopologySupportsNativeTransactions(hello)) {
        this.nativeMultiDocSupported = true;
        const h = hello as Record<string, unknown>;
        if (typeof h.setName === 'string' && h.setName.length > 0) {
          this.logger.log(
            'MongoDB: replica set detected (hello.setName), native multi-document transactions enabled',
          );
        } else {
          this.logger.log(
            'MongoDB: mongos detected (hello.msg=isdbgrid), native multi-document transactions enabled',
          );
        }
        return;
      }
    } catch {}
    const probeSession = this.client.startSession();
    try {
      probeSession.startTransaction();
      await probeSession.abortTransaction();
      this.nativeMultiDocSupported = true;
      this.logger.log('MongoDB: native transaction probe succeeded');
    } catch {
      this.nativeMultiDocSupported = false;
      this.logger.log(
        'MongoDB: native transactions unavailable, using application-level transactions',
      );
    } finally {
      await probeSession.endSession();
    }
  }

  collection<T extends Document = Document>(name: string): Collection<T> {
    const nativeCtx = this.nativeTxBundleAls.getStore();
    if (nativeCtx) {
      return new NativeSessionCollection(
        this.getDb().collection<T>(name),
        nativeCtx.session,
      ) as unknown as Collection<T>;
    }
    if (this.appTxSessionAls.getStore()) {
      return new SagaCollection(name, this) as unknown as Collection<T>;
    }
    return this.getDb().collection<T>(name);
  }

  async applyDefaultValues(tableName: string, data: any): Promise<any> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata || !metadata.columns) {
      return data;
    }

    const result = { ...data };

    for (const column of metadata.columns) {
      if (result[column.name] !== undefined && result[column.name] !== null) {
        continue;
      }

      if (column.defaultValue !== undefined && column.defaultValue !== null) {
        if (typeof column.defaultValue === 'string') {
          try {
            result[column.name] = JSON.parse(column.defaultValue);
          } catch {
            result[column.name] = column.defaultValue;
          }
        } else {
          result[column.name] = column.defaultValue;
        }
      }
    }

    return result;
  }

  async parseJsonFields(tableName: string, data: any): Promise<any> {
    const metadata =
      await this.metadataCacheService.lookupTableByName(tableName);
    if (!metadata || !metadata.columns) {
      return data;
    }

    const result = { ...data };

    for (const column of metadata.columns) {
      const fieldName = column.name;
      const fieldValue = result[fieldName];

      if (fieldValue === undefined || fieldValue === null) {
        continue;
      }

      if (column.type === 'bigint' && typeof fieldValue === 'number') {
        result[fieldName] = Long.fromNumber(fieldValue);
        continue;
      }

      if (column.type === 'simple-json' || column.type === 'json') {
        if (typeof fieldValue === 'string') {
          try {
            result[fieldName] = JSON.parse(fieldValue);
          } catch (error: any) {
            this.logger.warn(
              `Failed to parse JSON field '${fieldName}': ${error.message}`,
            );
          }
        }
      }
    }

    return result;
  }

  applyTimestamps(data: any | any[]): any | any[] {
    return MongoService.applyTimestampsStatic(data);
  }

  static applyTimestampsStatic(data: any | any[]): any | any[] {
    const now = new Date();

    if (Array.isArray(data)) {
      return data.map((record) => {
        const {
          id: _id,
          createdAt: _ca,
          updatedAt: _ua,
          ...cleanRecord
        } = record;
        return {
          ...cleanRecord,
          createdAt: now,
          updatedAt: now,
        };
      });
    } else {
      const { id: _id, createdAt: _ca, updatedAt: _ua, ...cleanData } = data;
      return {
        ...cleanData,
        createdAt: now,
        updatedAt: now,
      };
    }
  }

  async stripInverseRelations(tableName: string, data: any): Promise<any> {
    return this.mongoRelationManagerService.stripInverseRelations(
      tableName,
      data,
    );
  }

  async insertOne(collectionName: string, data: any): Promise<any> {
    const collection = this.collection(collectionName);

    const dataParsed = await this.parseJsonFields(collectionName, data);
    const dataWithDefaults = await this.applyDefaultValues(
      collectionName,
      dataParsed,
    );
    const dataWithRelations = await this.processNestedRelations(
      collectionName,
      dataWithDefaults,
    );
    const dataWithoutInverse = await this.stripInverseRelations(
      collectionName,
      dataWithRelations,
    );
    const dataStripped = await this.stripUnknownColumns(
      collectionName,
      dataWithoutInverse,
    );
    const dataWithTimestamps = this.applyTimestamps(dataStripped);

    await this.checkFieldPermission(
      collectionName,
      'create',
      dataWithTimestamps,
    );

    await this.mongoRelationManagerService.clearUniqueFKHolders(
      collectionName,
      new ObjectId(),
      dataWithTimestamps,
      (name) => this.collection(name),
    );

    let result;
    let insertedId;
    try {
      result = await collection.insertOne(dataWithTimestamps);
      insertedId = result.insertedId;
    } catch (err: any) {
      const errorMessage = err.errInfo?.details?.details
        ? JSON.stringify(err.errInfo.details.details, null, 2)
        : err.errInfo
          ? JSON.stringify(err.errInfo, null, 2)
          : err.message || 'Unknown validation error';

      console.error(
        `[insertOne] Validation error for ${collectionName}:`,
        errorMessage,
      );

      const validationError = new Error(
        `MongoDB validation failed for ${collectionName}: ${errorMessage}`,
      );
      (validationError as any).errInfo = err.errInfo;
      throw validationError;
    }

    await this.mongoRelationManagerService.updateInverseRelationsOnUpdate(
      collectionName,
      insertedId,
      {},
      dataWithRelations,
      (name) => this.collection(name),
    );

    await this.mongoRelationManagerService.writeM2mJunctionsForInsert(
      collectionName,
      insertedId,
      dataWithRelations,
      (name) => this.collection(name),
    );

    return {
      ...dataWithTimestamps,
      _id: insertedId,
    };
  }

  async find(options: {
    tableName: string;
    filter?: any;
    limit?: number;
    skip?: number;
  }): Promise<any[]> {
    const { tableName, filter = {}, limit, skip } = options;
    const collection = this.collection(tableName);

    let cursor = collection.find(filter);

    if (skip) cursor = cursor.skip(skip);
    if (limit) cursor = cursor.limit(limit);

    const results = await cursor.toArray();
    return results;
  }

  async findOne(collectionName: string, filter: any): Promise<any> {
    const collection = this.collection(collectionName);
    const result = await collection.findOne(filter);
    return result;
  }

  async processNestedRelations(tableName: string, data: any): Promise<any> {
    return this.mongoRelationManagerService.processNestedRelations(
      tableName,
      data,
      (name) => this.collection(name),
      this.checkPolicy.bind(this),
      this.insertOne.bind(this),
      this.updateOne.bind(this),
    );
  }

  async stripNonUpdatableFields(
    collectionName: string,
    data: any,
  ): Promise<any> {
    const tableMetadata =
      await this.metadataCacheService.getTableMetadata(collectionName);
    if (!tableMetadata || !tableMetadata.columns) return data;

    const filteredData = { ...data };

    for (const column of tableMetadata.columns) {
      if (column.isUpdatable === false && column.name in filteredData) {
        delete filteredData[column.name];
      }
    }

    return filteredData;
  }

  async stripUnknownColumns(collectionName: string, data: any): Promise<any> {
    const tableMetadata =
      await this.metadataCacheService.lookupTableByName(collectionName);
    if (!tableMetadata) return data;

    const validFields = new Set<string>();
    if (tableMetadata.columns) {
      for (const col of tableMetadata.columns) {
        validFields.add(col.name);
      }
    }
    if (tableMetadata.relations) {
      for (const rel of tableMetadata.relations) {
        if (
          rel.propertyName &&
          ['many-to-one', 'one-to-one'].includes(rel.type) &&
          !(rel.type === 'one-to-one' && (rel.mappedBy || rel.isInverse))
        ) {
          validFields.add(rel.propertyName);
        }
        if (rel.foreignKeyColumn) {
          validFields.add(rel.foreignKeyColumn);
        }
      }
    }

    const stripped = { ...data };
    for (const key of Object.keys(stripped)) {
      if (!validFields.has(key)) {
        delete stripped[key];
      }
    }
    return stripped;
  }

  applyUpdateTimestamp(data: any): any {
    const {
      _id,
      id: _idField,
      createdAt: _ca,
      updatedAt: _ua,
      ...cleanData
    } = data;
    return {
      ...cleanData,
      updatedAt: new Date(),
    };
  }

  async updateOne(collectionName: string, id: string, data: any): Promise<any> {
    const collection = this.collection(collectionName);
    const objectId = new ObjectId(id);

    const oldRecord = await this.findOne(collectionName, { _id: objectId });

    const dataParsed = await this.parseJsonFields(collectionName, data);
    const dataWithRelations = await this.processNestedRelations(
      collectionName,
      dataParsed,
    );
    const dataWithoutInverse = await this.stripInverseRelations(
      collectionName,
      dataWithRelations,
    );
    const dataStripped = await this.stripUnknownColumns(
      collectionName,
      dataWithoutInverse,
    );
    const dataWithoutNonUpdatable = await this.stripNonUpdatableFields(
      collectionName,
      dataStripped,
    );
    const dataWithTimestamp = this.applyUpdateTimestamp(
      dataWithoutNonUpdatable,
    );

    await this.checkFieldPermission(
      collectionName,
      'update',
      dataWithTimestamp,
    );

    await this.mongoRelationManagerService.clearUniqueFKHolders(
      collectionName,
      objectId,
      dataWithTimestamp,
      (name) => this.collection(name),
    );

    await collection.updateOne({ _id: objectId }, { $set: dataWithTimestamp });

    await this.mongoRelationManagerService.updateInverseRelationsOnUpdate(
      collectionName,
      objectId,
      oldRecord,
      dataWithRelations,
      (name) => this.collection(name),
    );

    await this.mongoRelationManagerService.writeM2mJunctionsForUpdate(
      collectionName,
      objectId,
      dataWithRelations,
      (name) => this.collection(name),
    );

    return this.findOne(collectionName, { _id: objectId });
  }

  async deleteOne(collectionName: string, id: string): Promise<boolean> {
    const collection = this.collection(collectionName);
    const objectId = new ObjectId(id);

    const record = await this.findOne(collectionName, { _id: objectId });
    if (!record) {
      return false;
    }

    await this.mongoRelationManagerService.cleanupInverseRelationsOnDelete(
      collectionName,
      objectId,
      record,
      (name) => this.collection(name),
    );

    const result = await collection.deleteOne({ _id: objectId });
    return result.deletedCount > 0;
  }

  async count(collectionName: string, filter: any = {}): Promise<number> {
    const collection = this.collection(collectionName);
    return collection.countDocuments(filter);
  }

  private extractDbName(uri: string): string {
    const match = uri.match(/\/([^/?]+)(\?|$)/);
    return match ? match[1] : 'enfyra';
  }
}

export class NativeSessionCollection<T extends Document = Document> {
  constructor(
    private readonly base: Collection<T>,
    private readonly session: ClientSession,
  ) {}

  find(filter: any): any {
    const cursor = this.base.find(filter as any, { session: this.session });
    let skipVal = 0;
    let limitVal: number | undefined;
    const self = {
      skip: (n: number) => {
        skipVal = n;
        return self;
      },
      limit: (n: number) => {
        limitVal = n;
        return self;
      },
      toArray: () => {
        let c = cursor;
        if (skipVal) {
          c = c.skip(skipVal);
        }
        if (limitVal !== undefined) {
          c = c.limit(limitVal);
        }
        return c.toArray();
      },
      then: (onFulfilled: any, onRejected: any) =>
        self.toArray().then(onFulfilled, onRejected),
    };
    return self;
  }

  findOne(filter: any, options?: any) {
    return this.base.findOne(filter, { ...options, session: this.session });
  }

  countDocuments(filter?: any, options?: any) {
    return this.base.countDocuments(filter || {}, {
      ...options,
      session: this.session,
    });
  }

  insertOne(doc: any, options?: any) {
    return this.base.insertOne(doc, { ...options, session: this.session });
  }

  insertMany(docs: any[], options?: any) {
    return this.base.insertMany(docs, { ...options, session: this.session });
  }

  updateOne(filter: any, update: any, options?: any) {
    return this.base.updateOne(filter, update, {
      ...options,
      session: this.session,
    });
  }

  updateMany(filter: any, update: any, options?: any) {
    return this.base.updateMany(filter, update, {
      ...options,
      session: this.session,
    });
  }

  deleteOne(filter?: any, options?: any) {
    return this.base.deleteOne(filter, { ...options, session: this.session });
  }

  deleteMany(filter?: any, options?: any) {
    return this.base.deleteMany(filter || {}, {
      ...options,
      session: this.session,
    });
  }

  aggregate(pipeline: any[], options?: any) {
    return this.base.aggregate(pipeline, {
      ...options,
      session: this.session,
    });
  }

  bulkWrite(operations: any[], options?: any) {
    return this.base.bulkWrite(operations, {
      ...options,
      session: this.session,
    });
  }
}

export class SagaCollection<_T extends Document = Document> {
  constructor(
    private readonly name: string,
    private readonly mongo: MongoService,
  ) {}

  private session(): MongoSagaSession {
    const tx = this.mongo.getActiveSagaSession();
    if (!tx) {
      throw new Error('No active saga session');
    }
    return tx;
  }

  find(filter?: any): any {
    const collName = this.name;
    const getTx = () => this.session();
    let skipVal = 0;
    let limitVal: number | undefined;
    const self = {
      skip: (n: number) => {
        skipVal = n;
        return self;
      },
      limit: (n: number) => {
        limitVal = n;
        return self;
      },
      toArray: () =>
        getTx().find(collName, filter, {
          skip: skipVal || undefined,
          limit: limitVal,
        }),
      then: (onFulfilled: any, onRejected: any) =>
        self.toArray().then(onFulfilled, onRejected),
    };
    return self;
  }

  findOne(filter: any, options?: any) {
    return this.session().findOne(this.name, filter, options);
  }

  countDocuments(filter?: any) {
    return this.session().countDocuments(this.name, filter || {});
  }

  async insertOne(doc: any, options?: any) {
    const r = await this.session().insertOne(this.name, doc, options);
    return { acknowledged: true, insertedId: r._id };
  }

  insertMany(docs: any[], options?: any) {
    return this.session().insertMany(this.name, docs, options);
  }

  async updateOne(filter: any, update: any, options?: any) {
    const tx = this.session();
    if (filter && filter._id != null) {
      const opKeys =
        update && typeof update === 'object' && !Array.isArray(update)
          ? Object.keys(update)
          : [];
      const onlyPlainOrSet =
        opKeys.length === 0 ||
        (opKeys.length === 1 && opKeys[0] === '$set') ||
        opKeys.every((k) => !k.startsWith('$'));
      if (onlyPlainOrSet) {
        const payload = update.$set ?? update;
        return tx.updateOne(this.name, filter._id, payload, options);
      }
    }
    return tx.updateOneByFilter(this.name, filter, update, options);
  }

  updateMany(filter: any, update: any, options?: any) {
    return this.session().updateManyByFilter(
      this.name,
      filter,
      update,
      options,
    );
  }

  async deleteOne(filter?: any, options?: any) {
    const tx = this.session();
    if (filter && filter._id != null) {
      const ok = await tx.deleteOne(this.name, filter._id, options);
      return { deletedCount: ok ? 1 : 0, acknowledged: true };
    }
    const doc = await tx.findOne(this.name, filter || {});
    if (!doc) {
      return { deletedCount: 0, acknowledged: true };
    }
    const ok = await tx.deleteOne(this.name, doc._id, options);
    return { deletedCount: ok ? 1 : 0, acknowledged: true };
  }

  async deleteMany(filter?: any, options?: any) {
    const tx = this.session();
    const coll = this.mongo.getDb().collection(this.name);
    const cursor = coll.find(filter || {});
    const batch: ObjectId[] = [];
    const BATCH = 500;
    let total = 0;
    const flush = async () => {
      if (batch.length === 0) return;
      const r = await tx.deleteMany(this.name, batch, options);
      total += r.deletedCount ?? 0;
      batch.length = 0;
    };
    for await (const doc of cursor) {
      batch.push(doc._id);
      if (batch.length >= BATCH) {
        await flush();
      }
    }
    await flush();
    return { deletedCount: total, acknowledged: true };
  }

  aggregate(pipeline: any[], options?: any) {
    return this.session().aggregate(this.name, pipeline, options);
  }

  bulkWrite(operations: any[], options?: any) {
    this.session().assertWithinMaxDuration();
    return this.mongo
      .getDb()
      .collection(this.name)
      .bulkWrite(operations, options);
  }
}
