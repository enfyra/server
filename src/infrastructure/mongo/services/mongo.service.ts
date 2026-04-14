import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
  Inject,
  forwardRef,
  Optional,
} from '@nestjs/common';
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
import { ConfigService } from '@nestjs/config';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import {
  MongoSagaCoordinator,
  MongoSagaSession,
} from './mongo-saga-coordinator.service';
import { mongoTopologySupportsNativeTransactions } from '../utils/mongo-native-transaction-topology.util';
import {
  normalizeRelationOnDelete,
  TRelationOnDeleteAction,
} from '../utils/mongo-relation-on-delete.util';
import { resolveMongoJunctionInfo } from '../utils/mongo-junction.util';

const M2M_PENDING = Symbol('mongoService.m2mPending');
import {
  DatabaseException,
  ValidationException,
} from '../../../core/exceptions/custom-exceptions';

@Injectable()
export class MongoService implements OnModuleInit, OnModuleDestroy {
  private client: MongoClient;
  private db: Db;
  private readonly logger = new Logger(MongoService.name);
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

  constructor(
    private readonly configService: ConfigService,
    private readonly databaseConfig: DatabaseConfigService,
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCache: MetadataCacheService,
    @Optional()
    @Inject(forwardRef(() => MongoSagaCoordinator))
    private readonly sagaCoordinator?: MongoSagaCoordinator,
  ) {}

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

  async onModuleInit() {
    if (!this.databaseConfig.isMongoDb()) {
      return;
    }

    const uri =
      this.configService.get<string>('DB_URI');

    if (!uri) {
      throw new Error(
        'DB_URI is not defined in environment variables',
      );
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

  async onModuleDestroy() {
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
          dataOut = await this.nativeTxBundleAls.run({ session, logicalTxId }, fn);
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
    if (!this.sagaCoordinator) {
      throw new Error(
        'MongoSagaCoordinator is required when native multi-document transactions are not available',
      );
    }
    const execResult = await this.sagaCoordinator.execute((tx) =>
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
    const metadata = await this.metadataCache.lookupTableByName(tableName);
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
    const metadata = await this.metadataCache.lookupTableByName(tableName);
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
          } catch (error) {
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
        const { id, createdAt, updatedAt, ...cleanRecord } = record;
        return {
          ...cleanRecord,
          createdAt: now,
          updatedAt: now,
        };
      });
    } else {
      const { id, createdAt, updatedAt, ...cleanData } = data;
      return {
        ...cleanData,
        createdAt: now,
        updatedAt: now,
      };
    }
  }

  async stripInverseRelations(tableName: string, data: any): Promise<any> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata?.relations) {
      return data;
    }

    const result = { ...data };

    for (const relation of metadata.relations) {
      if (relation.type === 'many-to-many') continue;

      const isInverse =
        relation.type === 'one-to-many' ||
        relation.isInverse;

      if (isInverse && relation.propertyName in result) {
        delete result[relation.propertyName];
      }
    }

    return result;
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

    await this.checkFieldPermission(collectionName, 'create', dataWithTimestamps);

    // Clear unique FK holders before insert to prevent unique constraint violations
    // Use a dummy ObjectId since we don't have the real one yet
    await this.clearUniqueFKHolders(
      collectionName,
      new ObjectId(),
      dataWithTimestamps,
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

    await this.updateInverseRelationsOnUpdate(
      collectionName,
      insertedId,
      {},
      dataWithRelations,
    );

    await this.writeM2mJunctionsForInsert(
      collectionName,
      insertedId,
      dataWithRelations,
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

  async updateInverseRelationsOnUpdate(
    tableName: string,
    recordId: ObjectId,
    oldData: any,
    newData: any,
  ): Promise<void> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (relation.type === 'many-to-many') {
        continue;
      }
      if (!relation.mappedBy) {
        continue;
      }

      const fieldName = relation.propertyName;

      if (!(fieldName in newData)) {
        continue;
      }

      const oldValue = oldData?.[fieldName];
      const newValue = newData?.[fieldName];

      const targetCollection = relation.targetTableName || relation.targetTable;

      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        const oldId =
          oldValue instanceof ObjectId
            ? oldValue
            : oldValue
              ? typeof oldValue === 'object' && oldValue._id
                ? new ObjectId(oldValue._id)
                : new ObjectId(oldValue)
              : null;
        const newId =
          newValue instanceof ObjectId
            ? newValue
            : newValue
              ? typeof newValue === 'object' && newValue._id
                ? new ObjectId(newValue._id)
                : new ObjectId(newValue)
              : null;

        if (oldId && (!newId || oldId.toString() !== newId.toString())) {
          if (relation.type === 'many-to-one') {
            await this.collection(targetCollection).updateOne(
              { _id: oldId },
              {
                $pull: { [relation.mappedBy]: recordId },
              } as any,
            );
          } else {
            await this.collection(targetCollection).updateOne(
              { _id: oldId },
              {
                $unset: { [relation.mappedBy]: '' },
              } as any,
            );
          }
        }

        if (newId && (!oldId || oldId.toString() !== newId.toString())) {
          if (relation.type === 'many-to-one') {
            await this.collection(targetCollection).updateOne(
              { _id: newId },
              { $addToSet: { [relation.mappedBy]: recordId } },
            );
          } else {
            await this.collection(targetCollection).updateOne(
              { _id: newId },
              { $set: { [relation.mappedBy]: recordId } },
            );
          }
        }
      } else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        const oldIds = Array.isArray(oldValue)
          ? oldValue.map((v) => {
              if (v instanceof ObjectId) return v;
              if (typeof v === 'object' && v._id) return new ObjectId(v._id);
              return new ObjectId(v);
            })
          : [];
        const newIds = Array.isArray(newValue)
          ? newValue.map((v) => {
              if (v instanceof ObjectId) return v;
              if (typeof v === 'object' && v._id) return new ObjectId(v._id);
              return new ObjectId(v);
            })
          : [];

        const removed = oldIds.filter(
          (oldId) =>
            !newIds.some((newId) => newId.toString() === oldId.toString()),
        );
        const added = newIds.filter(
          (newId) =>
            !oldIds.some((oldId) => oldId.toString() === newId.toString()),
        );

        for (const targetId of removed) {
          if (relation.type === 'one-to-many') {
            await this.collection(targetCollection).updateOne(
              { _id: targetId },
              {
                $unset: { [relation.mappedBy]: '' },
              } as any,
            );
          } else {
            await this.collection(targetCollection).updateOne(
              { _id: targetId },
              {
                $pull: { [relation.mappedBy]: recordId },
              } as any,
            );
          }
        }

        for (const targetId of added) {
          if (relation.type === 'one-to-many') {
            await this.collection(targetCollection).updateOne(
              { _id: targetId },
              { $set: { [relation.mappedBy]: recordId } },
            );
          } else {
            await this.collection(targetCollection).updateOne(
              { _id: targetId },
              { $addToSet: { [relation.mappedBy]: recordId } },
            );
          }
        }
      }
    }
  }

  async processNestedRelations(tableName: string, data: any): Promise<any> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return data;
    }

    const processed = { ...data };

    for (const relation of metadata.relations) {
      const fieldName = relation.propertyName;

      if (!(fieldName in processed)) continue;

      const isInverse =
        relation.type === 'one-to-many' ||
        (relation.type === 'one-to-one' &&
          (relation.mappedBy || relation.isInverse));

      if (isInverse) {
        continue;
      }

      const fieldValue = processed[fieldName];
      const targetCollection = relation.targetTableName || relation.targetTable;

      if (fieldValue === null || fieldValue === undefined) {
        if (relation.type === 'many-to-many') {
          this.setM2mPending(processed, fieldName, []);
          delete processed[fieldName];
        } else {
          processed[fieldName] = null;
        }
        continue;
      }

      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        if (
          typeof fieldValue !== 'object' ||
          Array.isArray(fieldValue) ||
          fieldValue instanceof ObjectId ||
          fieldValue instanceof Date
        ) {
          if (typeof fieldValue === 'string' && fieldValue.length === 24) {
            try {
              processed[fieldName] = new ObjectId(fieldValue);
            } catch (_) {
              // Not a valid ObjectId, leave as-is
            }
          }
          continue;
        }

        const { _id: nestedId, id, ...nestedData } = fieldValue;
        const hasDataToUpdate = Object.keys(nestedData).length > 0;

        if (!nestedId && !id) {
          if (hasDataToUpdate) {
            await this.checkPolicy(targetCollection, 'create', nestedData);
            const inserted = await this.insertOne(targetCollection, nestedData);
            processed[fieldName] = new ObjectId(inserted._id);
          } else {
            processed[fieldName] = null;
          }
        } else if (hasDataToUpdate) {
          const idToUse = nestedId || id;
          await this.checkPolicy(targetCollection, 'update', nestedData);
          await this.updateOne(targetCollection, idToUse, nestedData);
          processed[fieldName] =
            typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        } else {
          const idToUse = nestedId || id;
          processed[fieldName] =
            typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        }
      } else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        if (!Array.isArray(fieldValue)) {
          if (relation.type === 'many-to-many') {
            this.setM2mPending(processed, fieldName, []);
            delete processed[fieldName];
          } else {
            processed[fieldName] = [];
          }
          continue;
        }

        const processedArray = [];
        for (const item of fieldValue) {
          if (
            typeof item !== 'object' ||
            item instanceof ObjectId ||
            item instanceof Date
          ) {
            processedArray.push(
              item instanceof ObjectId ? item : new ObjectId(item),
            );
            continue;
          }

          const { _id: itemId, id: itemIdAlt, ...itemData } = item;
          const hasDataToUpdate = Object.keys(itemData).length > 0;

          if (!itemId && !itemIdAlt) {
            if (hasDataToUpdate) {
              await this.checkPolicy(targetCollection, 'create', itemData);
              const inserted = await this.insertOne(targetCollection, itemData);
              processedArray.push(new ObjectId(inserted._id));
            }
          } else if (hasDataToUpdate) {
            const idToUse = itemId || itemIdAlt;
            await this.checkPolicy(targetCollection, 'update', itemData);
            await this.updateOne(targetCollection, idToUse, itemData);
            processedArray.push(
              typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse,
            );
          } else {
            const idToUse = itemId || itemIdAlt;
            processedArray.push(
              typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse,
            );
          }
        }
        if (relation.type === 'many-to-many') {
          this.setM2mPending(processed, fieldName, processedArray);
          delete processed[fieldName];
        } else {
          processed[fieldName] = processedArray;
        }
      }
    }

    return processed;
  }

  async stripNonUpdatableFields(
    collectionName: string,
    data: any,
  ): Promise<any> {
    const tableMetadata =
      await this.metadataCache.getTableMetadata(collectionName);
    if (!tableMetadata || !tableMetadata.columns) return data;

    const filteredData = { ...data };

    for (const column of tableMetadata.columns) {
      if (column.isUpdatable === false && column.name in filteredData) {
        delete filteredData[column.name];
      }
    }

    return filteredData;
  }

  async stripUnknownColumns(
    collectionName: string,
    data: any,
  ): Promise<any> {
    const tableMetadata =
      await this.metadataCache.lookupTableByName(collectionName);
    if (!tableMetadata) return data;

    const validFields = new Set<string>();
    if (tableMetadata.columns) {
      for (const col of tableMetadata.columns) {
        validFields.add(col.name);
      }
    }
    if (tableMetadata.relations) {
      for (const rel of tableMetadata.relations) {
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
    const { _id, id: idField, createdAt, updatedAt, ...cleanData } = data;
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

    await this.checkFieldPermission(collectionName, 'update', dataWithTimestamp);

    // Clear unique FK holders before update to prevent unique constraint violations
    await this.clearUniqueFKHolders(
      collectionName,
      objectId,
      dataWithTimestamp,
    );

    await collection.updateOne({ _id: objectId }, { $set: dataWithTimestamp });

    await this.updateInverseRelationsOnUpdate(
      collectionName,
      objectId,
      oldRecord,
      dataWithRelations,
    );

    await this.writeM2mJunctionsForUpdate(
      collectionName,
      objectId,
      dataWithRelations,
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

    await this.cleanupInverseRelationsOnDelete(
      collectionName,
      objectId,
      record,
    );

    const result = await collection.deleteOne({ _id: objectId });
    return result.deletedCount > 0;
  }

  async cleanupInverseRelationsOnDelete(
    tableName: string,
    recordId: ObjectId,
    recordData: any,
  ): Promise<void> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);

    if (metadata?.relations) {
      for (const relation of metadata.relations) {
        const onDelete = normalizeRelationOnDelete(relation);
        const fieldName = relation.propertyName;
        const fieldValue = recordData?.[fieldName];
        const targetCollection = relation.targetTableName || relation.targetTable;

        if (relation.type === 'many-to-many') {
          await this.applyManyToManyOnDelete(tableName, relation, recordId, onDelete);
          continue;
        }

        if (!relation.mappedBy) {
          continue;
        }

        if (relation.type === 'many-to-one') {
          await this.unlinkManyToOneInverse(
            relation,
            recordId,
            recordData,
            targetCollection,
          );
          continue;
        }

        if (relation.type === 'one-to-many') {
          await this.applyOneToManyOnDelete(
            relation,
            recordId,
            targetCollection,
            fieldValue,
            onDelete,
          );
          continue;
        }

        if (relation.type === 'one-to-one') {
          await this.applyOneToOneOnDelete(
            relation,
            recordId,
            targetCollection,
            fieldValue,
            onDelete,
          );
        }
      }
    }

    await this.cleanupReverseManyToManyOnDelete(tableName, recordId);
  }

  private async cleanupReverseManyToManyOnDelete(
    tableName: string,
    recordId: ObjectId,
  ): Promise<void> {
    const allTables = await this.metadataCache.getAllTablesMetadata();
    if (!allTables) return;

    for (const table of allTables) {
      if (table.name === tableName) continue;
      if (!table.relations) continue;

      for (const relation of table.relations) {
        if (relation.type !== 'many-to-many') continue;

        const targetTable = relation.targetTableName || relation.targetTable;
        if (targetTable !== tableName) continue;

        const info = this.resolveJunctionInfo(table.name, relation);
        if (!info) continue;

        await this.collection(info.junctionName).deleteMany({
          [info.otherColumn]: recordId,
        } as any);
      }
    }
  }

  private async isSystemFilterIfApplicable(
    targetCollection: string,
  ): Promise<Record<string, unknown>> {
    const meta = await this.metadataCache.lookupTableByName(targetCollection);
    const has = !!meta?.columns?.some((c: { name?: string }) => c.name === 'isSystem');
    return has ? { isSystem: { $ne: true } } : {};
  }

  private async unlinkManyToOneInverse(
    relation: any,
    recordId: ObjectId,
    recordData: any,
    targetCollection: string,
  ): Promise<void> {
    const fieldName = relation.propertyName;
    const mappedBy = relation.mappedBy;
    const raw = recordData?.[fieldName];
    const coll = this.collection(targetCollection);

    if (raw != null && raw !== undefined) {
      let parentId: ObjectId;
      try {
        parentId =
          raw instanceof ObjectId ? raw : new ObjectId(String(raw));
      } catch {
        return;
      }
      const parent = await coll.findOne({ _id: parentId });
      if (!parent) {
        return;
      }
      if (Array.isArray(parent[mappedBy])) {
        await coll.updateOne(
          { _id: parentId },
          { $pull: { [mappedBy]: recordId } } as any,
        );
      } else if (
        parent[mappedBy] != null &&
        parent[mappedBy].toString() === recordId.toString()
      ) {
        await coll.updateOne(
          { _id: parentId },
          { $unset: { [mappedBy]: '' } } as any,
        );
      }
      return;
    }

    const alt = await coll.findOne({ [mappedBy]: recordId } as any);
    if (!alt) {
      return;
    }
    if (Array.isArray(alt[mappedBy])) {
      await coll.updateOne(
        { _id: alt._id },
        { $pull: { [mappedBy]: recordId } } as any,
      );
    } else if (
      alt[mappedBy] != null &&
      alt[mappedBy].toString() === recordId.toString()
    ) {
      await coll.updateOne(
        { _id: alt._id },
        { $unset: { [mappedBy]: '' } } as any,
      );
    }
  }

  private async applyOneToManyOnDelete(
    relation: any,
    recordId: ObjectId,
    targetCollection: string,
    fieldValue: any,
    onDelete: TRelationOnDeleteAction,
  ): Promise<void> {
    const mappedBy = relation.mappedBy;
    let targetIds: ObjectId[] = [];
    if (Array.isArray(fieldValue) && fieldValue.length > 0) {
      targetIds = fieldValue.map((v) =>
        v instanceof ObjectId ? v : new ObjectId(v),
      );
    } else {
      const targets = await this.collection(targetCollection)
        .find({ [mappedBy]: recordId } as any)
        .toArray();
      targetIds = targets.map((t) => t._id);
    }

    if (targetIds.length === 0) {
      return;
    }

    if (onDelete === 'RESTRICT') {
      throw new ValidationException(
        `Cannot delete: related records exist in "${targetCollection}" (${relation.propertyName}, onDelete: RESTRICT).`,
        { relation: relation.propertyName, targetCollection },
      );
    }

    const coll = this.collection(targetCollection);
    const sys = await this.isSystemFilterIfApplicable(targetCollection);

    if (onDelete === 'CASCADE') {
      await coll.deleteMany({
        _id: { $in: targetIds },
        ...sys,
      } as any);
      return;
    }

    await coll.updateMany(
      { _id: { $in: targetIds }, ...sys } as any,
      { $set: { [mappedBy]: null } } as any,
    );
  }

  private async applyManyToManyOnDelete(
    tableName: string,
    relation: any,
    recordId: ObjectId,
    onDelete: TRelationOnDeleteAction,
    matchColumnOverride?: string,
  ): Promise<void> {
    const info = this.resolveJunctionInfo(tableName, relation);
    if (!info) return;

    const junctionColl = this.collection(info.junctionName);
    const matchColumn = matchColumnOverride ?? info.selfColumn;

    if (onDelete === 'RESTRICT') {
      const count = await junctionColl.countDocuments({
        [matchColumn]: recordId,
      } as any);
      if (count > 0) {
        const targetCollection =
          relation.targetTableName || relation.targetTable;
        throw new ValidationException(
          `Cannot delete: related records exist in "${targetCollection}" (${relation.propertyName}, onDelete: RESTRICT).`,
          { relation: relation.propertyName, targetCollection },
        );
      }
    }

    await junctionColl.deleteMany({ [matchColumn]: recordId } as any);
  }

  private setM2mPending(
    carrier: any,
    propertyName: string,
    ids: ObjectId[],
  ): void {
    if (!carrier[M2M_PENDING]) {
      carrier[M2M_PENDING] = new Map<string, ObjectId[]>();
    }
    (carrier[M2M_PENDING] as Map<string, ObjectId[]>).set(propertyName, ids);
  }

  private getM2mPending(carrier: any): Map<string, ObjectId[]> | null {
    return (carrier?.[M2M_PENDING] as Map<string, ObjectId[]>) || null;
  }

  private resolveJunctionInfo(currentTable: string, relation: any) {
    return resolveMongoJunctionInfo(currentTable, relation);
  }

  private async writeM2mJunctionsForInsert(
    tableName: string,
    recordId: ObjectId,
    data: any,
  ): Promise<void> {
    const pending = this.getM2mPending(data);
    if (!pending || pending.size === 0) return;

    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata?.relations) return;

    for (const [propertyName, targetIds] of pending.entries()) {
      const relation = metadata.relations.find(
        (r: any) => r.propertyName === propertyName,
      );
      if (!relation) continue;
      const info = this.resolveJunctionInfo(tableName, relation);
      if (!info) continue;
      if (!targetIds.length) continue;

      const rows = targetIds.map((otherId) => ({
        [info.selfColumn]: recordId,
        [info.otherColumn]: otherId,
      }));

      try {
        await this.collection(info.junctionName).insertMany(rows as any, {
          ordered: false,
        });
      } catch (err: any) {
        if (err?.code !== 11000) throw err;
      }
    }
  }

  private async writeM2mJunctionsForUpdate(
    tableName: string,
    recordId: ObjectId,
    data: any,
  ): Promise<void> {
    const pending = this.getM2mPending(data);
    if (!pending || pending.size === 0) return;

    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata?.relations) return;

    for (const [propertyName, targetIds] of pending.entries()) {
      const relation = metadata.relations.find(
        (r: any) => r.propertyName === propertyName,
      );
      if (!relation) continue;
      const info = this.resolveJunctionInfo(tableName, relation);
      if (!info) continue;

      const junctionColl = this.collection(info.junctionName);

      await junctionColl.deleteMany({ [info.selfColumn]: recordId } as any);

      if (!targetIds.length) continue;

      const rows = targetIds.map((otherId) => ({
        [info.selfColumn]: recordId,
        [info.otherColumn]: otherId,
      }));
      try {
        await junctionColl.insertMany(rows as any, { ordered: false });
      } catch (err: any) {
        if (err?.code !== 11000) throw err;
      }
    }
  }

  private async applyOneToOneOnDelete(
    relation: any,
    recordId: ObjectId,
    targetCollection: string,
    fieldValue: any,
    onDelete: TRelationOnDeleteAction,
  ): Promise<void> {
    const mappedBy = relation.mappedBy;
    const coll = this.collection(targetCollection);

    const inverseDocs = await coll
      .find({ [mappedBy]: recordId } as any)
      .toArray();

    let ownedChildId: ObjectId | null = null;
    if (fieldValue != null && fieldValue !== undefined) {
      try {
        ownedChildId =
          fieldValue instanceof ObjectId
            ? fieldValue
            : new ObjectId(String(fieldValue));
      } catch {
        ownedChildId = null;
      }
    }

    const hasInverse = inverseDocs.length > 0;
    const hasOwned = !!ownedChildId;

    if (onDelete === 'RESTRICT' && (hasInverse || hasOwned)) {
      throw new ValidationException(
        `Cannot delete: related records exist in "${targetCollection}" (${relation.propertyName}, onDelete: RESTRICT).`,
        { relation: relation.propertyName, targetCollection },
      );
    }

    const sys = await this.isSystemFilterIfApplicable(targetCollection);

    if (onDelete === 'CASCADE') {
      const byId = new Map<string, ObjectId>();
      for (const d of inverseDocs) {
        byId.set(d._id.toString(), d._id);
      }
      if (ownedChildId) {
        byId.set(ownedChildId.toString(), ownedChildId);
      }
      for (const id of byId.values()) {
        await coll.deleteOne({ _id: id, ...sys } as any);
      }
      return;
    }

    for (const d of inverseDocs) {
      await coll.updateOne(
        { _id: d._id },
        { $unset: { [mappedBy]: '' } } as any,
      );
    }
    if (ownedChildId) {
      await coll.updateOne(
        { _id: ownedChildId },
        { $unset: { [mappedBy]: '' } } as any,
      );
    }
  }

  async count(collectionName: string, filter: any = {}): Promise<number> {
    const collection = this.collection(collectionName);
    return collection.countDocuments(filter);
  }

  private async clearUniqueFKHolders(
    collectionName: string,
    recordId: ObjectId,
    data: any,
  ): Promise<void> {
    const metadata = await this.metadataCache.lookupTableByName(collectionName);
    if (!metadata?.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (!['one-to-one', 'many-to-one'].includes(relation.type)) continue;
      if (relation.isInverse || relation.mappedBy) continue;

      const fieldName = relation.propertyName;
      const hasUnique = this.hasUniqueConstraintOnField(metadata, fieldName);

      if (!hasUnique) continue;

      const newValue = data[fieldName];
      if (newValue == null) continue;

      const newId =
        newValue instanceof ObjectId ? newValue : new ObjectId(newValue);

      await this.collection(collectionName).updateMany(
        {
          [fieldName]: newId,
          _id: { $ne: recordId },
        },
        { $set: { [fieldName]: null } },
      );
    }
  }

  private hasUniqueConstraintOnField(
    metadata: any,
    fieldName: string,
  ): boolean {
    if (!metadata?.uniques) return false;

    const uniques = Array.isArray(metadata.uniques)
      ? metadata.uniques
      : Object.values(metadata.uniques || {});

    for (const unique of uniques) {
      const fields = Array.isArray(unique) ? unique : [unique];
      if (fields.length === 1 && fields[0] === fieldName) {
        return true;
      }
    }

    return false;
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
      then: (onFulfilled: any, onRejected: any) => self.toArray().then(onFulfilled, onRejected),
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
    return this.base.aggregate(pipeline, { ...options, session: this.session });
  }

  bulkWrite(operations: any[], options?: any) {
    return this.base.bulkWrite(operations, { ...options, session: this.session });
  }
}

export class SagaCollection<T extends Document = Document> {
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
      then: (onFulfilled: any, onRejected: any) => self.toArray().then(onFulfilled, onRejected),
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
    return this.session().updateManyByFilter(this.name, filter, update, options);
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
    return this.mongo.getDb().collection(this.name).bulkWrite(operations, options);
  }
}
