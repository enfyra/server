import { Injectable, OnModuleInit, OnModuleDestroy, Logger, Inject, forwardRef } from '@nestjs/common';
import { MongoClient, Db, Collection, Document, ObjectId } from 'mongodb';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';

@Injectable()
export class MongoService implements OnModuleInit, OnModuleDestroy {
  private client: MongoClient;
  private db: Db;
  private readonly logger = new Logger(MongoService.name);

  constructor(
    @Inject(forwardRef(() => MetadataCacheService))
    private readonly metadataCache: MetadataCacheService,
  ) {}

  async onModuleInit() {
    const dbType = process.env.DB_TYPE;
    
    if (dbType !== 'mongodb') {
      this.logger.log('DB_TYPE is not mongodb, skipping MongoDB initialization');
      return;
    }

    const uri = process.env.MONGO_URI;
    
    if (!uri) {
      throw new Error('MONGO_URI is not defined in environment variables');
    }

    try {
      this.client = new MongoClient(uri);
      await this.client.connect();
      
      const dbName = this.extractDbName(uri);
      this.db = this.client.db(dbName);
      
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

  collection<T extends Document = Document>(name: string): Collection<T> {
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

      if (column.type === 'simple-json' || column.type === 'json') {
        if (typeof fieldValue === 'string') {
          try {
            result[fieldName] = JSON.parse(fieldValue);
          } catch (error) {
            this.logger.warn(`Failed to parse JSON field '${fieldName}': ${error.message}`);
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
      return data.map(record => {
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
      const isInverse = relation.type === 'one-to-many' ||
                       (relation.type === 'many-to-many' && relation.mappedBy) ||
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
    const dataWithDefaults = await this.applyDefaultValues(collectionName, dataParsed);
    const dataWithRelations = await this.processNestedRelations(collectionName, dataWithDefaults);
    const dataWithoutInverse = await this.stripInverseRelations(collectionName, dataWithRelations);
    const dataWithTimestamps = this.applyTimestamps(dataWithoutInverse);

    let result;
    let insertedId;
    try {
      result = await collection.insertOne(dataWithTimestamps);
      insertedId = result.insertedId;
    } catch (err) {
      console.error(`[insertOne] Validation error for ${collectionName}:`, err.errInfo);
      throw err;
    }

    await this.updateInverseRelationsOnUpdate(collectionName, insertedId, {}, dataWithRelations);

    return {
      ...dataWithTimestamps,
      _id: insertedId,
      id: insertedId.toString(),
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

  async updateInverseRelationsOnUpdate(tableName: string, recordId: ObjectId, oldData: any, newData: any): Promise<void> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (!relation.inversePropertyName) {
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
        const oldId = oldValue instanceof ObjectId ? oldValue : (oldValue ? (typeof oldValue === 'object' && oldValue._id ? new ObjectId(oldValue._id) : new ObjectId(oldValue)) : null);
        const newId = newValue instanceof ObjectId ? newValue : (newValue ? (typeof newValue === 'object' && newValue._id ? new ObjectId(newValue._id) : new ObjectId(newValue)) : null);

        if (oldId && (!newId || oldId.toString() !== newId.toString())) {
          if (relation.type === 'many-to-one') {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: oldId },
              { $pull: { [relation.inversePropertyName]: recordId } } as any
            );
          } else {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: oldId },
              { $unset: { [relation.inversePropertyName]: "" } } as any
            );
          }
        }

        if (newId && (!oldId || oldId.toString() !== newId.toString())) {
          if (relation.type === 'many-to-one') {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: newId },
              { $addToSet: { [relation.inversePropertyName]: recordId } }
            );
          } else {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: newId },
              { $set: { [relation.inversePropertyName]: recordId } }
            );
          }
        }
      }
      else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        const oldIds = Array.isArray(oldValue) ? oldValue.map(v => {
          if (v instanceof ObjectId) return v;
          if (typeof v === 'object' && v._id) return new ObjectId(v._id);
          return new ObjectId(v);
        }) : [];
        const newIds = Array.isArray(newValue) ? newValue.map(v => {
          if (v instanceof ObjectId) return v;
          if (typeof v === 'object' && v._id) return new ObjectId(v._id);
          return new ObjectId(v);
        }) : [];

        const removed = oldIds.filter(oldId => !newIds.some(newId => newId.toString() === oldId.toString()));
        const added = newIds.filter(newId => !oldIds.some(oldId => oldId.toString() === newId.toString()));

        for (const targetId of removed) {
          if (relation.type === 'one-to-many') {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $unset: { [relation.inversePropertyName]: "" } } as any
            );
          } else {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $pull: { [relation.inversePropertyName]: recordId } } as any
            );
          }
        }

        for (const targetId of added) {
          if (relation.type === 'one-to-many') {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $set: { [relation.inversePropertyName]: recordId } }
            );
          } else {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $addToSet: { [relation.inversePropertyName]: recordId } }
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

      const isInverse = relation.type === 'one-to-many' ||
                       (relation.type === 'many-to-many' && relation.mappedBy) ||
                       (relation.type === 'one-to-one' && (relation.mappedBy || relation.isInverse));

      if (isInverse) {
        continue;
      }

      const fieldValue = processed[fieldName];
      const targetCollection = relation.targetTableName || relation.targetTable;

      if (fieldValue === null || fieldValue === undefined) {
        if (relation.type === 'many-to-many') {
          processed[fieldName] = [];
        } else {
          processed[fieldName] = null;
        }
        continue;
      }

      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        if (typeof fieldValue !== 'object' || 
            Array.isArray(fieldValue) ||
            fieldValue instanceof ObjectId ||
            fieldValue instanceof Date) {
          continue;
        }

        const { _id: nestedId, id, ...nestedData } = fieldValue;
        const hasDataToUpdate = Object.keys(nestedData).length > 0;

        if (!nestedId && !id) {
          if (hasDataToUpdate) {
            const inserted = await this.insertOne(targetCollection, nestedData);
            processed[fieldName] = new ObjectId(inserted._id);
          } else {
            processed[fieldName] = null;
          }
        } else if (hasDataToUpdate) {
          const idToUse = nestedId || id;
          await this.updateOne(targetCollection, idToUse, nestedData);
          processed[fieldName] = typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        } else {
          const idToUse = nestedId || id;
          processed[fieldName] = typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        }
      }
      else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        if (!Array.isArray(fieldValue)) {
          processed[fieldName] = [];
          continue;
        }

        const processedArray = [];
        for (const item of fieldValue) {
          if (typeof item !== 'object' || item instanceof ObjectId || item instanceof Date) {
            processedArray.push(item instanceof ObjectId ? item : new ObjectId(item));
            continue;
          }

          const { _id: itemId, id: itemIdAlt, ...itemData } = item;
          const hasDataToUpdate = Object.keys(itemData).length > 0;

          if (!itemId && !itemIdAlt) {
            if (hasDataToUpdate) {
              const inserted = await this.insertOne(targetCollection, itemData);
              processedArray.push(new ObjectId(inserted._id));
            }
          } else if (hasDataToUpdate) {
            const idToUse = itemId || itemIdAlt;
            await this.updateOne(targetCollection, idToUse, itemData);
            processedArray.push(typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse);
          } else {
            const idToUse = itemId || itemIdAlt;
            processedArray.push(typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse);
          }
        }
        processed[fieldName] = processedArray;
      }
    }

    return processed;
  }

  async stripHiddenNullFields(collectionName: string, data: any): Promise<any> {
    const tableMetadata = await this.metadataCache.getTableMetadata(collectionName);
    if (!tableMetadata || !tableMetadata.columns) return data;

    const filteredData = { ...data };
    
    for (const column of tableMetadata.columns) {
      if (column.isHidden === true && column.name in filteredData && filteredData[column.name] === null) {
        delete filteredData[column.name];
      }
    }

    return filteredData;
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
    const dataWithRelations = await this.processNestedRelations(collectionName, dataParsed);
    const dataWithoutInverse = await this.stripInverseRelations(collectionName, dataWithRelations);
    const dataWithoutHiddenNulls = await this.stripHiddenNullFields(collectionName, dataWithoutInverse);
    const dataWithTimestamp = this.applyUpdateTimestamp(dataWithoutHiddenNulls);

    await collection.updateOne({ _id: objectId }, { $set: dataWithTimestamp });

    await this.updateInverseRelationsOnUpdate(collectionName, objectId, oldRecord, dataWithRelations);
    
    return this.findOne(collectionName, { _id: objectId });
  }

  async deleteOne(collectionName: string, id: string): Promise<boolean> {
    const collection = this.collection(collectionName);
    const objectId = new ObjectId(id);

    const record = await this.findOne(collectionName, { _id: objectId });
    if (!record) {
      return false;
    }

    await this.cleanupInverseRelationsOnDelete(collectionName, objectId, record);

    const result = await collection.deleteOne({ _id: objectId });
    return result.deletedCount > 0;
  }

  async cleanupInverseRelationsOnDelete(tableName: string, recordId: ObjectId, recordData: any): Promise<void> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata?.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (!relation.inversePropertyName) continue;

      const fieldName = relation.propertyName;
      const fieldValue = recordData?.[fieldName];
      const targetCollection = relation.targetTableName || relation.targetTable;

      if (!fieldValue && !['one-to-many', 'many-to-many'].includes(relation.type)) {
        continue;
      }

      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        const targetId = fieldValue instanceof ObjectId ? fieldValue : new ObjectId(fieldValue);

        if (relation.type === 'one-to-one') {
          await this.getDb().collection(targetCollection).updateOne(
            { _id: targetId },
            { $unset: { [relation.inversePropertyName]: "" } } as any
          );
        }
      }
      else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        let targetIds = [];

        if (Array.isArray(fieldValue) && fieldValue.length > 0) {
          targetIds = fieldValue.map(v => v instanceof ObjectId ? v : new ObjectId(v));
        } else {
          const targets = await this.getDb().collection(targetCollection)
            .find({ [relation.inversePropertyName]: recordId })
            .toArray();
          targetIds = targets.map(t => t._id);
        }

        for (const targetId of targetIds) {
          if (relation.type === 'one-to-many') {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $set: { [relation.inversePropertyName]: null } }
            );
          } else {
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $pull: { [relation.inversePropertyName]: recordId } } as any
            );
          }
        }
      }
    }
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

