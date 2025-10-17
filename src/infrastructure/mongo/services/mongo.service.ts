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
      this.logger.log(`✅ Connected to MongoDB: ${dbName}`);
    } catch (error) {
      this.logger.error('❌ Failed to connect to MongoDB:', error);
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

  /**
   * Apply default values for missing fields
   */
  async applyDefaultValues(tableName: string, data: any): Promise<any> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.columns) {
      return data;
    }

    const result = { ...data };
    
    for (const column of metadata.columns) {
      // Skip if field already has a value
      if (result[column.name] !== undefined && result[column.name] !== null) {
        continue;
      }
      
      // Apply defaultValue if defined
      if (column.defaultValue !== undefined && column.defaultValue !== null) {
        // Parse JSON if defaultValue is string representation
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

  /**
   * Parse JSON fields (simple-json, json types)
   * MongoDB stores JSON natively, so parse string → object before insert/update
   */
  async parseJsonFields(tableName: string, data: any): Promise<any> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.columns) {
      return data;
    }

    const result = { ...data };
    
    for (const column of metadata.columns) {
      const fieldName = column.name;
      const fieldValue = result[fieldName];
      
      // Skip if field not present or already an object
      if (fieldValue === undefined || fieldValue === null) {
        continue;
      }
      
      // Parse simple-json and json types
      if (column.type === 'simple-json' || column.type === 'json') {
        // If it's a string, try to parse
        if (typeof fieldValue === 'string') {
          try {
            result[fieldName] = JSON.parse(fieldValue);
          } catch (error) {
            // If parse fails, keep as string (will fail validation if strict)
            this.logger.warn(`Failed to parse JSON field '${fieldName}': ${error.message}`);
          }
        }
        // If already object/array, keep as is (MongoDB native storage)
      }
    }
    
    return result;
  }

  /**
   * Apply timestamps hook to data (single or array)
   * Forces createdAt and updatedAt, strips client-provided values
   */
  applyTimestamps(data: any | any[]): any | any[] {
    return MongoService.applyTimestampsStatic(data);
  }

  /**
   * Static helper for applying timestamps (can be used without service instance)
   */
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

  async insertOne(collectionName: string, data: any): Promise<any> {
    const collection = this.collection(collectionName);
    
    // 1. Parse JSON fields (string → object)
    const dataParsed = await this.parseJsonFields(collectionName, data);
    
    // 2. Apply default values for missing fields
    const dataWithDefaults = await this.applyDefaultValues(collectionName, dataParsed);
    
    // 3. Process nested relations (create/update related records)
    const dataWithRelations = await this.processNestedRelations(collectionName, dataWithDefaults);
    
    // 4. Apply timestamps
    const dataWithTimestamps = this.applyTimestamps(dataWithRelations);
    
    const result = await collection.insertOne(dataWithTimestamps);
    const insertedId = result.insertedId;
    
    // Update inverse relations (add this record's ID to inverse side)
    // For insert, oldData is null/empty
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
    return results.map(doc => this.mapDocument(doc, tableName));
  }

  async findOne(collectionName: string, filter: any): Promise<any> {
    const collection = this.collection(collectionName);
    const result = await collection.findOne(filter);
    return result ? this.mapDocument(result, collectionName) : null;
  }

  /**
   * Update inverse relations on record update (handle remove old + add new)
   */
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
      
      // CRITICAL: Only process if field is explicitly in newData
      // If field is not in newData → user didn't touch it → keep old relations
      if (!(fieldName in newData)) {
        continue;
      }
      
      const oldValue = oldData?.[fieldName];
      const newValue = newData?.[fieldName];
      
      const targetCollection = relation.targetTableName || relation.targetTable;

      // M2O/O2O
      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        const oldId = oldValue instanceof ObjectId ? oldValue : (oldValue ? new ObjectId(oldValue) : null);
        const newId = newValue instanceof ObjectId ? newValue : (newValue ? new ObjectId(newValue) : null);
        
        // Remove from old target
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
        
        // Add to new target
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
      // O2M/M2M
      else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        const oldIds = Array.isArray(oldValue) ? oldValue.map(v => v instanceof ObjectId ? v : new ObjectId(v)) : [];
        const newIds = Array.isArray(newValue) ? newValue.map(v => v instanceof ObjectId ? v : new ObjectId(v)) : [];
        
        // Find removed IDs
        const removed = oldIds.filter(oldId => !newIds.some(newId => newId.toString() === oldId.toString()));
        // Find added IDs
        const added = newIds.filter(newId => !oldIds.some(oldId => oldId.toString() === newId.toString()));
        
        // Remove from old targets
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
        
        // Add to new targets
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


  /**
   * Process nested relation inserts/updates
   * Handles M2O/O2O (objects) and O2M/M2M (arrays)
   */
  async processNestedRelations(tableName: string, data: any): Promise<any> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return data;
    }

    const processed = { ...data };

    for (const relation of metadata.relations) {
      const fieldName = relation.propertyName;
      
      // Check if field exists in data (even if null/undefined)
      if (!(fieldName in processed)) continue;
      
      const fieldValue = processed[fieldName];
      const targetCollection = relation.targetTableName || relation.targetTable;

      // Handle explicit null/undefined → clear relation
      if (fieldValue === null || fieldValue === undefined) {
        processed[fieldName] = null;
        continue;
      }

      // Handle M2O/O2O: single object
      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        // Skip if not a plain object
        if (typeof fieldValue !== 'object' || 
            Array.isArray(fieldValue) ||
            fieldValue instanceof ObjectId ||
            fieldValue instanceof Date) {
          continue;
        }

        const { _id: nestedId, id, ...nestedData } = fieldValue;
        const hasDataToUpdate = Object.keys(nestedData).length > 0;

        if (!nestedId && !id) {
          // Case 1: No ID → Create new
          if (hasDataToUpdate) {
            const inserted = await this.insertOne(targetCollection, nestedData);
            processed[fieldName] = new ObjectId(inserted._id);
          } else {
            // Empty object {} → treat as null
            processed[fieldName] = null;
          }
        } else if (hasDataToUpdate) {
          // Case 2: Has ID + data → Update existing
          const idToUse = nestedId || id;
          await this.updateOne(targetCollection, idToUse, nestedData);
          processed[fieldName] = typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        } else {
          // Case 3: Only ID, no data → Just convert to ObjectId
          const idToUse = nestedId || id;
          processed[fieldName] = typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse;
        }
      }
      // Handle O2M/M2M: array of objects
      else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        // Allow empty array [] to clear relations
        if (!Array.isArray(fieldValue)) {
          processed[fieldName] = [];
          continue;
        }

        const processedArray = [];
        for (const item of fieldValue) {
          if (typeof item !== 'object' || item instanceof ObjectId || item instanceof Date) {
            // Already an ObjectId or primitive → keep as is
            processedArray.push(item instanceof ObjectId ? item : new ObjectId(item));
            continue;
          }

          const { _id: itemId, id: itemIdAlt, ...itemData } = item;
          const hasDataToUpdate = Object.keys(itemData).length > 0;

          if (!itemId && !itemIdAlt) {
            // No ID → Create new (only if has data)
            if (hasDataToUpdate) {
              const inserted = await this.insertOne(targetCollection, itemData);
              processedArray.push(new ObjectId(inserted._id));
            }
            // Empty object {} in array → skip
          } else if (hasDataToUpdate) {
            // Has ID + data → Update existing
            const idToUse = itemId || itemIdAlt;
            await this.updateOne(targetCollection, idToUse, itemData);
            processedArray.push(typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse);
          } else {
            // Only ID → Just convert to ObjectId
            const idToUse = itemId || itemIdAlt;
            processedArray.push(typeof idToUse === 'string' ? new ObjectId(idToUse) : idToUse);
          }
        }
        processed[fieldName] = processedArray;
      }
    }

    return processed;
  }

  /**
   * Apply update timestamp hook to data
   * Forces updatedAt, strips createdAt, _id, id
   */
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
    
    // Get old record to compare relations
    const oldRecord = await this.findOne(collectionName, { _id: objectId });
    
    // 1. Parse JSON fields (string → object)
    const dataParsed = await this.parseJsonFields(collectionName, data);
    
    // 2. Process nested relations (create/update related records)
    const dataWithRelations = await this.processNestedRelations(collectionName, dataParsed);
    
    // 3. Apply update timestamp
    const dataWithTimestamp = this.applyUpdateTimestamp(dataWithRelations);
    
    await collection.updateOne({ _id: objectId }, { $set: dataWithTimestamp });
    
    // Update inverse relations (handle both add and remove)
    await this.updateInverseRelationsOnUpdate(collectionName, objectId, oldRecord, dataWithRelations);
    
    return this.findOne(collectionName, { _id: objectId });
  }

  async deleteOne(collectionName: string, id: string): Promise<boolean> {
    const collection = this.collection(collectionName);
    const objectId = new ObjectId(id);
    
    // Get record before delete to clean up inverse relations
    const record = await this.findOne(collectionName, { _id: objectId });
    
    if (!record) {
      return false;
    }
    
    // Clean up inverse relations before delete
    await this.cleanupInverseRelationsOnDelete(collectionName, objectId, record);
    
    const result = await collection.deleteOne({ _id: objectId });
    return result.deletedCount > 0;
  }

  /**
   * Clean up inverse relations when a record is deleted
   * Removes this record's ID from all related records
   */
  async cleanupInverseRelationsOnDelete(tableName: string, recordId: ObjectId, recordData: any): Promise<void> {
    const metadata = await this.metadataCache.lookupTableByName(tableName);
    if (!metadata || !metadata.relations) {
      return;
    }

    for (const relation of metadata.relations) {
      if (!relation.inversePropertyName) continue;
      
      const fieldName = relation.propertyName;
      const fieldValue = recordData?.[fieldName];
      
      if (!fieldValue) continue;
      
      const targetCollection = relation.targetTableName || relation.targetTable;

      // M2O/O2O: Remove from single target
      if (['many-to-one', 'one-to-one'].includes(relation.type)) {
        const targetId = fieldValue instanceof ObjectId ? fieldValue : new ObjectId(fieldValue);
        
        if (relation.type === 'many-to-one') {
          // Inverse is O2M (array) → $pull
          await this.getDb().collection(targetCollection).updateOne(
            { _id: targetId },
            { $pull: { [relation.inversePropertyName]: recordId } } as any
          );
        } else {
          // Inverse is O2O (single) → $unset
          await this.getDb().collection(targetCollection).updateOne(
            { _id: targetId },
            { $unset: { [relation.inversePropertyName]: "" } } as any
          );
        }
      }
      // O2M/M2M: Remove from multiple targets
      else if (['one-to-many', 'many-to-many'].includes(relation.type)) {
        const targetIds = Array.isArray(fieldValue) 
          ? fieldValue.map(v => v instanceof ObjectId ? v : new ObjectId(v))
          : [];
        
        for (const targetId of targetIds) {
          if (relation.type === 'one-to-many') {
            // Inverse is M2O (single) → $unset
            await this.getDb().collection(targetCollection).updateOne(
              { _id: targetId },
              { $unset: { [relation.inversePropertyName]: "" } } as any
            );
          } else {
            // Inverse is M2M (array) → $pull
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

  private mapDocument(doc: any, tableName?: string): any {
    if (!doc) return doc;
    
    // Recursively convert ObjectId and Date to proper JSON types
    const convertTypes = (obj: any): any => {
      if (obj instanceof ObjectId) {
        return obj.toString();
      }
      
      if (obj instanceof Date) {
        return obj.toISOString();
      }
      
      if (Array.isArray(obj)) {
        return obj.map(item => convertTypes(item));
      }
      
      if (obj !== null && typeof obj === 'object') {
        const converted: any = {};
        for (const [key, value] of Object.entries(obj)) {
          converted[key] = convertTypes(value);
        }
        return converted;
      }
      
      return obj;
    };
    
    return convertTypes(doc);
  }

  private extractDbName(uri: string): string {
    const match = uri.match(/\/([^/?]+)(\?|$)/);
    return match ? match[1] : 'enfyra';
  }
}

