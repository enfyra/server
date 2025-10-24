import { Injectable, Logger } from '@nestjs/common';
import { MongoService } from './mongo.service';

type BsonType = 'string' | 'int' | 'long' | 'double' | 'bool' | 'date' | 'objectId' | 'object' | 'array';

/**
 * MongoSchemaMigrationService - Handle MongoDB schema migrations
 * Creates/updates collections with JSON Schema validation and indexes
 */
@Injectable()
export class MongoSchemaMigrationService {
  private readonly logger = new Logger(MongoSchemaMigrationService.name);

  constructor(private readonly mongoService: MongoService) {}

  /**
   * Map application types to BSON types for MongoDB validation
   */
  private getBsonType(type: string): BsonType {
    const typeMap: Record<string, BsonType> = {
      'string': 'string',
      'text': 'string',
      'uuid': 'string',
      'int': 'int',
      'integer': 'int',
      'bigint': 'long',
      'float': 'double',
      'decimal': 'double',
      'boolean': 'bool',
      'date': 'date',
      'datetime': 'date',
      'timestamp': 'date',
      'json': 'object',
      'array': 'array',
    };
    return typeMap[type] || 'string';
  }

  /**
   * Create JSON Schema validation for MongoDB collection
   */
  private createValidationSchema(columns: any[]): any {
    const properties: any = {};
    const required: string[] = [];

    for (const col of columns) {
      // Skip auto-generated fields from validation
      if (col.name === '_id' || col.name === 'createdAt' || col.name === 'updatedAt') {
        continue;
      }
      
      const bsonType = this.getBsonType(col.type);
      
      properties[col.name] = {
        bsonType: col.isNullable ? [bsonType, 'null'] : bsonType,
        description: col.description || col.name,
      };

      // Add to required if not nullable AND no default value AND not generated
      // If field has defaultValue or isGenerated, client doesn't need to provide it
      if (!col.isNullable && !col.defaultValue && !col.isGenerated) {
        required.push(col.name);
      }
    }

    const schema: any = {
      bsonType: 'object',
      properties,
    };

    if (required.length > 0) {
      schema.required = required;
    }

    return schema;
  }

  /**
   * Create partial filter expression to allow multiple null values in unique indexes
   */
  private createPartialFilterForUnique(fields: string[]): any {
    const filter: any = {};
    for (const field of fields) {
      filter[field] = { $exists: true, $ne: null };
    }
    return filter;
  }

  /**
   * Create a new collection with validation schema and indexes
   */
  async createCollection(tableMetadata: any): Promise<void> {
    const db = this.mongoService.getDb();
    const collectionName = tableMetadata.name;

    try {
      // Check if collection already exists
      const collections = await db.listCollections({ name: collectionName }).toArray();
      if (collections.length > 0) {
        this.logger.warn(` Collection ${collectionName} already exists, skipping creation`);
        return;
      }

      this.logger.log(`Creating collection: ${collectionName}`);

      // Create validation schema
      const validationSchema = this.createValidationSchema(tableMetadata.columns || []);

      // Create collection with validation
      await db.createCollection(collectionName, {
        validator: { $jsonSchema: validationSchema },
        validationLevel: 'moderate',
        validationAction: 'error',
      });

      this.logger.log(`Created collection with validation: ${collectionName}`);

      // Create indexes
      await this.createIndexes(
        collectionName,
        tableMetadata.columns || [],
        tableMetadata.uniques || [],
        tableMetadata.indexes || [],
        tableMetadata.relations || []
      );

      this.logger.log(`Collection creation complete: ${collectionName}`);
    } catch (error) {
      this.logger.error(`Failed to create collection ${collectionName}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Update collection validation schema and indexes
   */
  async updateCollection(
    collectionName: string,
    oldMetadata: any,
    newMetadata: any,
  ): Promise<void> {
    const db = this.mongoService.getDb();

    try {
      this.logger.log(`🔧 Updating collection: ${collectionName}`);

      // Update validation schema
      const validationSchema = this.createValidationSchema(newMetadata.columns || []);
      
      await db.command({
        collMod: collectionName,
        validator: { $jsonSchema: validationSchema },
        validationLevel: 'moderate',
        validationAction: 'error',
      });

      this.logger.log(`Updated validation schema for: ${collectionName}`);

      // Drop all indexes (except _id)
      const collection = db.collection(collectionName);
      try {
        await collection.dropIndexes();
        this.logger.log(` Dropped all indexes for ${collectionName}`);
      } catch (error) {
        this.logger.warn(`Failed to drop indexes: ${error.message}`);
      }

      // Recreate indexes
      await this.createIndexes(
        collectionName,
        newMetadata.columns || [],
        newMetadata.uniques || [],
        newMetadata.indexes || [],
        newMetadata.relations || []
      );

      this.logger.log(`Collection update complete: ${collectionName}`);
    } catch (error) {
      this.logger.error(`Failed to update collection ${collectionName}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Drop a collection
   */
  async dropCollection(collectionName: string): Promise<void> {
    const db = this.mongoService.getDb();

    try {
      // Check if collection exists
      const collections = await db.listCollections({ name: collectionName }).toArray();
      if (collections.length === 0) {
        this.logger.warn(` Collection ${collectionName} does not exist, skipping drop`);
        return;
      }

      this.logger.log(` Dropping collection: ${collectionName}`);

      await db.collection(collectionName).drop();

      this.logger.log(`Collection dropped: ${collectionName}`);
    } catch (error) {
      this.logger.error(`Failed to drop collection ${collectionName}: ${error.message}`);
      throw error;
    }
  }

  /**
   * Create indexes for collection
   */
  private async createIndexes(
    collectionName: string,
    columns: any[],
    uniques: any[] = [],
    indexes: any[] = [],
    relations: any[] = []
  ): Promise<void> {
    const db = this.mongoService.getDb();
    const collection = db.collection(collectionName);

    try {
      // Create unique indexes from column definitions
      for (const col of columns) {
        if (col.name === '_id') {
          continue;
        }

        if (col.isPrimary || col.name === 'id') {
          await collection.createIndex(
            { [col.name]: 1 },
            {
              unique: true,
              name: `${collectionName}_${col.name}_unique`,
            }
          );
          this.logger.log(`Created unique index on ${collectionName}.${col.name}`);
        }
      }

      // Create unique constraints
      for (const unique of uniques) {
        const indexSpec: any = {};
        for (const field of unique) {
          indexSpec[field] = 1;
        }
        await collection.createIndex(indexSpec, {
          unique: true,
          name: `${collectionName}_${unique.join('_')}_unique`,
          partialFilterExpression: this.createPartialFilterForUnique(unique),
        });
        this.logger.log(`Created unique index on ${collectionName}: ${unique.join(', ')}`);
      }

      // Create regular indexes
      for (const index of indexes) {
        const indexSpec: any = {};
        for (const field of index) {
          indexSpec[field] = 1;
        }
        await collection.createIndex(indexSpec, {
          name: `${collectionName}_${index.join('_')}_idx`,
        });
        this.logger.log(`Created index on ${collectionName}: ${index.join(', ')}`);
      }

      // Create indexes for relation fields (owner side only)
      for (const relation of relations) {
        // Owner M2O/O2O relations store ObjectId
        if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
          const fieldName = relation.propertyName;
          await collection.createIndex(
            { [fieldName]: 1 },
            { name: `${collectionName}_${fieldName}_fk_idx` }
          );
          this.logger.log(`Created FK index on ${collectionName}.${fieldName}`);
        }

        // Owner M2M relations (without mappedBy) store array of ObjectIds
        if (relation.type === 'many-to-many' && !relation.mappedBy) {
          const fieldName = relation.propertyName;
          await collection.createIndex(
            { [fieldName]: 1 },
            { name: `${collectionName}_${fieldName}_fk_idx` }
          );
          this.logger.log(`Created M2M FK index on ${collectionName}.${fieldName}`);
        }
      }
    } catch (error) {
      this.logger.warn(`Failed to create some indexes for ${collectionName}: ${error.message}`);
    }
  }

}

