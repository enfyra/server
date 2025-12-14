import { Injectable, Logger } from '@nestjs/common';
import { MongoService } from './mongo.service';

type BsonType = 'string' | 'int' | 'long' | 'double' | 'bool' | 'date' | 'objectId' | 'object' | 'array';

@Injectable()
export class MongoSchemaMigrationService {
  private readonly logger = new Logger(MongoSchemaMigrationService.name);

  constructor(private readonly mongoService: MongoService) {}

  private getBsonType(type: string): BsonType {
    const typeMap: Record<string, BsonType> = {
      // String types
      'string': 'string',
      'text': 'string',
      'varchar': 'string',
      'char': 'string',
      'uuid': 'string',
      'richtext': 'string',
      
      // Integer types
      'int': 'int',
      'integer': 'int',
      'smallint': 'int',
      'tinyint': 'int',
      'bigint': 'long',
      
      // Float types
      'float': 'double',
      'double': 'double',
      'decimal': 'double',
      'numeric': 'double',
      'real': 'double',
      
      // Boolean
      'boolean': 'bool',
      'bool': 'bool',
      
      // Date/Time types
      'date': 'date',
      'datetime': 'date',
      'timestamp': 'date',
      
      // JSON/Object types
      'json': 'object',
      'simple-json': 'object',
      
      // Array types
      'array': 'array',
      
      // Enum - stored as string in MongoDB
      'enum': 'string',
    };
    return typeMap[type] || 'string';
  }

  private createValidationSchema(columns: any[]): any {
    const properties: any = {};
    const required: string[] = [];

    for (const col of columns) {
      if (col.name === '_id' || col.name === 'createdAt' || col.name === 'updatedAt') {
        continue;
      }

      const bsonType = this.getBsonType(col.type);

      properties[col.name] = {
        bsonType: col.isNullable ? [bsonType, 'null'] : bsonType,
        description: col.description || col.name,
      };

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

  private createPartialFilterForUnique(fields: string[]): any {
    const filter: any = {};
    for (const field of fields) {
      filter[field] = { $exists: true, $ne: null };
    }
    return filter;
  }

  async createCollection(tableMetadata: any): Promise<void> {
    const db = this.mongoService.getDb();
    const collectionName = tableMetadata.name;

    try {
      const collections = await db.listCollections({ name: collectionName }).toArray();
      if (collections.length > 0) {
        this.logger.warn(` Collection ${collectionName} already exists, skipping creation`);
        return;
      }

      this.logger.log(`Creating collection: ${collectionName}`);

      const validationSchema = this.createValidationSchema(tableMetadata.columns || []);

      await db.createCollection(collectionName, {
        validator: { $jsonSchema: validationSchema },
        validationLevel: 'moderate',
        validationAction: 'error',
      });

      this.logger.log(`Created collection with validation: ${collectionName}`);

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

  async updateCollection(
    collectionName: string,
    oldMetadata: any,
    newMetadata: any,
  ): Promise<void> {
    const db = this.mongoService.getDb();

    try {
      this.logger.log(`🔧 Updating collection: ${collectionName}`);

      const validationSchema = this.createValidationSchema(newMetadata.columns || []);

      await db.command({
        collMod: collectionName,
        validator: { $jsonSchema: validationSchema },
        validationLevel: 'moderate',
        validationAction: 'error',
      });

      this.logger.log(`Updated validation schema for: ${collectionName}`);

      const collection = db.collection(collectionName);
      try {
        await collection.dropIndexes();
        this.logger.log(` Dropped all indexes for ${collectionName}`);
      } catch (error) {
        this.logger.warn(`Failed to drop indexes: ${error.message}`);
      }

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

  async dropCollection(collectionName: string): Promise<void> {
    const db = this.mongoService.getDb();

    try {
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

      for (const relation of relations) {
        if (relation.type === 'many-to-one' || relation.type === 'one-to-one') {
          const fieldName = relation.propertyName;
          await collection.createIndex(
            { [fieldName]: 1 },
            { name: `${collectionName}_${fieldName}_fk_idx` }
          );
          this.logger.log(`Created FK index on ${collectionName}.${fieldName}`);
        }

        if (relation.type === 'many-to-many' && !relation.mappedBy) {
          const fieldName = relation.propertyName;
          await collection.createIndex(
            { [fieldName]: 1 },
            { name: `${collectionName}_${fieldName}_fk_idx` }
          );
          this.logger.log(`Created M2M FK index on ${collectionName}.${fieldName}`);
        }
      }

      await collection.createIndex(
        { createdAt: -1 },
        { name: `${collectionName}_createdAt_idx` }
      );
      this.logger.log(`Created timestamp index on ${collectionName}.createdAt`);

      await collection.createIndex(
        { updatedAt: -1 },
        { name: `${collectionName}_updatedAt_idx` }
      );
      this.logger.log(`Created timestamp index on ${collectionName}.updatedAt`);

      await collection.createIndex(
        { createdAt: -1, updatedAt: -1 },
        { name: `${collectionName}_timestamps_idx` }
      );
      this.logger.log(`Created compound timestamp index on ${collectionName}: createdAt + updatedAt`);

      const timestampFields = columns.filter(col =>
        col.type === 'datetime' || col.type === 'timestamp' || col.type === 'date'
      );

      for (const field of timestampFields) {
        await collection.createIndex(
          { [field.name]: -1 },
          { name: `${collectionName}_${field.name}_idx` }
        );
        this.logger.log(`Created timestamp index on ${collectionName}.${field.name}`);
      }
    } catch (error) {
      this.logger.warn(`Failed to create some indexes for ${collectionName}: ${error.message}`);
    }
  }

}
