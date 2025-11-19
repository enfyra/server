import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MongoSchemaMigrationService } from '../../../infrastructure/mongo/services/mongo-schema-migration.service';
import { MongoService } from '../../../infrastructure/mongo/services/mongo.service';
import { MongoSchemaMigrationLockService } from '../../../infrastructure/mongo/services/mongo-schema-migration-lock.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import {
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../core/exceptions/custom-exceptions';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { CreateTableDto } from '../dto/create-table.dto';

/**
 * MongoTableHandlerService - Manages MongoDB collection metadata and validation
 * 1. Validates and saves metadata to DB
 * 2. Creates/updates JSON Schema validation via MongoSchemaMigrationService
 * 3. Manages indexes
 */
@Injectable()
export class MongoTableHandlerService {
  private logger = new Logger(MongoTableHandlerService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private schemaMigrationService: MongoSchemaMigrationService,
    private mongoService: MongoService,
    private schemaMigrationLockService: MongoSchemaMigrationLockService,
    private metadataCacheService: MetadataCacheService,
    private loggingService: LoggingService,
  ) {}

  private validateRelations(relations: any[]) {
    for (const relation of relations || []) {
      if (relation.type === 'one-to-many' && !relation.inversePropertyName) {
        throw new ValidationException(
          `One-to-many relation '${relation.propertyName}' must have inversePropertyName`,
          {
            relationName: relation.propertyName,
            relationType: relation.type,
            missingField: 'inversePropertyName',
          },
        );
      }
    }
  }

  private async validateNoDuplicateInverseRelation(
    sourceTableId: any,
    sourceTableName: string,
    newRelations: any[],
  ): Promise<void> {
    const { ObjectId } = require('mongodb');
    const querySourceId = typeof sourceTableId === 'string' ? new ObjectId(sourceTableId) : sourceTableId;

    for (const rel of newRelations || []) {
      let targetTableId: any;
      if (typeof rel.targetTable === 'object' && rel.targetTable._id) {
        targetTableId = typeof rel.targetTable._id === 'string' ? new ObjectId(rel.targetTable._id) : rel.targetTable._id;
      } else if (typeof rel.targetTable === 'object' && rel.targetTable.id) {
        targetTableId = typeof rel.targetTable.id === 'string' ? new ObjectId(rel.targetTable.id) : rel.targetTable.id;
      } else if (typeof rel.targetTable === 'string') {
        const targetTableRecord = await this.queryBuilder.findOneWhere('table_definition', { name: rel.targetTable });
        if (targetTableRecord) {
          targetTableId = typeof targetTableRecord._id === 'string' ? new ObjectId(targetTableRecord._id) : targetTableRecord._id;
        } else {
          continue;
        }
      } else {
        continue;
      }

      if (!targetTableId) continue;

      let targetTableName: string;
      const targetTableRecord = await this.queryBuilder.findOneWhere('table_definition', { _id: targetTableId });
      if (targetTableRecord) {
        targetTableName = targetTableRecord.name;
      } else {
        continue;
      }

      let inverseExists = false;
      let inverseRelationInfo = null;

      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const targetRelations = await this.queryBuilder.findWhere('relation_definition', {
          sourceTable: targetTableId,
          targetTable: querySourceId,
        });

        const matchingRelation = targetRelations.find((tr: any) => {
          if (rel.inversePropertyName) {
            return tr.propertyName === rel.inversePropertyName || tr.inversePropertyName === rel.propertyName;
          } else {
            return tr.inversePropertyName === rel.propertyName;
          }
        });

        if (matchingRelation) {
          inverseExists = true;
          inverseRelationInfo = {
            table: targetTableName,
            propertyName: matchingRelation.propertyName,
            type: matchingRelation.type,
          };
        }
      } else if (rel.type === 'one-to-many') {
        if (!rel.inversePropertyName) continue;

        const targetRelations = await this.queryBuilder.findWhere('relation_definition', {
          sourceTable: targetTableId,
          targetTable: querySourceId,
          propertyName: rel.inversePropertyName,
        });

        const matchingRelation = targetRelations.find((tr: any) => 
          ['many-to-one', 'one-to-one'].includes(tr.type)
        );

        if (matchingRelation) {
          inverseExists = true;
          inverseRelationInfo = {
            table: targetTableName,
            propertyName: matchingRelation.propertyName,
            type: matchingRelation.type,
          };
        }
      } else if (rel.type === 'many-to-many') {
        if (!rel.inversePropertyName) continue;

        const targetRelations = await this.queryBuilder.findWhere('relation_definition', {
          sourceTable: targetTableId,
          targetTable: querySourceId,
          propertyName: rel.inversePropertyName,
          type: 'many-to-many',
        });

        if (targetRelations.length > 0) {
          inverseExists = true;
          inverseRelationInfo = {
            table: targetTableName,
            propertyName: targetRelations[0].propertyName,
            type: targetRelations[0].type,
          };
        }
      }

      if (inverseExists && inverseRelationInfo) {
        throw new ValidationException(
          `Cannot create relation '${rel.propertyName}' (${rel.type}) from '${sourceTableName}' to '${targetTableName}': ` +
          `The inverse relation already exists on target table '${targetTableName}' as '${inverseRelationInfo.propertyName}' (${inverseRelationInfo.type}). ` +
          `Relations should be created on ONLY ONE side. System automatically handles the inverse relation. ` +
          `Please remove the relation from '${targetTableName}' or update it instead of creating a duplicate.`,
          {
            sourceTable: sourceTableName,
            targetTable: targetTableName,
            relationName: rel.propertyName,
            relationType: rel.type,
            existingInverseTable: targetTableName,
            existingInverseRelation: inverseRelationInfo.propertyName,
            existingInverseType: inverseRelationInfo.type,
          },
        );
      }
    }
  }

  /**
   * Migrate renamed fields in background (fire & forget)
   * Uses $rename to move data from old field to new field
   */
  private migrateRenamedFieldsInBackground(
    renamedColumns: Array<{ oldName: string; newName: string; collectionName: string }>
  ): void {
    // Fire & forget - don't await
    (async () => {
      const db = this.mongoService.getDb();
      
      for (const { oldName, newName, collectionName } of renamedColumns) {
        try {
          this.logger.log(`[Background] Renaming field '${oldName}' → '${newName}' in ${collectionName}`);
          
          // MongoDB $rename atomically renames field
          const result = await db.collection(collectionName).updateMany(
            { [oldName]: { $exists: true } }, // Only documents that have the old field
            { $rename: { [oldName]: newName } }
          );
          
          this.logger.log(`  [Background] Renamed ${result.modifiedCount} documents in ${collectionName}`);
        } catch (error) {
          this.logger.error(`  [Background] Failed to rename field in ${collectionName}:`, error.message);
        }
      }
    })().catch(err => {
      this.logger.error('Background migration error:', err);
    });
  }

  /**
   * Drop relation fields before metadata update
   * Drops fields based on NEW relations (from body) to ensure clean state
   */
  private async dropRelationFieldsBeforeUpdate(
    newRelations: any[],
    sourceTableName: string,
  ): Promise<void> {
    const db = this.mongoService.getDb();
    
    if (!newRelations || newRelations.length === 0) {
      return;
    }
    
    this.logger.log(`Dropping relation fields for ${newRelations.length} relation(s) before update`);
    
    for (const relation of newRelations) {
      // Get target table name
      let targetTableName: string;
      if (typeof relation.targetTable === 'object' && relation.targetTable.name) {
        targetTableName = relation.targetTable.name;
      } else if (typeof relation.targetTable === 'string') {
        targetTableName = relation.targetTable;
      } else {
        // Need to lookup targetTable by ID
        const { ObjectId } = require('mongodb');
        const targetId = relation.targetTable._id || relation.targetTable;
        const targetTableRecord = await this.queryBuilder.findOneWhere('table_definition', { 
          _id: typeof targetId === 'string' ? new ObjectId(targetId) : targetId 
        });
        if (targetTableRecord) {
          targetTableName = targetTableRecord.name;
        } else {
          this.logger.warn(`Cannot find target table for relation ${relation.propertyName}, skipping drop`);
          continue;
        }
      }
      
      const sourceFieldName = relation.propertyName;
      const inverseFieldName = relation.inversePropertyName;
      
      // 1. DROP source field from ALL records in source table
      if (sourceFieldName) {
        await db.collection(sourceTableName).updateMany(
          {}, // Empty filter = all documents
          { $unset: { [sourceFieldName]: "" } }
        );
        this.logger.log(`  Dropped '${sourceFieldName}' from '${sourceTableName}'`);
      }
      
      // 2. DROP inverse field from ALL records in target table (if has inverse)
      if (inverseFieldName && targetTableName) {
        await db.collection(targetTableName).updateMany(
          {}, // Empty filter = all documents
          { $unset: { [inverseFieldName]: "" } }
        );
        this.logger.log(`  Dropped '${inverseFieldName}' from '${targetTableName}'`);
      }
    }
    
    this.logger.log('✨ All relation fields dropped. Will be recreated by runtime logic.');
  }

  async createTable(body: CreateTableDto) {
    return await this.runWithSchemaLock(`mongo:create:${body?.name || 'unknown'}`, async () => {
    if (/[A-Z]/.test(body?.name)) {
      throw new ValidationException('Table name must be lowercase (no uppercase letters).', {
        tableName: body?.name,
      });
    }
    if (!/^[a-z0-9_]+$/.test(body?.name)) {
      throw new ValidationException('Table name must be snake_case (a-z, 0-9, _).', {
        tableName: body?.name,
      });
    }

    this.validateRelations(body.relations);

    try {
      const db = this.queryBuilder.getMongoDb();
      
      // Check metadata and physical collection SEPARATELY
      const collections = await db.listCollections({ name: body.name }).toArray();
      const hasCollection = collections.length > 0;
      
      const existing = await this.queryBuilder.findOneWhere('table_definition', {
        name: body.name,
      });

      // If metadata exists, throw error (normal case)
      if (existing) {
        throw new DuplicateResourceException(
          'table_definition',
          'name',
          body.name
        );
      }

      // If physical collection exists but no metadata (mismatch) -> drop physical collection first
      if (hasCollection && !existing) {
        this.logger.warn(`Mismatch detected: Physical collection "${body.name}" exists but no metadata found. Dropping physical collection...`);
        try {
          await db.collection(body.name).drop();
          this.logger.log(`Physical collection "${body.name}" dropped successfully`);
        } catch (dropError) {
          this.logger.error(`Failed to drop physical collection "${body.name}": ${dropError.message}`);
          throw new DatabaseException(
            `Failed to drop existing physical collection "${body.name}": ${dropError.message}`,
            { collectionName: body.name, operation: 'drop_existing_collection' }
          );
        }
      }

      // MongoDB: primary key must be named "_id", not "id"
      const idCol = body.columns.find(
        (col: any) => col.name === '_id' && col.isPrimary,
      );
      if (!idCol) {
        throw new ValidationException(
          `Table must contain a column named "_id" with isPrimary = true.`,
          { tableName: body.name }
        );
      }

      const validTypes = ['int', 'uuid'];
      if (!validTypes.includes(idCol.type)) {
        throw new ValidationException(
          `The primary column "_id" must be of type int or uuid.`,
          { tableName: body.name, idColumnType: idCol.type }
        );
      }

      const primaryCount = body.columns.filter(
        (col: any) => col.isPrimary,
      ).length;
      if (primaryCount !== 1) {
        throw new ValidationException(
          `Only one column is allowed to have isPrimary = true.`,
          { tableName: body.name, primaryCount }
        );
      }

      validateUniquePropertyNames(body.columns || [], body.relations || []);

      body.isSystem = false;

      // Insert table metadata
      const tableRecord = await this.queryBuilder.insertAndGet('table_definition', {
        name: body.name,
        isSystem: body.isSystem,
        alias: body.alias,
        description: body.description,
        uniques: JSON.stringify(body.uniques || []),
        indexes: JSON.stringify(body.indexes || []),
        // NOTE: columns and relations are inverse one-to-many - NOT stored
        // They will be computed via $lookup based on metadata
      });

      // MongoDB returns _id, not id
      const { ObjectId } = require('mongodb');
      const tableId = typeof tableRecord._id === 'string' ? new ObjectId(tableRecord._id) : tableRecord._id;

      // Insert columns
      const insertedColumnIds = [];
      try {
        if (body.columns?.length > 0) {
          for (const col of body.columns) {
            const columnRecord = await this.queryBuilder.insertAndGet('column_definition', {
              name: col.name,
              type: col.type,
              isPrimary: col.isPrimary || false,
              isGenerated: col.isGenerated || false,
              isNullable: col.isNullable ?? true,
              isSystem: col.isSystem || false,
              isUpdatable: col.isUpdatable ?? true,
              isHidden: col.isHidden || false,
              defaultValue: col.defaultValue ? JSON.stringify(col.defaultValue) : null,
              options: col.options ? JSON.stringify(col.options) : null,
              description: col.description,
              placeholder: col.placeholder,
              table: tableId, // MongoDB uses 'table' field, not 'tableId'
            });
            const colId = typeof columnRecord._id === 'string' ? new ObjectId(columnRecord._id) : columnRecord._id;
            insertedColumnIds.push(colId);
            this.logger.log(`   Column inserted: ${col.name}`);
          }
        }
      } catch (error) {
        // Rollback: delete table metadata if column insert fails
        this.logger.error(`   Failed to insert columns, rolling back table creation`);
        await this.queryBuilder.deleteById('table_definition', tableId);
        throw new ValidationException(
          `Failed to create table: ${error.message}`,
          { tableName: body.name, error: error.message }
        );
      }

      // Insert relations
      const insertedRelationIds = [];
      try {
        if (body.relations?.length > 0) {
          for (const rel of body.relations) {
            // Get target table ObjectId
            let targetTableObjectId;
            if (typeof rel.targetTable === 'object' && rel.targetTable._id) {
              targetTableObjectId = typeof rel.targetTable._id === 'string' ? new ObjectId(rel.targetTable._id) : rel.targetTable._id;
            } else if (typeof rel.targetTable === 'string') {
              // Lookup table by name
              const targetTableRecord = await this.queryBuilder.findOneWhere('table_definition', { name: rel.targetTable });
              if (targetTableRecord) {
                targetTableObjectId = typeof targetTableRecord._id === 'string' ? new ObjectId(targetTableRecord._id) : targetTableRecord._id;
              }
            }
            
            if (!targetTableObjectId) {
              throw new ValidationException(
                `Target table '${rel.targetTable}' not found for relation ${rel.propertyName}`,
                { tableName: body.name, relation: rel.propertyName }
              );
            }
            
            const relationRecord = await this.queryBuilder.insertAndGet('relation_definition', {
              propertyName: rel.propertyName,
              type: rel.type,
              sourceTable: tableId, // ObjectId
              targetTable: targetTableObjectId, // ObjectId
              targetTableName: typeof rel.targetTable === 'string' ? rel.targetTable : rel.targetTable.name,
              sourceTableName: body.name,
              inversePropertyName: rel.inversePropertyName,
              isNullable: rel.isNullable ?? true,
              isSystem: rel.isSystem || false,
              description: rel.description,
            });
            const relId = typeof relationRecord._id === 'string' ? new ObjectId(relationRecord._id) : relationRecord._id;
            insertedRelationIds.push(relId);
            this.logger.log(`   Relation inserted: ${rel.propertyName}`);
          }

          // NOTE: relations is inverse O2M - NOT stored in table_definition
          // Computed via $lookup based on relation.sourceTable field
        }
      } catch (error) {
        // Rollback: delete table metadata and columns if relation insert fails
        this.logger.error(`   Failed to insert relations, rolling back table creation`);
        
        // Delete inserted columns
        for (const colId of insertedColumnIds) {
          await this.queryBuilder.deleteById('column_definition', colId);
        }
        
        // Delete table
        await this.queryBuilder.deleteById('table_definition', tableId);
        
        throw new ValidationException(
          `Failed to create table: ${error.message}`,
          { tableName: body.name, error: error.message }
        );
      }

      // Create route
      const existingRoute = await this.queryBuilder.findOneWhere('route_definition', {
        path: `/${body.name}`,
      });

      if (!existingRoute) {
        await this.queryBuilder.insert({
          table: 'route_definition',
          data: {
            path: `/${body.name}`,
            mainTable: tableId, // MongoDB uses 'mainTable' field (ObjectId)
            isEnabled: true,
            isSystem: false,
            icon: 'lucide:table',
            // Initialize inverse fields
            publishedMethods: [],
            routePermissions: [],
            handlers: [],
            hooks: [],
          },
        });
        this.logger.log(`Route /${body.name} created for collection ${body.name}`);
      }

      // Fetch full metadata
      const fullMetadata = await this.getFullTableMetadata(tableId);

      // Create collection with validation and indexes
      await this.schemaMigrationService.createCollection(fullMetadata);

      this.logger.log(`Collection created: ${body.name} (metadata + validation + indexes)`);
      return fullMetadata;
    } catch (error) {
      this.loggingService.error('Collection creation failed', {
        context: 'createTable',
        error: error.message,
        stack: error.stack,
        collectionName: body?.name,
      });

      throw new DatabaseException(
        `Failed to create collection: ${error.message}`,
        {
          collectionName: body?.name,
          operation: 'create',
        },
      );
    }
    });
  }

  async updateTable(id: any, body: CreateTableDto) {
    return await this.runWithSchemaLock(`mongo:update:${id}`, async () => {
    if (body.name && /[A-Z]/.test(body.name)) {
      throw new ValidationException('Table name must be lowercase.', {
        tableName: body.name,
      });
    }
    if (body.name && !/^[a-z0-9_]+$/.test(body.name)) {
      throw new ValidationException('Table name must be snake_case.', {
        tableName: body.name,
      });
    }

    this.validateRelations(body.relations);

    try {
      // MongoDB uses _id
      const { ObjectId } = require('mongodb');
      const queryId = typeof id === 'string' ? new ObjectId(id) : id;
      
      const exists = await this.queryBuilder.findOneWhere('table_definition', { _id: queryId });

      if (!exists) {
        throw new ResourceNotFoundException(
          'table_definition',
          String(id)
        );
      }

      if (exists.isSystem) {
        throw new ValidationException(
          'Cannot modify system table',
          { tableId: id, tableName: exists.name }
        );
      }

      validateUniquePropertyNames(body.columns || [], body.relations || []);

      if (body.relations && body.relations.length > 0) {
        await this.validateNoDuplicateInverseRelation(queryId, exists.name, body.relations);
      }

      // Update table metadata (only update fields that are present in body)
      const updateData: any = {};
      if ('name' in body) updateData.name = body.name;
      if ('alias' in body) updateData.alias = body.alias;
      if ('description' in body) updateData.description = body.description;
      if ('uniques' in body) updateData.uniques = body.uniques;
      if ('indexes' in body) updateData.indexes = body.indexes;

      if (Object.keys(updateData).length > 0) {
        await this.queryBuilder.updateById('table_definition', id, updateData);
      }

      // Update columns
      if (body.columns) {
        const existingColumns = await this.queryBuilder.findWhere('column_definition', {
          table: queryId, // MongoDB uses 'table' field
        });

        const deletedColumnIds = getDeletedIds(
          existingColumns,
          body.columns,
        );

        // Delete removed columns and drop fields from data
        for (const colId of deletedColumnIds) {
          const deletedCol = existingColumns.find((c: any) => c._id?.toString() === colId.toString());
          if (deletedCol) {
            // Drop field from all records
            await this.mongoService.getDb().collection(exists.name).updateMany(
              {},
              { $unset: { [deletedCol.name]: "" } }
            );
            this.logger.log(`  Dropped field '${deletedCol.name}' from all records`);
          }
          await this.queryBuilder.deleteById('column_definition', colId);
        }

        // Detect renamed columns and trigger background migration
        const renamedColumns = [];
        for (const col of body.columns) {
          if (col._id || col.id) {
            const colId = col._id || col.id;
            const existingCol = existingColumns.find((c: any) => 
              c._id?.toString() === colId.toString()
            );
            
            // Check if column name changed
            if (existingCol && existingCol.name !== col.name) {
              renamedColumns.push({
                oldName: existingCol.name,
                newName: col.name,
                collectionName: exists.name,
              });
            }
          }
        }
        
        // Fire & forget: Migrate renamed fields in background
        if (renamedColumns.length > 0) {
          this.migrateRenamedFieldsInBackground(renamedColumns);
        }

        // Update or insert columns and collect their IDs
        const columnIds = [];
        for (const col of body.columns) {
          const columnData = {
            name: col.name,
            type: col.type,
            isPrimary: col.isPrimary || false,
            isGenerated: col.isGenerated || false,
            isNullable: col.isNullable ?? true,
            isSystem: col.isSystem || false,
            isUpdatable: col.isUpdatable ?? true,
            isHidden: col.isHidden || false,
            defaultValue: col.defaultValue ? JSON.stringify(col.defaultValue) : null,
            options: col.options ? JSON.stringify(col.options) : null,
            description: col.description,
            placeholder: col.placeholder,
            table: queryId, // MongoDB uses 'table' field
          };

          let colObjectId;
          if (col._id || col.id) {
            const colId = col._id || col.id;
            await this.queryBuilder.updateById('column_definition', colId, columnData);
            colObjectId = typeof colId === 'string' ? new ObjectId(colId) : colId;
          } else {
            const inserted = await this.queryBuilder.insertAndGet('column_definition', columnData);
            colObjectId = typeof inserted._id === 'string' ? new ObjectId(inserted._id) : inserted._id;
          }
          columnIds.push(colObjectId);
        }
      }

      // Update relations
      if (body.relations) {
        const existingRelations = await this.queryBuilder.findWhere('relation_definition', {
          sourceTable: queryId, // MongoDB uses 'sourceTable' field
        });

        // Drop fields FIRST before updating metadata
        // This ensures clean state and prevents stale data
        await this.dropRelationFieldsBeforeUpdate(
          body.relations,
          exists.name
        );

        const deletedRelationIds = getDeletedIds(
          existingRelations,
          body.relations,
        );

        // Delete removed relations and cleanup data
        for (const relId of deletedRelationIds) {
          const deletedRelation = existingRelations.find((r: any) => r._id?.toString() === relId.toString());

          if (deletedRelation) {
            // Owner side relations store data - need to cleanup
            if (deletedRelation.type === 'many-to-one' ||
                deletedRelation.type === 'one-to-one' ||
                (deletedRelation.type === 'many-to-many' && !deletedRelation.mappedBy)) {

              const fieldName = deletedRelation.propertyName;
              await this.mongoService.getDb().collection(exists.name).updateMany(
                {},
                { $unset: { [fieldName]: "" } }
              );
              this.logger.log(`  Cleaned up relation field '${fieldName}' from all records in ${exists.name}`);
            }
          }

          await this.queryBuilder.deleteById('relation_definition', relId);
        }

        // Update or insert relations and collect their IDs
        const relationIds = [];
        for (const rel of body.relations) {
          // Get target table ObjectId
          let targetTableObjectId;
          if (typeof rel.targetTable === 'object' && rel.targetTable._id) {
            targetTableObjectId = typeof rel.targetTable._id === 'string' ? new ObjectId(rel.targetTable._id) : rel.targetTable._id;
          } else if (typeof rel.targetTable === 'string') {
            const targetTableRecord = await this.queryBuilder.findOneWhere('table_definition', { name: rel.targetTable });
            if (targetTableRecord) {
              targetTableObjectId = typeof targetTableRecord._id === 'string' ? new ObjectId(targetTableRecord._id) : targetTableRecord._id;
            }
          }
          
          if (!targetTableObjectId) {
            this.logger.warn(`Target table not found for relation ${rel.propertyName}, skipping`);
            continue;
          }
          
          const relationData = {
            propertyName: rel.propertyName,
            type: rel.type,
            sourceTable: queryId, // ObjectId
            targetTable: targetTableObjectId, // ObjectId
            targetTableName: typeof rel.targetTable === 'string' ? rel.targetTable : (rel.targetTable.name || exists.name),
            sourceTableName: exists.name,
            inversePropertyName: rel.inversePropertyName,
            isNullable: rel.isNullable ?? true,
            isSystem: rel.isSystem || false,
            description: rel.description,
          };

          let relObjectId;
          if (rel._id || rel.id) {
            const relId = rel._id || rel.id;
            await this.queryBuilder.updateById('relation_definition', relId, relationData);
            relObjectId = typeof relId === 'string' ? new ObjectId(relId) : relId;
          } else {
            const inserted = await this.queryBuilder.insertAndGet('relation_definition', relationData);
            relObjectId = typeof inserted._id === 'string' ? new ObjectId(inserted._id) : inserted._id;
          }
          relationIds.push(relObjectId);
        }

      }

      const oldMetadata = await this.metadataCacheService.lookupTableByName(exists.name);
      const newMetadata = await this.getFullTableMetadata(id);

      if (oldMetadata && newMetadata) {
        await this.schemaMigrationService.updateCollection(exists.name, oldMetadata, newMetadata);
      }

      this.logger.log(`Collection updated: ${exists.name} (metadata + validation + indexes)`);
      return newMetadata;
    } catch (error) {
      this.loggingService.error('Collection update failed', {
        context: 'updateTable',
        error: error.message,
        stack: error.stack,
        tableId: id,
        collectionName: body?.name,
      });

      throw new DatabaseException(
        `Failed to update collection: ${error.message}`,
        {
          tableId: id,
          operation: 'update',
        },
      );
    }
    });
  }

  async delete(id: string | number) {
    return await this.runWithSchemaLock(`mongo:delete:${id}`, async () => {
    try {
      const { ObjectId } = require('mongodb');
      const tableId = typeof id === 'string' ? new ObjectId(id) : id;
      
      const exists = await this.queryBuilder.findOneWhere('table_definition', { _id: tableId });

      if (!exists) {
        throw new ResourceNotFoundException(
          'table_definition',
          String(id)
        );
      }

      if (exists.isSystem) {
        throw new ValidationException(
          'Cannot delete system table',
          { tableId: id, tableName: exists.name }
        );
      }

      const collectionName = exists.name;

      // Delete routes (MongoDB: mainTable is ObjectId)
      const routes = await this.queryBuilder.findWhere('route_definition', {
        mainTable: tableId,
      });
      
      for (const route of routes) {
        await this.queryBuilder.deleteById('route_definition', route._id);
      }
      this.logger.log(`Deleted ${routes.length} routes with mainTable = ${id}`);

      // Delete metadata (MongoDB: sourceTableId and table are ObjectIds)
      const relations = await this.queryBuilder.findWhere('relation_definition', {
        sourceTableId: tableId,
      });
      for (const rel of relations) {
        await this.queryBuilder.deleteById('relation_definition', rel._id);
      }

      const columns = await this.queryBuilder.findWhere('column_definition', {
        table: tableId,
      });
      for (const col of columns) {
        await this.queryBuilder.deleteById('column_definition', col._id);
      }

      await this.queryBuilder.deleteById('table_definition', tableId);

      // Drop collection
      await this.schemaMigrationService.dropCollection(collectionName);

      this.logger.log(`Collection deleted: ${collectionName} (metadata + collection)`);
      return exists;
    } catch (error) {
      this.loggingService.error('Collection deletion failed', {
        context: 'delete',
        error: error.message,
        stack: error.stack,
        tableId: id,
      });

      throw new DatabaseException(
        `Failed to delete collection: ${error.message}`,
        {
          tableId: id,
          operation: 'delete',
        },
      );
    }
    });
  }

  /**
   * Get full table metadata with columns and relations
   */
  private async getFullTableMetadata(tableId: any): Promise<any> {
    // MongoDB uses _id, SQL uses id
    // Convert string to ObjectId if needed
    const { ObjectId } = require('mongodb');
    const queryId = typeof tableId === 'string' ? new ObjectId(tableId) : tableId;
    
    const table = await this.queryBuilder.findOneWhere('table_definition', { _id: queryId });
    if (!table) return null;

    // Parse JSON fields
    if (table.uniques && typeof table.uniques === 'string') {
      try {
        table.uniques = JSON.parse(table.uniques);
      } catch (e) {
        table.uniques = [];
      }
    }
    if (table.indexes && typeof table.indexes === 'string') {
      try {
        table.indexes = JSON.parse(table.indexes);
      } catch (e) {
        table.indexes = [];
      }
    }

    // Load columns (MongoDB uses 'table' field, not 'tableId')
    table.columns = await this.queryBuilder.findWhere('column_definition', {
      table: queryId,
    });

    // Parse column JSON fields
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e) {
          // Keep as string
        }
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e) {
          // Keep as string
        }
      }
    }

    // Load relations (MongoDB uses 'sourceTable' field, not 'sourceTableId')
    table.relations = await this.queryBuilder.findWhere('relation_definition', {
      sourceTable: queryId,
    });

    return table;
  }
  private async runWithSchemaLock<T>(context: string, handler: () => Promise<T>): Promise<T> {
    const lock = await this.schemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.schemaMigrationLockService.release(lock);
    }
  }
}

