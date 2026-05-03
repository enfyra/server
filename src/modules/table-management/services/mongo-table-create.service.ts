import { Logger } from '../../../shared/logger';
import { ObjectId } from 'mongodb';
import { QueryBuilderService } from '@enfyra/kernel';
import {
  type MongoPhysicalMigrationService,
  MongoSchemaMigrationService,
  MongoService,
  MongoSchemaMigrationLockService,
} from '../../../engines/mongo';
import { MetadataCacheService } from '../../../engines/cache';
import {
  LoggingService,
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../domain/exceptions';
import {
  PolicyService,
  isPolicyDeny,
  isPolicyPreview,
} from '../../../domain/policy';
import { TDynamicContext } from '../../../shared/types';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { DatabaseConfigService } from '../../../shared/services';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { TCreateTableBody } from '../types/table-handler.types';
import { TableManagementValidationService } from './table-validation.service';
import { MongoMetadataSnapshotService } from './mongo-metadata-snapshot.service';
import {
  MONGO_PRIMARY_KEY_TYPE,
  isMongoPrimaryKeyType,
  normalizeMongoPrimaryKeyColumn,
} from '../utils/mongo-primary-key.util';
import { getRelationMappedByProperty } from '../utils/relation-target-id.util';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { ensureMongoTableRouteArtifacts } from './table-route-artifacts.service';
import {
  ensureMongoSingleRecord,
  syncMongoGqlDefinition,
} from './table-post-migration.service';
import { MongoTableHandlerService } from './mongo-table-handler-base.service';

export class MongoTableCreateService extends MongoTableHandlerService {
  async createTable(body: TCreateTableBody, context?: TDynamicContext) {
    const decision = await this.policyService.checkSchemaMigration({
      operation: 'create',
      tableName: 'table_definition',
      data: body,
      currentUser: context?.$user,
    });
    if (isPolicyDeny(decision)) {
      throw new ValidationException(decision.message);
    }
    const affectedTableNames = new Set<string>();
    return await this.runWithSchemaLock(
      `mongo:create:${body?.name || 'unknown'}`,
      async () => {
        if (/[A-Z]/.test(body?.name)) {
          throw new ValidationException(
            'Table name must be lowercase (no uppercase letters).',
            {
              tableName: body?.name,
            },
          );
        }
        if (!/^[a-z0-9_]+$/.test(body?.name)) {
          throw new ValidationException(
            'Table name must be snake_case (a-z, 0-9, _).',
            {
              tableName: body?.name,
            },
          );
        }
        const bodyRelations = body.relations ?? [];
        this.tableValidationService.validateRelations(bodyRelations);
        try {
          const db = this.queryBuilderService.getMongoDb();
          const collections = await db
            .listCollections({ name: body.name })
            .toArray();
          const hasCollection = collections.length > 0;
          const existing = await this.queryBuilderService.findOne({
            table: 'table_definition',
            where: {
              name: body.name,
            },
          });
          if (existing) {
            throw new DuplicateResourceException(
              'table_definition',
              'name',
              body.name,
            );
          }
          if (hasCollection && !existing) {
            this.logger.warn(
              `Mismatch detected: Physical collection "${body.name}" exists but no metadata found. Dropping physical collection...`,
            );
            try {
              await db.collection(body.name).drop();
            } catch (dropError: any) {
              this.logger.error(
                `Failed to drop physical collection "${body.name}": ${dropError.message}`,
              );
              throw new DatabaseException(
                `Failed to drop existing physical collection "${body.name}": ${dropError.message}`,
                {
                  collectionName: body.name,
                  operation: 'drop_existing_collection',
                },
              );
            }
          }
          body.columns = (body.columns || []).map((col: any) =>
            normalizeMongoPrimaryKeyColumn(col),
          );
          const idCol = body.columns.find(
            (col: any) => col.name === '_id' && col.isPrimary,
          );
          if (!idCol) {
            throw new ValidationException(
              `Table must contain a column named "_id" with isPrimary = true.`,
              { tableName: body.name },
            );
          }
          if (!isMongoPrimaryKeyType(idCol.type)) {
            throw new ValidationException(
              `The primary column "_id" must be of type ${MONGO_PRIMARY_KEY_TYPE}.`,
              { tableName: body.name, idColumnType: idCol.type },
            );
          }
          const primaryCount = body.columns.filter(
            (col: any) => col.isPrimary,
          ).length;
          if (primaryCount !== 1) {
            throw new ValidationException(
              `Only one column is allowed to have isPrimary = true.`,
              { tableName: body.name, primaryCount },
            );
          }
          validateUniquePropertyNames(body.columns || [], body.relations || []);
          body.isSystem = false;
          const tableRecord = await this.queryBuilderService.insert(
            'table_definition',
            {
              name: body.name,
              isSystem: body.isSystem,
              ...(body.isSingleRecord && { isSingleRecord: true }),
              alias: body.alias,
              description: body.description,
              uniques: body.uniques || [],
              indexes: body.indexes || [],
            },
          );
          const tableId =
            typeof tableRecord._id === 'string'
              ? new ObjectId(tableRecord._id)
              : tableRecord._id;
          const insertedColumnIds = [];
          try {
            if (body.columns?.length > 0) {
              for (const col of body.columns) {
                const columnRecord = await this.queryBuilderService.insert(
                  'column_definition',
                  {
                    name: col.name,
                    type: col.type,
                    isPrimary: col.isPrimary || false,
                    isGenerated: col.isGenerated || false,
                    isNullable: col.isNullable ?? true,
                    isSystem: col.isSystem || false,
                    isUpdatable: col.isUpdatable ?? true,
                    isPublished: col.isPublished ?? true,
                    defaultValue: col.defaultValue || null,
                    options: col.options || null,
                    description: col.description,
                    placeholder: col.placeholder,
                    metadata: col.metadata ?? null,
                    table: tableId,
                  },
                );
                const colId =
                  typeof columnRecord._id === 'string'
                    ? new ObjectId(columnRecord._id)
                    : columnRecord._id;
                insertedColumnIds.push(colId);
              }
            }
          } catch (error: any) {
            this.logger.error(
              `   Failed to insert columns, rolling back table creation`,
            );
            await this.queryBuilderService.delete('table_definition', tableId);
            throw new ValidationException(
              `Failed to create table: ${error.message}`,
              { tableName: body.name, error: error.message },
            );
          }
          const insertedRelationIds = [];
          try {
            if (bodyRelations.length > 0) {
              for (const rel of bodyRelations) {
                let targetTableObjectId;
                const targetTableIdFromObj =
                  typeof rel.targetTable === 'object'
                    ? rel.targetTable._id || rel.targetTable.id
                    : null;
                if (targetTableIdFromObj) {
                  targetTableObjectId =
                    typeof targetTableIdFromObj === 'string'
                      ? new ObjectId(targetTableIdFromObj)
                      : targetTableIdFromObj;
                } else if (typeof rel.targetTable === 'string') {
                  const targetTableRecord =
                    await this.queryBuilderService.findOne({
                      table: 'table_definition',
                      where: { name: rel.targetTable },
                    });
                  if (targetTableRecord) {
                    targetTableObjectId =
                      typeof targetTableRecord._id === 'string'
                        ? new ObjectId(targetTableRecord._id)
                        : targetTableRecord._id;
                  }
                }
                if (!targetTableObjectId) {
                  throw new ValidationException(
                    `Target table '${rel.targetTable}' not found for relation ${rel.propertyName}`,
                    { tableName: body.name, relation: rel.propertyName },
                  );
                }
                let resolvedMappedBy = null;
                const mappedByProperty = getRelationMappedByProperty(rel);
                if (mappedByProperty) {
                  const { data: owningRels } =
                    await this.queryBuilderService.find({
                      table: 'relation_definition',
                      where: {
                        sourceTable: targetTableObjectId,
                        propertyName: mappedByProperty,
                      },
                    });
                  if (owningRels.length > 0)
                    resolvedMappedBy = owningRels[0]._id;
                }
                const targetName =
                  typeof rel.targetTable === 'string'
                    ? rel.targetTable
                    : rel.targetTable?.name;
                const relationData: any = {
                  propertyName: rel.propertyName,
                  type: rel.type,
                  sourceTable: tableId,
                  targetTable: targetTableObjectId,
                  targetTableName: targetName,
                  sourceTableName: body.name,
                  mappedBy: resolvedMappedBy,
                  isNullable: rel.isNullable ?? true,
                  isSystem: rel.isSystem || false,
                  isUpdatable: rel.isUpdatable ?? true,
                  isPublished: rel.isPublished ?? true,
                  onDelete: rel.onDelete || 'SET NULL',
                  description: rel.description,
                };
                const ownsMongoReference =
                  (rel.type === 'many-to-one' || rel.type === 'one-to-one') &&
                  !resolvedMappedBy;
                if (ownsMongoReference) {
                  relationData.foreignKeyColumn =
                    rel.foreignKeyColumn || rel.propertyName;
                } else if (resolvedMappedBy) {
                  const owningRel = await this.queryBuilderService.findOne({
                    table: 'relation_definition',
                    where: { _id: resolvedMappedBy },
                  });
                  relationData.foreignKeyColumn =
                    owningRel?.foreignKeyColumn || owningRel?.propertyName || null;
                }
                if (
                  rel.type === 'many-to-many' &&
                  !mappedByProperty &&
                  targetName
                ) {
                  const junction = getSqlJunctionPhysicalNames({
                    sourceTable: body.name,
                    propertyName: rel.propertyName,
                    targetTable: targetName,
                  });
                  relationData.junctionTableName = junction.junctionTableName;
                  relationData.junctionSourceColumn =
                    junction.junctionSourceColumn;
                  relationData.junctionTargetColumn =
                    junction.junctionTargetColumn;
                }
                const relationRecord = await this.queryBuilderService.insert(
                  'relation_definition',
                  relationData,
                );
                const relId =
                  typeof relationRecord._id === 'string'
                    ? new ObjectId(relationRecord._id)
                    : relationRecord._id;
                insertedRelationIds.push(relId);
                if (rel.inversePropertyName) {
                  if (mappedByProperty) {
                    throw new ValidationException(
                      `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
                      { relationName: rel.propertyName },
                    );
                  }
                  const { data: existingOnTarget } =
                    await this.queryBuilderService.find({
                      table: 'relation_definition',
                      where: {
                        sourceTable: targetTableObjectId,
                        propertyName: rel.inversePropertyName,
                      },
                    });
                  if (existingOnTarget.length > 0) {
                    throw new ValidationException(
                      `Cannot create inverse '${rel.inversePropertyName}' on target table: property name already exists`,
                      { relationName: rel.inversePropertyName },
                    );
                  }
                  const { data: existingInverse } =
                    await this.queryBuilderService.find({
                      table: 'relation_definition',
                      where: { mappedBy: relId },
                    });
                  if (existingInverse.length > 0) {
                    throw new ValidationException(
                      `Relation '${rel.propertyName}' already has an inverse '${existingInverse[0].propertyName}'`,
                      { relationName: rel.propertyName },
                    );
                  }
                  let inverseType = rel.type;
                  if (rel.type === 'many-to-one') inverseType = 'one-to-many';
                  else if (rel.type === 'one-to-many')
                    inverseType = 'many-to-one';
                  const inverseData: any = {
                    propertyName: rel.inversePropertyName,
                    type: inverseType,
                    sourceTable: targetTableObjectId,
                    targetTable: tableId,
                    mappedBy: relId,
                    isNullable: rel.isNullable ?? true,
                    isSystem: false,
                    isUpdatable: rel.isUpdatable ?? true,
                    isPublished: rel.isPublished ?? true,
                    onDelete: rel.onDelete || 'SET NULL',
                  };
                  if (
                    rel.type === 'many-to-one' ||
                    rel.type === 'one-to-one'
                  ) {
                    inverseData.foreignKeyColumn =
                      relationData.foreignKeyColumn ||
                      rel.foreignKeyColumn ||
                      rel.propertyName;
                  }
                  if (rel.type === 'many-to-many') {
                    const invTargetName =
                      typeof rel.targetTable === 'string'
                        ? rel.targetTable
                        : rel.targetTable?.name;
                    if (invTargetName) {
                      const junction = getSqlJunctionPhysicalNames({
                        sourceTable: body.name,
                        propertyName: rel.propertyName,
                        targetTable: invTargetName,
                      });
                      inverseData.junctionTableName =
                        junction.junctionTableName;
                      inverseData.junctionSourceColumn =
                        junction.junctionTargetColumn;
                      inverseData.junctionTargetColumn =
                        junction.junctionSourceColumn;
                    }
                  }
                  const inverseRecord = await this.queryBuilderService.insert(
                    'relation_definition',
                    inverseData,
                  );
                  const inverseId =
                    typeof inverseRecord._id === 'string'
                      ? new ObjectId(inverseRecord._id)
                      : inverseRecord._id;
                  insertedRelationIds.push(inverseId);
                  const targetName =
                    typeof rel.targetTable === 'string'
                      ? rel.targetTable
                      : rel.targetTable?.name;
                  if (targetName) affectedTableNames.add(targetName);
                  this.logger.log(
                    `Auto-created inverse relation '${rel.inversePropertyName}'`,
                  );
                }
              }
            }
          } catch (error: any) {
            this.logger.error(
              `   Failed to insert relations, rolling back table creation`,
            );
            for (const colId of insertedColumnIds) {
              await this.queryBuilderService.delete('column_definition', colId);
            }
            await this.queryBuilderService.delete('table_definition', tableId);
            throw new ValidationException(
              `Failed to create table: ${error.message}`,
              { tableName: body.name, error: error.message },
            );
          }
          await ensureMongoTableRouteArtifacts({
            mongoService: this.mongoService,
            queryBuilderService: this.queryBuilderService,
            tableName: body.name,
            tableId,
          });

          await syncMongoGqlDefinition({
            mongoService: this.mongoService,
            queryBuilderService: this.queryBuilderService,
            tableId,
            isEnabled: body.graphqlEnabled === true,
            isSystem: false,
          });

          const fullMetadata =
            await this.mongoMetadataSnapshotService.getFullTableMetadata(
              tableId,
            );
          await this.mongoSchemaMigrationService.createCollection(fullMetadata);

          const owningM2m = (fullMetadata.relations || []).filter(
            (r: any) =>
              r.type === 'many-to-many' && !r.mappedBy && r.junctionTableName,
          );
          for (const m2m of owningM2m) {
            await this.mongoSchemaMigrationService.ensureJunctionCollection(
              m2m.junctionTableName,
              m2m.junctionSourceColumn,
              m2m.junctionTargetColumn,
            );
          }

          if (body.isSingleRecord) {
            await ensureMongoSingleRecord({
              mongoService: this.mongoService,
              tableName: body.name,
              columns: fullMetadata.columns || [],
            });
          }

          fullMetadata.affectedTables = [...affectedTableNames];
          return fullMetadata;
        } catch (error: any) {
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
      },
    );
}

}
