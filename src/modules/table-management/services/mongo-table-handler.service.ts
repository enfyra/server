import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MongoSchemaMigrationService } from '../../../infrastructure/mongo/services/mongo-schema-migration.service';
import { MongoService } from '../../../infrastructure/mongo/services/mongo.service';
import { MongoSchemaMigrationLockService } from '../../../infrastructure/mongo/services/mongo-schema-migration-lock.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { LoggingService } from '../../../core/exceptions/services/logging.service';
import { PolicyService } from '../../../core/policy/policy.service';
import { TDynamicContext } from '../../../shared/types';
import {
  isPolicyDeny,
  isPolicyPreview,
} from '../../../core/policy/policy.types';
import {
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../core/exceptions/custom-exceptions';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { CreateTableDto } from '../dto/create-table.dto';
import { generateDefaultRecord } from '../utils/generate-default-record';
import { DEFAULT_REST_HANDLER_LOGIC } from '../../../core/bootstrap/utils/canonical-table-route.util';
import { getJunctionTableName, getJunctionColumnNames } from '../../../infrastructure/knex/utils/sql-schema-naming.util';
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
    private policyService: PolicyService,
  ) {}
  private validateRelations(relations: any[]) {
    for (const relation of relations || []) {
      if (relation.type === 'one-to-many' && !relation.mappedBy) {
        throw new ValidationException(
          `One-to-many relation '${relation.propertyName}' must have mappedBy`,
          {
            relationName: relation.propertyName,
            relationType: relation.type,
            missingField: 'mappedBy',
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
    const querySourceId =
      typeof sourceTableId === 'string'
        ? new ObjectId(sourceTableId)
        : sourceTableId;
    for (const rel of newRelations || []) {
      let targetTableId: any;
      if (typeof rel.targetTable === 'object' && rel.targetTable._id) {
        targetTableId =
          typeof rel.targetTable._id === 'string'
            ? new ObjectId(rel.targetTable._id)
            : rel.targetTable._id;
      } else if (typeof rel.targetTable === 'object' && rel.targetTable.id) {
        targetTableId =
          typeof rel.targetTable.id === 'string'
            ? new ObjectId(rel.targetTable.id)
            : rel.targetTable.id;
      } else if (typeof rel.targetTable === 'string') {
        const targetTableRecord = await this.queryBuilder.findOne({
          table: 'table_definition',
          where: { name: rel.targetTable },
        });
        if (targetTableRecord) {
          targetTableId =
            typeof targetTableRecord._id === 'string'
              ? new ObjectId(targetTableRecord._id)
              : targetTableRecord._id;
        } else {
          continue;
        }
      } else {
        continue;
      }
      if (!targetTableId) continue;
      let targetTableName: string;
      const targetTableRecord = await this.queryBuilder.findOne({
        table: 'table_definition',
        where: { _id: targetTableId },
      });
      if (targetTableRecord) {
        targetTableName = targetTableRecord.name;
      } else {
        continue;
      }
      let inverseExists = false;
      let inverseRelationInfo = null;
      if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
        const { data: targetRelations } = await this.queryBuilder.find({
          table: 'relation_definition',
          where: {
            sourceTable: targetTableId,
            targetTable: querySourceId,
          },
        });
        let sourceRelId: any = null;
        const { data: sourceRels } = await this.queryBuilder.find({
          table: 'relation_definition',
          where: { sourceTable: querySourceId, propertyName: rel.propertyName },
        });
        if (sourceRels.length > 0) sourceRelId = sourceRels[0]._id;
        const matchingRelation = targetRelations.find((tr: any) => {
          if (rel.mappedBy && tr.propertyName === rel.mappedBy) return true;
          if (sourceRelId && tr.mappedBy) {
            return tr.mappedBy.toString() === sourceRelId.toString();
          }
          return false;
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
        if (!rel.mappedBy) continue;
        const { data: targetRelations } = await this.queryBuilder.find({
          table: 'relation_definition',
          where: {
            sourceTable: targetTableId,
            targetTable: querySourceId,
            propertyName: rel.mappedBy,
          },
        });
        const matchingRelation = targetRelations.find((tr: any) =>
          ['many-to-one', 'one-to-one'].includes(tr.type),
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
        if (!rel.mappedBy) continue;
        const { data: targetRelations } = await this.queryBuilder.find({
          table: 'relation_definition',
          where: {
            sourceTable: targetTableId,
            targetTable: querySourceId,
            propertyName: rel.mappedBy,
            type: 'many-to-many',
          },
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
  private migrateRenamedFieldsInBackground(
    renamedColumns: Array<{
      oldName: string;
      newName: string;
      collectionName: string;
    }>,
  ): void {
    (async () => {
      const db = this.mongoService.getDb();
      for (const { oldName, newName, collectionName } of renamedColumns) {
        try {
          const result = await db
            .collection(collectionName)
            .updateMany(
              { [oldName]: { $exists: true } },
              { $rename: { [oldName]: newName } },
            );
        } catch (error) {
          this.logger.error(
            `  [Background] Failed to rename field in ${collectionName}:`,
            error.message,
          );
        }
      }
    })().catch((err) => {
      this.logger.error('Background migration error:', err);
    });
  }
  private async dropRelationFieldsBeforeUpdate(
    newRelations: any[],
    sourceTableName: string,
  ): Promise<void> {
    const db = this.mongoService.getDb();
    if (!newRelations || newRelations.length === 0) {
      return;
    }
    for (const relation of newRelations) {
      let targetTableName: string;
      if (
        typeof relation.targetTable === 'object' &&
        relation.targetTable.name
      ) {
        targetTableName = relation.targetTable.name;
      } else if (typeof relation.targetTable === 'string') {
        targetTableName = relation.targetTable;
      } else {
        const { ObjectId } = require('mongodb');
        const targetId = relation.targetTable._id || relation.targetTable;
        const targetTableRecord = await this.queryBuilder.findOne({
          table: 'table_definition',
          where: {
            _id:
              typeof targetId === 'string' ? new ObjectId(targetId) : targetId,
          },
        });
        if (targetTableRecord) {
          targetTableName = targetTableRecord.name;
        } else {
          this.logger.warn(
            `Cannot find target table for relation ${relation.propertyName}, skipping drop`,
          );
          continue;
        }
      }
      const sourceFieldName = relation.propertyName;
      const inverseFieldName = relation.mappedBy;
      if (sourceFieldName) {
        await db
          .collection(sourceTableName)
          .updateMany({}, { $unset: { [sourceFieldName]: '' } });
      }
      if (inverseFieldName && targetTableName) {
        await db
          .collection(targetTableName)
          .updateMany({}, { $unset: { [inverseFieldName]: '' } });
      }
    }
  }
  async createTable(body: CreateTableDto, context?: TDynamicContext) {
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
        this.validateRelations(body.relations);
        try {
          const db = this.queryBuilder.getMongoDb();
          const collections = await db
            .listCollections({ name: body.name })
            .toArray();
          const hasCollection = collections.length > 0;
          const existing = await this.queryBuilder.findOne({
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
            } catch (dropError) {
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
          const idCol = body.columns.find(
            (col: any) => col.name === '_id' && col.isPrimary,
          );
          if (!idCol) {
            throw new ValidationException(
              `Table must contain a column named "_id" with isPrimary = true.`,
              { tableName: body.name },
            );
          }
          const validTypes = ['int', 'uuid'];
          if (!validTypes.includes(idCol.type)) {
            throw new ValidationException(
              `The primary column "_id" must be of type int or uuid.`,
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
          const tableRecord = await this.queryBuilder.insert(
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
          const { ObjectId } = require('mongodb');
          const tableId =
            typeof tableRecord._id === 'string'
              ? new ObjectId(tableRecord._id)
              : tableRecord._id;
          const insertedColumnIds = [];
          try {
            if (body.columns?.length > 0) {
              for (const col of body.columns) {
                const columnRecord = await this.queryBuilder.insert(
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
          } catch (error) {
            this.logger.error(
              `   Failed to insert columns, rolling back table creation`,
            );
            await this.queryBuilder.delete('table_definition', tableId);
            throw new ValidationException(
              `Failed to create table: ${error.message}`,
              { tableName: body.name, error: error.message },
            );
          }
          const insertedRelationIds = [];
          try {
            if (body.relations?.length > 0) {
              for (const rel of body.relations) {
                let targetTableObjectId;
                const targetTableIdFromObj = typeof rel.targetTable === 'object'
                  ? (rel.targetTable._id || rel.targetTable.id)
                  : null;
                if (targetTableIdFromObj) {
                  targetTableObjectId =
                    typeof targetTableIdFromObj === 'string'
                      ? new ObjectId(targetTableIdFromObj)
                      : targetTableIdFromObj;
                } else if (typeof rel.targetTable === 'string') {
                  const targetTableRecord =
                    await this.queryBuilder.findOne({
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
                if (rel.mappedBy) {
                  const { data: owningRels } = await this.queryBuilder.find({
                    table: 'relation_definition',
                    where: { sourceTable: targetTableObjectId, propertyName: rel.mappedBy },
                  });
                  if (owningRels.length > 0) resolvedMappedBy = owningRels[0]._id;
                }
                const targetName = typeof rel.targetTable === 'string'
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
                  description: rel.description,
                };
                if (rel.type === 'many-to-many' && !rel.mappedBy && targetName) {
                  const junctionTableName = getJunctionTableName(body.name, rel.propertyName, targetName);
                  const { sourceColumn, targetColumn } = getJunctionColumnNames(body.name, rel.propertyName, targetName);
                  relationData.junctionTableName = junctionTableName;
                  relationData.junctionSourceColumn = sourceColumn;
                  relationData.junctionTargetColumn = targetColumn;
                }
                const relationRecord = await this.queryBuilder.insert(
                  'relation_definition',
                  relationData,
                );
                const relId =
                  typeof relationRecord._id === 'string'
                    ? new ObjectId(relationRecord._id)
                    : relationRecord._id;
                insertedRelationIds.push(relId);
                if (rel.inversePropertyName) {
                  if (rel.mappedBy) {
                    throw new ValidationException(
                      `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
                      { relationName: rel.propertyName },
                    );
                  }
                  const { data: existingOnTarget } = await this.queryBuilder.find({
                    table: 'relation_definition',
                    where: { sourceTable: targetTableObjectId, propertyName: rel.inversePropertyName },
                  });
                  if (existingOnTarget.length > 0) {
                    throw new ValidationException(
                      `Cannot create inverse '${rel.inversePropertyName}' on target table: property name already exists`,
                      { relationName: rel.inversePropertyName },
                    );
                  }
                  const { data: existingInverse } = await this.queryBuilder.find({
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
                  else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
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
                  };
                  if (rel.type === 'many-to-many') {
                    const invTargetName = typeof rel.targetTable === 'string'
                      ? rel.targetTable
                      : rel.targetTable?.name;
                    if (invTargetName) {
                      const junctionTableName = getJunctionTableName(body.name, rel.propertyName, invTargetName);
                      const { sourceColumn, targetColumn } = getJunctionColumnNames(body.name, rel.propertyName, invTargetName);
                      inverseData.junctionTableName = junctionTableName;
                      inverseData.junctionSourceColumn = targetColumn;
                      inverseData.junctionTargetColumn = sourceColumn;
                    }
                  }
                  const inverseRecord = await this.queryBuilder.insert(
                    'relation_definition',
                    inverseData,
                  );
                  const inverseId =
                    typeof inverseRecord._id === 'string'
                      ? new ObjectId(inverseRecord._id)
                      : inverseRecord._id;
                  insertedRelationIds.push(inverseId);
                  const targetName = typeof rel.targetTable === 'string'
                    ? rel.targetTable
                    : rel.targetTable?.name;
                  if (targetName) affectedTableNames.add(targetName);
                  this.logger.log(
                    `Auto-created inverse relation '${rel.inversePropertyName}'`,
                  );
                }
              }
            }
          } catch (error) {
            this.logger.error(
              `   Failed to insert relations, rolling back table creation`,
            );
            for (const colId of insertedColumnIds) {
              await this.queryBuilder.delete('column_definition', colId);
            }
            await this.queryBuilder.delete('table_definition', tableId);
            throw new ValidationException(
              `Failed to create table: ${error.message}`,
              { tableName: body.name, error: error.message },
            );
          }
          const existingRoute = await this.queryBuilder.findOne({
            table: 'route_definition',
            where: {
              path: `/${body.name}`,
            },
          });
          if (!existingRoute) {
            const db = this.mongoService.getDb();
            const methods = await db
              .collection('method_definition')
              .find({}, { projection: { _id: 1, method: 1 } })
              .toArray();
            const allMethodIds = methods.map((m: any) => m._id);

            const { ObjectId } = require('mongodb');
            const routeDoc: any = {
              path: `/${body.name}`,
              mainTable: tableId,
              isEnabled: true,
              isSystem: false,
              icon: 'lucide:table',
              publishedMethods: [],
              skipRoleGuardMethods: [],
              availableMethods: allMethodIds,
              routePermissions: [],
              handlers: [],
              preHooks: [],
              postHooks: [],
              createdAt: new Date(),
              updatedAt: new Date(),
            };
            const routeResult = await db
              .collection('route_definition')
              .insertOne(routeDoc);
            const newRouteId = routeResult.insertedId;

            for (const m of methods) {
              const methodName = m.method;
              if (!DEFAULT_REST_HANDLER_LOGIC[methodName]) continue;
              await db.collection('route_handler_definition').insertOne({
                route: newRouteId,
                method: m._id,
                logic: DEFAULT_REST_HANDLER_LOGIC[methodName],
                timeout: 30000,
                createdAt: new Date(),
                updatedAt: new Date(),
              });
            }

            const junctionRows = allMethodIds.map((methodId: any) => ({
              route_definitionId: newRouteId,
              method_definitionId: methodId,
            }));
            if (junctionRows.length > 0) {
              try {
                await db
                  .collection('route_definition_availableMethods_method_definition')
                  .insertMany(junctionRows, { ordered: false });
              } catch (err: any) {
                if (err?.code !== 11000) throw err;
              }
            }
          }

          // Auto-create gql_definition for new table
          const existingGql = await this.queryBuilder.findOne({
            table: 'gql_definition',
            where: { table: tableId },
          });
          if (!existingGql) {
            await db.collection('gql_definition').insertOne({
              table: tableId,
              isEnabled: body.graphqlEnabled === true,
              isSystem: false,
              createdAt: new Date(),
              updatedAt: new Date(),
            });
          }

          const fullMetadata = await this.getFullTableMetadata(tableId);
          await this.schemaMigrationService.createCollection(fullMetadata);

          const owningM2m = (fullMetadata.relations || []).filter(
            (r: any) => r.type === 'many-to-many' && !r.mappedBy && r.junctionTableName,
          );
          for (const m2m of owningM2m) {
            await this.schemaMigrationService.ensureJunctionCollection(
              m2m.junctionTableName,
              m2m.junctionSourceColumn,
              m2m.junctionTargetColumn,
            );
          }

          if (body.isSingleRecord) {
            const db = this.mongoService.getDb();
            const existingRecord = await db.collection(body.name).findOne();
            if (!existingRecord) {
              const defaultRecord = generateDefaultRecord(
                fullMetadata.columns || [],
              );
              await db.collection(body.name).insertOne(defaultRecord);
            } else {
            }
          }

          fullMetadata.affectedTables = [...affectedTableNames];
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
      },
    );
  }
  async updateTable(id: any, body: CreateTableDto, context?: TDynamicContext) {
    const affectedTableNames = new Set<string>();
    const tag = `[mongo:updateTable:${id}]`;
    const stepLog = (msg: string) => this.logger.log(`${tag} ${msg}`);
    const t0 = Date.now();
    let t = Date.now();
    const lap = () => {
      const e = Date.now() - t;
      t = Date.now();
      return e;
    };
    stepLog(`STEP 0 acquiring schema lock`);
    const out = await this.runWithSchemaLock(`mongo:update:${id}`, async () => {
      stepLog(`STEP 1 lock acquired (+${Date.now() - t0}ms)`);
      t = Date.now();
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
      stepLog(`STEP 2 validated name+relations (+${lap()}ms)`);
      try {
        const { ObjectId } = require('mongodb');
        const queryId = typeof id === 'string' ? new ObjectId(id) : id;
        const exists = await this.queryBuilder.findOne({
          table: 'table_definition',
          where: { _id: queryId },
        });
        stepLog(`STEP 3 fetched table_definition (+${lap()}ms)`);
        if (!exists) {
          throw new ResourceNotFoundException('table_definition', String(id));
        }
        if (exists.isSystem) {
          throw new ValidationException('Cannot modify system table', {
            tableId: id,
            tableName: exists.name,
          });
        }
        validateUniquePropertyNames(body.columns || [], body.relations || []);
        if (body.relations && body.relations.length > 0) {
          await this.validateNoDuplicateInverseRelation(
            queryId,
            exists.name,
            body.relations,
          );
        }
        stepLog(`STEP 4 validators done (+${lap()}ms)`);
        const oldMetadata = await this.metadataCacheService.lookupTableByName(
          exists.name,
        );
        stepLog(`STEP 5 loaded oldMetadata (+${lap()}ms)`);
        let schemaDecision: any = null;
        if (oldMetadata) {
          const afterMetadata = {
            name: body.name ?? exists.name,
            columns: body.columns ?? oldMetadata?.columns ?? [],
            relations: body.relations ?? oldMetadata?.relations ?? [],
            uniques: body.uniques ?? exists.uniques ?? oldMetadata?.uniques,
            indexes: body.indexes ?? exists.indexes ?? oldMetadata?.indexes,
          };
          schemaDecision = await this.policyService.checkSchemaMigration({
            operation: 'update',
            tableName: exists.name,
            data: body,
            currentUser: context?.$user,
            beforeMetadata: oldMetadata,
            afterMetadata,
            requestContext: context,
          });
          if (isPolicyPreview(schemaDecision)) {
            return { _preview: true, ...schemaDecision.details };
          }
          if (isPolicyDeny(schemaDecision)) {
            throw new ValidationException(
              schemaDecision.message,
              schemaDecision.details,
            );
          }
          stepLog(`STEP 6 policy checked (+${lap()}ms)`);
        }
        stepLog(`STEP 6b capturing raw metadata snapshot...`);
        const rawSnapshot = await this.captureRawMetadataSnapshot(queryId);
        stepLog(`STEP 6b snapshot captured (+${lap()}ms)`);
        const updateData: any = {};
        if ('name' in body) updateData.name = body.name;
        if ('alias' in body) updateData.alias = body.alias;
        if ('description' in body) updateData.description = body.description;
        if ('uniques' in body) updateData.uniques = body.uniques;
        if ('indexes' in body) updateData.indexes = body.indexes;
        if ('isSingleRecord' in body)
          updateData.isSingleRecord = body.isSingleRecord;
        if (Object.keys(updateData).length > 0) {
          await this.queryBuilder.update(
            'table_definition',
            id,
            updateData,
          );
        }
        stepLog(`STEP 7 updated table_definition row (+${lap()}ms)`);
        if (body.columns) {
          const { data: existingColumns } = await this.queryBuilder.find({
            table: 'column_definition',
            where: {
              table: queryId,
            },
          });
          const deletedColumnIds = getDeletedIds(existingColumns, body.columns);
          for (const colId of deletedColumnIds) {
            const deletedCol = existingColumns.find(
              (c: any) => c._id?.toString() === colId.toString(),
            );
            if (deletedCol) {
              await this.mongoService
                .getDb()
                .collection(exists.name)
                .updateMany({}, { $unset: { [deletedCol.name]: '' } });
            }
            await this.queryBuilder.delete('column_definition', colId);
          }
          const renamedColumns = [];
          for (const col of body.columns) {
            if (col._id || col.id) {
              const colId = col._id || col.id;
              const existingCol = existingColumns.find(
                (c: any) => c._id?.toString() === colId.toString(),
              );
              if (existingCol && existingCol.name !== col.name) {
                renamedColumns.push({
                  oldName: existingCol.name,
                  newName: col.name,
                  collectionName: exists.name,
                });
              }
            }
          }
          if (renamedColumns.length > 0) {
            this.migrateRenamedFieldsInBackground(renamedColumns);
          }
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
              isPublished: col.isPublished ?? true,
              defaultValue: col.defaultValue || null,
              options: col.options || null,
              description: col.description,
              placeholder: col.placeholder,
              metadata: col.metadata ?? null,
              table: queryId,
            };
            let colObjectId;
            if (col._id || col.id) {
              const colId = col._id || col.id;
              await this.queryBuilder.update(
                'column_definition',
                colId,
                columnData,
              );
              colObjectId =
                typeof colId === 'string' ? new ObjectId(colId) : colId;
            } else {
              const inserted = await this.queryBuilder.insert(
                'column_definition',
                columnData,
              );
              colObjectId =
                typeof inserted._id === 'string'
                  ? new ObjectId(inserted._id)
                  : inserted._id;
            }
            columnIds.push(colObjectId);
          }
        }
        stepLog(
          `STEP 8 processed ${body.columns?.length ?? 0} column(s) (+${lap()}ms)`,
        );
        if (body.relations) {
          const { data: existingRelations } = await this.queryBuilder.find({
            table: 'relation_definition',
            where: {
              sourceTable: queryId,
            },
          });
          await this.dropRelationFieldsBeforeUpdate(
            body.relations,
            exists.name,
          );
          const deletedRelationIds = getDeletedIds(
            existingRelations,
            body.relations,
          );
          for (const relId of deletedRelationIds) {
            const deletedRelation = existingRelations.find(
              (r: any) => r._id?.toString() === relId.toString(),
            );
            if (deletedRelation) {
              if (
                deletedRelation.type === 'many-to-one' ||
                deletedRelation.type === 'one-to-one' ||
                (deletedRelation.type === 'many-to-many' &&
                  !deletedRelation.mappedBy)
              ) {
                const fieldName = deletedRelation.propertyName;
                await this.mongoService
                  .getDb()
                  .collection(exists.name)
                  .updateMany({}, { $unset: { [fieldName]: '' } });
              }
              const { data: inverseRels } = await this.queryBuilder.find({
                table: 'relation_definition',
                where: { mappedBy: deletedRelation._id },
              });
              for (const inv of inverseRels) {
                if (inv.sourceTableName) affectedTableNames.add(inv.sourceTableName);
                await this.queryBuilder.delete(
                  'relation_definition',
                  inv._id,
                );
              }
            }
            await this.queryBuilder.delete('relation_definition', relId);
          }
          for (const rel of body.relations) {
            if (!rel._id && !rel.id) continue;
            const relId = rel._id || rel.id;
            const existingRel = existingRelations.find(
              (r: any) => r._id?.toString() === relId.toString(),
            );
            if (existingRel?.mappedBy) {
              const changed =
                (rel.type !== undefined && rel.type !== existingRel.type) ||
                (rel.isNullable !== undefined &&
                  rel.isNullable !== existingRel.isNullable);
              if (changed) {
                throw new ValidationException(
                  `Inverse relation '${existingRel.propertyName}' can only have its propertyName modified`,
                  { relationName: existingRel.propertyName },
                );
              }
            }
          }
          const relationIds = [];
          for (const rel of body.relations) {
            let targetTableObjectId;
            const targetTableIdFromObj = typeof rel.targetTable === 'object'
              ? (rel.targetTable._id || rel.targetTable.id)
              : null;
            if (targetTableIdFromObj) {
              targetTableObjectId =
                typeof targetTableIdFromObj === 'string'
                  ? new ObjectId(targetTableIdFromObj)
                  : targetTableIdFromObj;
            } else if (typeof rel.targetTable === 'string') {
              const targetTableRecord = await this.queryBuilder.findOne({
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
              this.logger.warn(
                `Target table not found for relation ${rel.propertyName}, skipping`,
              );
              continue;
            }
            let updateResolvedMappedBy = null;
            if (rel.mappedBy) {
              const { data: owningRels } = await this.queryBuilder.find({
                table: 'relation_definition',
                where: { sourceTable: targetTableObjectId, propertyName: rel.mappedBy },
              });
              if (owningRels.length > 0) updateResolvedMappedBy = owningRels[0]._id;
            }
            const relationData: any = {
              propertyName: rel.propertyName,
              type: rel.type,
              sourceTable: queryId,
              targetTable: targetTableObjectId,
              targetTableName:
                typeof rel.targetTable === 'string'
                  ? rel.targetTable
                  : rel.targetTable.name || exists.name,
              sourceTableName: exists.name,
              mappedBy: updateResolvedMappedBy,
              isNullable: rel.isNullable ?? true,
              isSystem: rel.isSystem || false,
              isUpdatable: rel.isUpdatable ?? true,
              isPublished: rel.isPublished ?? true,
              description: rel.description,
            };
            const targetRelName = typeof rel.targetTable === 'string'
              ? rel.targetTable
              : rel.targetTable?.name;
            if (rel.type === 'many-to-many' && !rel.mappedBy && targetRelName) {
              const junctionTableName = getJunctionTableName(exists.name, rel.propertyName, targetRelName);
              const { sourceColumn, targetColumn } = getJunctionColumnNames(exists.name, rel.propertyName, targetRelName);
              relationData.junctionTableName = junctionTableName;
              relationData.junctionSourceColumn = sourceColumn;
              relationData.junctionTargetColumn = targetColumn;
            }
            let relObjectId;
            if (rel._id || rel.id) {
              const relId = rel._id || rel.id;
              await this.queryBuilder.update(
                'relation_definition',
                relId,
                relationData,
              );
              relObjectId =
                typeof relId === 'string' ? new ObjectId(relId) : relId;
            } else {
              const inserted = await this.queryBuilder.insert(
                'relation_definition',
                relationData,
              );
              relObjectId =
                typeof inserted._id === 'string'
                  ? new ObjectId(inserted._id)
                  : inserted._id;
            }
            relationIds.push(relObjectId);
            if (rel.inversePropertyName && !(rel._id || rel.id)) {
              if (rel.mappedBy) {
                throw new ValidationException(
                  `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
                  { relationName: rel.propertyName },
                );
              }
              const { data: existingOnTarget } = await this.queryBuilder.find({
                table: 'relation_definition',
                where: { sourceTable: targetTableObjectId, propertyName: rel.inversePropertyName },
              });
              if (existingOnTarget.length > 0) {
                throw new ValidationException(
                  `Cannot create inverse '${rel.inversePropertyName}' on target table: property name already exists`,
                  { relationName: rel.inversePropertyName },
                );
              }
              const { data: existingInverse } = await this.queryBuilder.find({
                table: 'relation_definition',
                where: { mappedBy: relObjectId },
              });
              if (existingInverse.length > 0) {
                throw new ValidationException(
                  `Relation '${rel.propertyName}' already has an inverse '${existingInverse[0].propertyName}'`,
                  { relationName: rel.propertyName },
                );
              }
              let inverseType = rel.type;
              if (rel.type === 'many-to-one') inverseType = 'one-to-many';
              else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
              const inverseData: any = {
                propertyName: rel.inversePropertyName,
                type: inverseType,
                sourceTable: targetTableObjectId,
                targetTable: queryId,
                mappedBy: relObjectId,
                isNullable: rel.isNullable ?? true,
                isSystem: false,
                isUpdatable: rel.isUpdatable ?? true,
                isPublished: rel.isPublished ?? true,
              };
              if (rel.type === 'many-to-many' && targetRelName) {
                const junctionTableName = getJunctionTableName(exists.name, rel.propertyName, targetRelName);
                const { sourceColumn, targetColumn } = getJunctionColumnNames(exists.name, rel.propertyName, targetRelName);
                inverseData.junctionTableName = junctionTableName;
                inverseData.junctionSourceColumn = targetColumn;
                inverseData.junctionTargetColumn = sourceColumn;
              }
              const inverseRecord = await this.queryBuilder.insert(
                'relation_definition',
                inverseData,
              );
              this.logger.log(
                `Auto-created inverse relation '${rel.inversePropertyName}'`,
              );
            }
          }
        }
        stepLog(
          `STEP 9 processed ${body.relations?.length ?? 0} relation(s) (+${lap()}ms)`,
        );
        const finalMetadata = await this.getFullTableMetadata(id);
        stepLog(`STEP 10 loaded finalMetadata (+${lap()}ms)`);

        if (
          schemaDecision?.details?.schemaChanged === true &&
          oldMetadata &&
          finalMetadata
        ) {
          stepLog(`STEP 11 running schemaMigrationService.updateCollection...`);
          try {
            await this.schemaMigrationService.updateCollection(
              exists.name,
              oldMetadata,
              finalMetadata,
              rawSnapshot,
            );
            stepLog(`STEP 11 updateCollection done (+${lap()}ms)`);
          } catch (ddlError) {
            stepLog(`STEP 11 updateCollection FAILED, restoring metadata from snapshot...`);
            this.loggingService.error(
              'DDL failed after metadata update, restoring metadata from snapshot',
              {
                context: 'updateTable',
                error: ddlError.message,
                tableName: exists.name,
              },
            );
            await this.restoreMetadataFromSnapshot(rawSnapshot, queryId).catch(
              (restoreErr) => {
                this.loggingService.error(
                  'Metadata restore ALSO failed — manual intervention required',
                  {
                    context: 'updateTable',
                    error: restoreErr.message,
                    tableName: exists.name,
                  },
                );
              },
            );
            throw ddlError;
          }

          const oldM2mJunctions = new Set<string>(
            (oldMetadata.relations || [])
              .filter((r: any) => r.type === 'many-to-many' && !r.mappedBy && r.junctionTableName)
              .map((r: any) => r.junctionTableName as string),
          );
          const newM2mJunctions = (finalMetadata.relations || []).filter(
            (r: any) => r.type === 'many-to-many' && !r.mappedBy && r.junctionTableName,
          );
          for (const j of newM2mJunctions) {
            if (!oldM2mJunctions.has(j.junctionTableName)) {
              await this.schemaMigrationService.ensureJunctionCollection(
                j.junctionTableName,
                j.junctionSourceColumn,
                j.junctionTargetColumn,
              );
            }
          }
          for (const oldJunctionName of [...oldM2mJunctions]) {
            if (!newM2mJunctions.some((r: any) => r.junctionTableName === oldJunctionName)) {
              await this.schemaMigrationService.dropJunctionCollection(oldJunctionName);
            }
          }
          if (body.name && body.name !== exists.name) {
            for (const rel of oldMetadata.relations || []) {
              if (rel.type !== 'many-to-many' || rel.mappedBy || !rel.junctionTableName) continue;
              if (!newM2mJunctions.some((r: any) => r.junctionTableName === rel.junctionTableName)) continue;
              const newJunction = getJunctionTableName(body.name, rel.propertyName, rel.targetTableName);
              if (rel.junctionTableName !== newJunction) {
                await this.schemaMigrationService.renameJunctionCollection(
                  rel.junctionTableName,
                  newJunction,
                );
              }
            }
          }
          stepLog(`STEP 12 junction collections synced (+${lap()}ms)`);
        }

        if (body.isSingleRecord === true && !exists.isSingleRecord) {
          const db = this.mongoService.getDb();
          const count = await db.collection(exists.name).countDocuments();

          if (count === 0) {
            const defaultRecord = generateDefaultRecord(
              finalMetadata?.columns || [],
            );
            await db.collection(exists.name).insertOne(defaultRecord);
          } else if (count > 1) {
            const firstRecord = await db
              .collection(exists.name)
              .find()
              .sort({ _id: 1 })
              .limit(1)
              .toArray();
            if (firstRecord[0]?._id) {
              await db
                .collection(exists.name)
                .deleteMany({ _id: { $ne: firstRecord[0]._id } });
            }
          }
        }

        if (body.graphqlEnabled !== undefined) {
          const db = this.mongoService.getDb();
          const pkField = DatabaseConfigService.getPkField();
          const existingGql = await this.queryBuilder.findOne({
            table: 'gql_definition',
            where: { table: exists[pkField] },
          });
          if (existingGql) {
            await db.collection('gql_definition').updateOne(
              { _id: existingGql._id },
              { $set: { isEnabled: body.graphqlEnabled === true, updatedAt: new Date() } },
            );
          } else {
            await db.collection('gql_definition').insertOne({
              table: exists[pkField],
              isEnabled: body.graphqlEnabled === true,
              isSystem: exists.isSystem || false,
              createdAt: new Date(),
              updatedAt: new Date(),
            });
          }
          stepLog(`STEP 14 gql_definition sync done (+${lap()}ms)`);
        }

        finalMetadata.affectedTables = [...affectedTableNames];
        stepLog(`STEP 15 isSingleRecord cleanup done (+${lap()}ms)`);
        return finalMetadata;
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
    stepLog(`STEP DONE total=${Date.now() - t0}ms`);
    return out;
  }
  async delete(id: string | number, context?: TDynamicContext) {
    const affectedTableNames = new Set<string>();
    return await this.runWithSchemaLock(`mongo:delete:${id}`, async () => {
      try {
        const { ObjectId } = require('mongodb');
        const tableId = typeof id === 'string' ? new ObjectId(id) : id;
        const exists = await this.queryBuilder.findOne({
          table: 'table_definition',
          where: { _id: tableId },
        });
        if (!exists) {
          throw new ResourceNotFoundException('table_definition', String(id));
        }
        if (exists.isSystem) {
          throw new ValidationException('Cannot delete system table', {
            tableId: id,
            tableName: exists.name,
          });
        }
        const collectionName = exists.name;
        const decision = await this.policyService.checkSchemaMigration({
          operation: 'delete',
          tableName: collectionName,
          currentUser: context?.$user,
          requestContext: context,
        });
        if (isPolicyDeny(decision)) {
          throw new ValidationException(decision.message, decision.details);
        }
        const { data: routes } = await this.queryBuilder.find({
          table: 'route_definition',
          where: {
            mainTable: tableId,
          },
        });
        for (const route of routes) {
          await this.queryBuilder.delete('route_definition', route._id);
        }
        const { data: relations } = await this.queryBuilder.find({
          table: 'relation_definition',
          where: {
            sourceTable: tableId,
          },
        });
        const { data: targetRelations } = await this.queryBuilder.find({
          table: 'relation_definition',
          where: {
            targetTable: tableId,
          },
        });
        const allRelations = [...relations, ...targetRelations];
        const droppedJunctions = new Set<string>();
        for (const rel of allRelations) {
          if (rel.targetTableName) affectedTableNames.add(rel.targetTableName);
          if (rel.sourceTableName) affectedTableNames.add(rel.sourceTableName);
          if (
            rel.type === 'many-to-many' &&
            rel.junctionTableName &&
            !droppedJunctions.has(rel.junctionTableName)
          ) {
            await this.schemaMigrationService.dropJunctionCollection(
              rel.junctionTableName,
            );
            droppedJunctions.add(rel.junctionTableName);
          }
          await this.queryBuilder.delete('relation_definition', rel._id);
        }
        const { data: columns } = await this.queryBuilder.find({
          table: 'column_definition',
          where: {
            table: tableId,
          },
        });
        for (const col of columns) {
          await this.queryBuilder.delete('column_definition', col._id);
        }
        await this.queryBuilder.delete('table_definition', tableId);
        await this.schemaMigrationService.dropCollection(collectionName);
        exists.affectedTables = [...affectedTableNames];
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
  private async getFullTableMetadata(tableId: any): Promise<any> {
    const { ObjectId } = require('mongodb');
    const queryId =
      typeof tableId === 'string' ? new ObjectId(tableId) : tableId;

    // Use direct MongoDB queries to avoid filter DSL routing issues
    // where FK column names (e.g. 'table', 'sourceTable') collide with relation names
    const db = this.mongoService.getDb();
    const normalize = (doc: any) => {
      if (!doc) return doc;
      const normalized: any = {};
      for (const [key, value] of Object.entries(doc)) {
        if (value instanceof ObjectId) {
          normalized[key] = value.toString();
        } else if (value instanceof Date) {
          normalized[key] = value.toISOString();
        } else {
          normalized[key] = value;
        }
      }
      return normalized;
    };

    const rawTable = await db.collection('table_definition').findOne({ _id: queryId });
    if (!rawTable) return null;
    const table = normalize(rawTable);

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
    const rawColumns = await db.collection('column_definition').find({ table: queryId }).toArray();
    const columns = rawColumns.map(normalize);
    table.columns = columns;
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e) {}
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e) {}
      }
    }
    const rawRelations = await db.collection('relation_definition').find({ sourceTable: queryId }).toArray();
    const relations = rawRelations.map(normalize);
    table.relations = relations;
    return table;
  }
  private async runWithSchemaLock<T>(
    context: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    const lock = await this.schemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.schemaMigrationLockService.release(lock);
    }
  }

  private async captureRawMetadataSnapshot(tableId: any): Promise<{
    table: any;
    columns: any[];
    relations: any[];
    inverseRelations: any[];
  }> {
    const { ObjectId } = require('mongodb');
    const db = this.mongoService.getDb();
    const oid = typeof tableId === 'string' ? new ObjectId(tableId) : tableId;
    const sourceRelations = await db
      .collection('relation_definition')
      .find({ sourceTable: oid })
      .toArray();
    const owningRelIds = sourceRelations
      .filter((r: any) => !r.mappedBy)
      .map((r: any) => r._id);
    const inverseRelations =
      owningRelIds.length > 0
        ? await db
            .collection('relation_definition')
            .find({ mappedBy: { $in: owningRelIds } })
            .toArray()
        : [];
    return {
      table: await db.collection('table_definition').findOne({ _id: oid }),
      columns: await db
        .collection('column_definition')
        .find({ table: oid })
        .toArray(),
      relations: sourceRelations,
      inverseRelations,
    };
  }

  private async restoreMetadataFromSnapshot(
    snapshot: {
      table: any;
      columns: any[];
      relations: any[];
      inverseRelations: any[];
    },
    tableId: any,
  ): Promise<void> {
    const { ObjectId } = require('mongodb');
    const db = this.mongoService.getDb();
    const oid = typeof tableId === 'string' ? new ObjectId(tableId) : tableId;
    this.logger.warn(
      `Restoring metadata from snapshot for table ${snapshot.table?.name} (${oid})`,
    );

    // 1. Restore table document
    if (snapshot.table) {
      await db
        .collection('table_definition')
        .replaceOne({ _id: oid }, snapshot.table, { upsert: true });
    }

    // 2. Restore columns (delete current, insert from snapshot)
    await db.collection('column_definition').deleteMany({ table: oid });
    if (snapshot.columns && snapshot.columns.length > 0) {
      await db.collection('column_definition').insertMany(snapshot.columns);
    }

    // 3. Restore source relations (delete current, insert from snapshot)
    await db.collection('relation_definition').deleteMany({ sourceTable: oid });
    if (snapshot.relations && snapshot.relations.length > 0) {
      await db
        .collection('relation_definition')
        .insertMany(snapshot.relations);
    }

    // 4. Clean up auto-created inverse relations on other tables
    const currentSourceRels = await db
      .collection('relation_definition')
      .find({ sourceTable: oid })
      .toArray();
    const owningRelIds = currentSourceRels
      .filter((r: any) => !r.mappedBy)
      .map((r: any) => r._id);
    if (owningRelIds.length > 0) {
      const currentInverse = await db
        .collection('relation_definition')
        .find({ mappedBy: { $in: owningRelIds } })
        .toArray();
      const snapshotInverseIds = new Set<string>(
        (snapshot.inverseRelations || []).map((r: any) =>
          String(r._id),
        ),
      );
      for (const inv of currentInverse) {
        if (!snapshotInverseIds.has(String(inv._id))) {
          await db
            .collection('relation_definition')
            .deleteOne({ _id: inv._id });
          this.logger.warn(
            `Cleaned up auto-created inverse relation ${inv.propertyName} (${inv._id})`,
          );
        }
      }
    }

    // 5. Re-insert snapshot inverse relations (if any were deleted during migration)
    for (const invRel of snapshot.inverseRelations || []) {
      const exists = await db
        .collection('relation_definition')
        .findOne({ _id: invRel._id });
      if (!exists) {
        await db.collection('relation_definition').insertOne(invRel);
        this.logger.warn(
          `Restored inverse relation ${invRel.propertyName} (${invRel._id})`,
        );
      }
    }

    this.logger.warn(`Metadata restore completed for table ${snapshot.table?.name}`);
  }
}
