import { Logger } from '../../../shared/logger';
import { ObjectId } from 'mongodb';
import {
  QueryBuilderService,
  getJunctionTableName,
  getJunctionColumnNames,
} from '@enfyra/kernel';
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
import { generateDefaultRecord } from '../utils/generate-default-record';
import { DEFAULT_REST_HANDLER_LOGIC } from '../../../domain/bootstrap';
import { compileScriptSource } from '@enfyra/kernel';
import { TableManagementValidationService } from './table-validation.service';
import { MongoMetadataSnapshotService } from './mongo-metadata-snapshot.service';
import {
  MONGO_PRIMARY_KEY_TYPE,
  isMongoPrimaryKeyType,
  normalizeMongoPrimaryKeyColumn,
} from '../utils/mongo-primary-key.util';
export class MongoTableHandlerService {
  private logger = new Logger(MongoTableHandlerService.name);
  private queryBuilderService: QueryBuilderService;
  private mongoSchemaMigrationService: MongoSchemaMigrationService;
  private mongoPhysicalMigrationService: MongoPhysicalMigrationService;
  private mongoService: MongoService;
  private mongoSchemaMigrationLockService: MongoSchemaMigrationLockService;
  private metadataCacheService: MetadataCacheService;
  private loggingService: LoggingService;
  private policyService: PolicyService;
  private tableValidationService: TableManagementValidationService;
  private mongoMetadataSnapshotService: MongoMetadataSnapshotService;
  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    mongoSchemaMigrationService: MongoSchemaMigrationService;
    mongoPhysicalMigrationService: MongoPhysicalMigrationService;
    mongoService: MongoService;
    mongoSchemaMigrationLockService: MongoSchemaMigrationLockService;
    metadataCacheService: MetadataCacheService;
    loggingService: LoggingService;
    policyService: PolicyService;
    tableManagementValidationService: TableManagementValidationService;
    mongoMetadataSnapshotService: MongoMetadataSnapshotService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.mongoSchemaMigrationService = deps.mongoSchemaMigrationService;
    this.mongoPhysicalMigrationService = deps.mongoPhysicalMigrationService;
    this.mongoService = deps.mongoService;
    this.mongoSchemaMigrationLockService = deps.mongoSchemaMigrationLockService;
    this.metadataCacheService = deps.metadataCacheService;
    this.loggingService = deps.loggingService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableManagementValidationService;
    this.mongoMetadataSnapshotService = deps.mongoMetadataSnapshotService;
  }
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
                if (rel.mappedBy) {
                  const { data: owningRels } =
                    await this.queryBuilderService.find({
                      table: 'relation_definition',
                      where: {
                        sourceTable: targetTableObjectId,
                        propertyName: rel.mappedBy,
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
                if (
                  rel.type === 'many-to-many' &&
                  !rel.mappedBy &&
                  targetName
                ) {
                  const junctionTableName = getJunctionTableName(
                    body.name,
                    rel.propertyName,
                    targetName,
                  );
                  const { sourceColumn, targetColumn } = getJunctionColumnNames(
                    body.name,
                    rel.propertyName,
                    targetName,
                  );
                  relationData.junctionTableName = junctionTableName;
                  relationData.junctionSourceColumn = sourceColumn;
                  relationData.junctionTargetColumn = targetColumn;
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
                  if (rel.mappedBy) {
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
                  if (rel.type === 'many-to-many') {
                    const invTargetName =
                      typeof rel.targetTable === 'string'
                        ? rel.targetTable
                        : rel.targetTable?.name;
                    if (invTargetName) {
                      const junctionTableName = getJunctionTableName(
                        body.name,
                        rel.propertyName,
                        invTargetName,
                      );
                      const { sourceColumn, targetColumn } =
                        getJunctionColumnNames(
                          body.name,
                          rel.propertyName,
                          invTargetName,
                        );
                      inverseData.junctionTableName = junctionTableName;
                      inverseData.junctionSourceColumn = targetColumn;
                      inverseData.junctionTargetColumn = sourceColumn;
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
          const existingRoute = await this.queryBuilderService.findOne({
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
                sourceCode: DEFAULT_REST_HANDLER_LOGIC[methodName],
                scriptLanguage: 'typescript',
                compiledCode: compileScriptSource(
                  DEFAULT_REST_HANDLER_LOGIC[methodName],
                  'typescript',
                ),
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
                  .collection(
                    'route_definition_availableMethods_method_definition',
                  )
                  .insertMany(junctionRows, { ordered: false });
              } catch (err: any) {
                if (err?.code !== 11000) throw err;
              }
            }
          }

          // Auto-create gql_definition for new table
          const existingGql = await this.queryBuilderService.findOne({
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
  async updateTable(
    id: any,
    body: TCreateTableBody,
    context?: TDynamicContext,
  ) {
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
      const bodyRelations = body.relations ?? [];
      this.tableValidationService.validateRelations(bodyRelations);
      stepLog(`STEP 2 validated name+relations (+${lap()}ms)`);
      try {
        const queryId = typeof id === 'string' ? new ObjectId(id) : id;
        const exists = await this.queryBuilderService.findOne({
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
        const rawSnapshot =
          await this.mongoMetadataSnapshotService.captureRawMetadataSnapshot(
            queryId,
          );
        stepLog(`STEP 6b snapshot captured (+${lap()}ms)`);
        const updateData: any = {};
        if ('name' in body) updateData.name = body.name;
        if ('alias' in body) updateData.alias = body.alias;
        if ('description' in body) updateData.description = body.description;
        if ('uniques' in body) updateData.uniques = body.uniques;
        if ('indexes' in body) updateData.indexes = body.indexes;
        if ('isSingleRecord' in body)
          updateData.isSingleRecord = body.isSingleRecord;
        if ('validateBody' in body) updateData.validateBody = body.validateBody;
        if (Object.keys(updateData).length > 0) {
          await this.queryBuilderService.update(
            'table_definition',
            id,
            updateData,
          );
        }
        stepLog(`STEP 7 updated table_definition row (+${lap()}ms)`);
        const renamedColumns: Array<{ oldName: string; newName: string }> = [];
        if (body.columns) {
          const { data: existingColumns } = await this.queryBuilderService.find(
            {
              table: 'column_definition',
              where: {
                table: queryId,
              },
            },
          );
          const deletedColumnIds = getDeletedIds(existingColumns, body.columns);
          for (const colId of deletedColumnIds) {
            await this.queryBuilderService.delete('column_definition', colId);
          }
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
                });
              }
            }
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
              await this.queryBuilderService.update(
                'column_definition',
                colId,
                columnData,
              );
              colObjectId =
                typeof colId === 'string' ? new ObjectId(colId) : colId;
            } else {
              const inserted = await this.queryBuilderService.insert(
                'column_definition',
                columnData,
              );
              colObjectId =
                typeof inserted._id === 'string'
                  ? new ObjectId(inserted._id)
                  : inserted._id;
            }
            columnIds.push(colObjectId);
            await this.writeNestedRulesMongo({
              rules: (col as any).rules,
              subjectFk: 'column',
              subjectFkValue: colObjectId,
            });
            await this.writeNestedFieldPermissionsMongo({
              permissions: (col as any).fieldPermissions,
              subjectFk: 'column',
              subjectFkValue: colObjectId,
            });
          }
        }
        stepLog(
          `STEP 8 processed ${body.columns?.length ?? 0} column(s) (+${lap()}ms)`,
        );
        if (body.relations) {
          const { data: existingRelations } =
            await this.queryBuilderService.find({
              table: 'relation_definition',
              where: {
                sourceTable: queryId,
              },
            });
          const deletedRelationIds = getDeletedIds(
            existingRelations,
            body.relations,
          );
          for (const relId of deletedRelationIds) {
            const deletedRelation = existingRelations.find(
              (r: any) => r._id?.toString() === relId.toString(),
            );
            if (deletedRelation) {
              const { data: inverseRels } = await this.queryBuilderService.find(
                {
                  table: 'relation_definition',
                  where: { mappedBy: deletedRelation._id },
                },
              );
              for (const inv of inverseRels) {
                if (inv.sourceTableName)
                  affectedTableNames.add(inv.sourceTableName);
                await this.queryBuilderService.delete(
                  'relation_definition',
                  inv._id,
                );
              }
            }
            await this.queryBuilderService.delete('relation_definition', relId);
          }
          const relationIds = [];
          for (const rel of body.relations) {
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
              const targetTableRecord = await this.queryBuilderService.findOne({
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
            let resolvedTargetTableName: string | undefined;
            if (typeof rel.targetTable === 'string') {
              resolvedTargetTableName = rel.targetTable;
            } else {
              resolvedTargetTableName = rel.targetTable?.name;
              if (!resolvedTargetTableName) {
                const targetRec = await this.queryBuilderService.findOne({
                  table: 'table_definition',
                  where: { _id: targetTableObjectId },
                });
                resolvedTargetTableName = targetRec?.name;
              }
            }
            let updateResolvedMappedBy = null;
            if (rel.mappedBy) {
              const { data: owningRels } = await this.queryBuilderService.find({
                table: 'relation_definition',
                where: {
                  sourceTable: targetTableObjectId,
                  propertyName: rel.mappedBy,
                },
              });
              if (owningRels.length > 0)
                updateResolvedMappedBy = owningRels[0]._id;
            }
            const relationData: any = {
              propertyName: rel.propertyName,
              type: rel.type,
              sourceTable: queryId,
              targetTable: targetTableObjectId,
              targetTableName: resolvedTargetTableName || exists.name,
              sourceTableName: exists.name,
              mappedBy: updateResolvedMappedBy,
              isNullable: rel.isNullable ?? true,
              isSystem: rel.isSystem || false,
              isUpdatable: rel.isUpdatable ?? true,
              isPublished: rel.isPublished ?? true,
              onDelete: rel.onDelete || 'SET NULL',
              description: rel.description,
            };
            const targetRelName = resolvedTargetTableName;
            if (rel.type === 'many-to-many' && targetRelName) {
              if (rel.mappedBy && updateResolvedMappedBy) {
                const owningRel = await this.queryBuilderService.findOne({
                  table: 'relation_definition',
                  where: { _id: updateResolvedMappedBy },
                });
                if (owningRel?.junctionTableName) {
                  relationData.junctionTableName = owningRel.junctionTableName;
                  relationData.junctionSourceColumn =
                    owningRel.junctionTargetColumn;
                  relationData.junctionTargetColumn =
                    owningRel.junctionSourceColumn;
                }
              } else if (!rel.mappedBy) {
                const junctionTableName = getJunctionTableName(
                  exists.name,
                  rel.propertyName,
                  targetRelName,
                );
                const { sourceColumn, targetColumn } = getJunctionColumnNames(
                  exists.name,
                  rel.propertyName,
                  targetRelName,
                );
                relationData.junctionTableName = junctionTableName;
                relationData.junctionSourceColumn = sourceColumn;
                relationData.junctionTargetColumn = targetColumn;
              }
            }
            let relObjectId;
            if (rel._id || rel.id) {
              const relId = rel._id || rel.id;
              await this.queryBuilderService.update(
                'relation_definition',
                relId,
                relationData,
              );
              relObjectId =
                typeof relId === 'string' ? new ObjectId(relId) : relId;
            } else {
              const inserted = await this.queryBuilderService.insert(
                'relation_definition',
                relationData,
              );
              relObjectId =
                typeof inserted._id === 'string'
                  ? new ObjectId(inserted._id)
                  : inserted._id;
            }
            relationIds.push(relObjectId);
            await this.writeNestedFieldPermissionsMongo({
              permissions: (rel as any).fieldPermissions,
              subjectFk: 'relation',
              subjectFkValue: relObjectId,
            });
            if (rel.inversePropertyName && !(rel._id || rel.id)) {
              if (rel.mappedBy) {
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
                onDelete: rel.onDelete || 'SET NULL',
              };
              if (rel.type === 'many-to-many' && targetRelName) {
                const junctionTableName = getJunctionTableName(
                  exists.name,
                  rel.propertyName,
                  targetRelName,
                );
                const { sourceColumn, targetColumn } = getJunctionColumnNames(
                  exists.name,
                  rel.propertyName,
                  targetRelName,
                );
                inverseData.junctionTableName = junctionTableName;
                inverseData.junctionSourceColumn = targetColumn;
                inverseData.junctionTargetColumn = sourceColumn;
              }
              await this.queryBuilderService.insert(
                'relation_definition',
                inverseData,
              );
              const invTargetName =
                typeof rel.targetTable === 'string'
                  ? rel.targetTable
                  : rel.targetTable?.name;
              if (invTargetName) affectedTableNames.add(invTargetName);
              this.logger.log(
                `Auto-created inverse relation '${rel.inversePropertyName}'`,
              );
            }
          }
        }
        stepLog(
          `STEP 9 processed ${body.relations?.length ?? 0} relation(s) (+${lap()}ms)`,
        );
        const finalMetadata =
          await this.mongoMetadataSnapshotService.getFullTableMetadata(id);
        stepLog(`STEP 10 loaded finalMetadata (+${lap()}ms)`);

        if (
          schemaDecision?.details?.schemaChanged === true &&
          oldMetadata &&
          finalMetadata
        ) {
          stepLog(`STEP 11 running schemaMigrationService.updateCollection...`);
          try {
            await this.mongoSchemaMigrationService.updateCollection(
              exists.name,
              oldMetadata,
              finalMetadata,
              rawSnapshot,
            );
            stepLog(`STEP 11 updateCollection done (+${lap()}ms)`);
          } catch (ddlError: any) {
            stepLog(
              `STEP 11 updateCollection FAILED, restoring metadata from snapshot...`,
            );
            this.loggingService.error(
              'DDL failed after metadata update, restoring metadata from snapshot',
              {
                context: 'updateTable',
                error: ddlError.message,
                tableName: exists.name,
              },
            );
            await this.mongoMetadataSnapshotService
              .restoreMetadataFromSnapshot(rawSnapshot, queryId)
              .catch((restoreErr) => {
                this.loggingService.error(
                  'Metadata restore ALSO failed — manual intervention required',
                  {
                    context: 'updateTable',
                    error: restoreErr.message,
                    tableName: exists.name,
                  },
                );
              });
            throw ddlError;
          }

          const oldM2mJunctions = new Set<string>(
            (oldMetadata.relations || [])
              .filter(
                (r: any) =>
                  r.type === 'many-to-many' &&
                  !r.mappedBy &&
                  r.junctionTableName,
              )
              .map((r: any) => r.junctionTableName as string),
          );
          const newM2mJunctions = (finalMetadata.relations || []).filter(
            (r: any) =>
              r.type === 'many-to-many' && !r.mappedBy && r.junctionTableName,
          );
          for (const j of newM2mJunctions) {
            if (!oldM2mJunctions.has(j.junctionTableName)) {
              await this.mongoSchemaMigrationService.ensureJunctionCollection(
                j.junctionTableName,
                j.junctionSourceColumn,
                j.junctionTargetColumn,
              );
            }
          }
          for (const oldJunctionName of [...oldM2mJunctions]) {
            if (
              !newM2mJunctions.some(
                (r: any) => r.junctionTableName === oldJunctionName,
              )
            ) {
              await this.mongoSchemaMigrationService.dropJunctionCollection(
                oldJunctionName,
              );
            }
          }
          if (body.name && body.name !== exists.name) {
            for (const rel of oldMetadata.relations || []) {
              if (
                rel.type !== 'many-to-many' ||
                rel.mappedBy ||
                !rel.junctionTableName
              )
                continue;
              if (
                !newM2mJunctions.some(
                  (r: any) => r.junctionTableName === rel.junctionTableName,
                )
              )
                continue;
              const newJunction = getJunctionTableName(
                body.name,
                rel.propertyName,
                rel.targetTableName,
              );
              if (rel.junctionTableName !== newJunction) {
                await this.mongoSchemaMigrationService.renameJunctionCollection(
                  rel.junctionTableName,
                  newJunction,
                );
              }
            }
          }
          stepLog(`STEP 12 junction collections synced (+${lap()}ms)`);
        }

        if (renamedColumns.length > 0) {
          await this.mongoPhysicalMigrationService.enqueueFieldRenames(
            exists.name,
            renamedColumns,
          );
          stepLog(`STEP 13 queued physical field rename jobs (+${lap()}ms)`);
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
          const existingGql = await this.queryBuilderService.findOne({
            table: 'gql_definition',
            where: { table: exists[pkField] },
          });
          if (existingGql) {
            await db.collection('gql_definition').updateOne(
              { _id: existingGql._id },
              {
                $set: {
                  isEnabled: body.graphqlEnabled === true,
                  updatedAt: new Date(),
                },
              },
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
      } catch (error: any) {
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
        const tableId = typeof id === 'string' ? new ObjectId(id) : id;
        const exists = await this.queryBuilderService.findOne({
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
        const { data: routes } = await this.queryBuilderService.find({
          table: 'route_definition',
          where: {
            mainTable: tableId,
          },
        });
        for (const route of routes) {
          await this.queryBuilderService.delete('route_definition', route._id);
        }
        const { data: relations } = await this.queryBuilderService.find({
          table: 'relation_definition',
          where: {
            sourceTable: tableId,
          },
        });
        const { data: targetRelations } = await this.queryBuilderService.find({
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
            await this.mongoSchemaMigrationService.dropJunctionCollection(
              rel.junctionTableName,
            );
            droppedJunctions.add(rel.junctionTableName);
          }
          await this.queryBuilderService.delete('relation_definition', rel._id);
        }
        const { data: columns } = await this.queryBuilderService.find({
          table: 'column_definition',
          where: {
            table: tableId,
          },
        });
        for (const col of columns) {
          await this.queryBuilderService.delete('column_definition', col._id);
        }
        await this.queryBuilderService.delete('table_definition', tableId);
        await this.mongoSchemaMigrationService.dropCollection(collectionName);
        exists.affectedTables = [...affectedTableNames];
        return exists;
      } catch (error: any) {
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
  private async writeNestedRulesMongo(opts: {
    rules: any[] | undefined;
    subjectFk: 'column' | 'relation';
    subjectFkValue: any;
  }): Promise<void> {
    if (!Array.isArray(opts.rules)) return;
    const { data: existing } = await this.queryBuilderService.find({
      table: 'column_rule_definition',
      where: { [opts.subjectFk]: opts.subjectFkValue },
    });
    const deletedIds = getDeletedIds(existing, opts.rules);
    for (const rid of deletedIds) {
      await this.queryBuilderService.delete('column_rule_definition', rid);
    }
    for (const rule of opts.rules) {
      const ruleData: any = {
        ruleType: rule.ruleType,
        value: rule.value ?? null,
        message: rule.message ?? null,
        isEnabled: rule.isEnabled !== false,
        [opts.subjectFk]: opts.subjectFkValue,
      };
      const ruleId = rule._id || rule.id;
      if (ruleId) {
        await this.queryBuilderService.update(
          'column_rule_definition',
          ruleId,
          ruleData,
        );
      } else {
        await this.queryBuilderService.insert(
          'column_rule_definition',
          ruleData,
        );
      }
    }
  }

  private async writeNestedFieldPermissionsMongo(opts: {
    permissions: any[] | undefined;
    subjectFk: 'column' | 'relation';
    subjectFkValue: any;
  }): Promise<void> {
    if (!Array.isArray(opts.permissions)) return;
    const { data: existing } = await this.queryBuilderService.find({
      table: 'field_permission_definition',
      where: { [opts.subjectFk]: opts.subjectFkValue },
    });
    const deletedIds = getDeletedIds(existing, opts.permissions);
    for (const pid of deletedIds) {
      await this.queryBuilderService.delete('field_permission_definition', pid);
    }
    for (const perm of opts.permissions) {
      const roleRef =
        perm.role && typeof perm.role === 'object'
          ? perm.role._id || perm.role.id
          : perm.role;
      const allowedUserIds = Array.isArray(perm.allowedUsers)
        ? perm.allowedUsers
            .map((u: any) => (typeof u === 'object' ? u._id || u.id : u))
            .filter((v: any) => v != null)
        : undefined;
      const permData: any = {
        action: perm.action,
        effect: perm.effect ?? 'allow',
        condition: perm.condition ?? null,
        isEnabled: perm.isEnabled !== false,
        description: perm.description ?? null,
        role: roleRef ?? null,
        column: opts.subjectFk === 'column' ? opts.subjectFkValue : null,
        relation: opts.subjectFk === 'relation' ? opts.subjectFkValue : null,
        ...(allowedUserIds !== undefined && { allowedUsers: allowedUserIds }),
      };
      const permId = perm._id || perm.id;
      if (permId) {
        await this.queryBuilderService.update(
          'field_permission_definition',
          permId,
          permData,
        );
      } else {
        await this.queryBuilderService.insert(
          'field_permission_definition',
          permData,
        );
      }
    }
  }

  private async getFullTableMetadata(tableId: any): Promise<any> {
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

    const rawTable = await db
      .collection('table_definition')
      .findOne({ _id: queryId });
    if (!rawTable) return null;
    const table = normalize(rawTable);

    if (table.uniques && typeof table.uniques === 'string') {
      try {
        table.uniques = JSON.parse(table.uniques);
      } catch (e: any) {
        table.uniques = [];
      }
    }
    if (table.indexes && typeof table.indexes === 'string') {
      try {
        table.indexes = JSON.parse(table.indexes);
      } catch (e: any) {
        table.indexes = [];
      }
    }
    const rawColumns = await db
      .collection('column_definition')
      .find({ table: queryId })
      .toArray();
    const columns = rawColumns.map(normalize);
    table.columns = columns;
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e: any) {}
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e: any) {}
      }
    }
    const rawRelations = await db
      .collection('relation_definition')
      .find({ sourceTable: queryId })
      .toArray();
    const relations = rawRelations.map(normalize);
    table.relations = relations;
    return table;
  }
  private async runWithSchemaLock<T>(
    context: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    const lock = await this.mongoSchemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.mongoSchemaMigrationLockService.release(lock);
    }
  }
}
