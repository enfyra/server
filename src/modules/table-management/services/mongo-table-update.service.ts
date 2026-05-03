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

export class MongoTableUpdateService extends MongoTableHandlerService {
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
        let preloadedRelations: any[] | null = null;
        if (body.relations) {
          const { data: existingRelations } =
            await this.queryBuilderService.find({
              table: 'relation_definition',
              where: {
                sourceTable: queryId,
              },
            });
          preloadedRelations = existingRelations;
          for (const rel of body.relations) {
            const relId = rel._id || rel.id;
            const existingRel = relId
              ? existingRelations.find(
                  (r: any) => String(r._id) === String(relId),
                )
              : null;
            if (existingRel && existingRel.type !== rel.type) {
              throw new ValidationException(
                `Cannot change relation type from '${existingRel.type}' to '${rel.type}' for property '${rel.propertyName}'. Please delete the old relation and create a new one.`,
                { relationName: rel.propertyName },
              );
            }

            let targetTableObjectId;
            const targetTableIdFromObj =
              typeof rel.targetTable === 'object'
                ? rel.targetTable._id || rel.targetTable.id
                : null;
            if (targetTableIdFromObj) {
              try {
                targetTableObjectId =
                  typeof targetTableIdFromObj === 'string'
                    ? new ObjectId(targetTableIdFromObj)
                    : targetTableIdFromObj;
              } catch {
                throw new ValidationException(
                  `Target table not found for relation ${rel.propertyName}`,
                  { relationName: rel.propertyName },
                );
              }
              const targetExists = await this.queryBuilderService.findOne({
                table: 'table_definition',
                where: { _id: targetTableObjectId },
              });
              if (!targetExists) {
                throw new ValidationException(
                  `Target table not found for relation ${rel.propertyName}`,
                  { relationName: rel.propertyName },
                );
              }
            } else if (typeof rel.targetTable === 'string') {
              const targetTableRecord = await this.queryBuilderService.findOne({
                table: 'table_definition',
                where: { name: rel.targetTable },
              });
              if (!targetTableRecord) {
                throw new ValidationException(
                  `Target table not found for relation ${rel.propertyName}`,
                  { relationName: rel.propertyName },
                );
              }
              targetTableObjectId =
                typeof targetTableRecord._id === 'string'
                  ? new ObjectId(targetTableRecord._id)
                  : targetTableRecord._id;
            }

            const mappedByProperty = getRelationMappedByProperty(rel);
            if (mappedByProperty && targetTableObjectId) {
              const { data: owningRels } = await this.queryBuilderService.find({
                table: 'relation_definition',
                where: {
                  sourceTable: targetTableObjectId,
                  propertyName: mappedByProperty,
                },
              });
              if (owningRels.length === 0) {
                throw new ValidationException(
                  `mappedBy relation '${mappedByProperty}' not found for relation ${rel.propertyName}`,
                  {
                    relationName: rel.propertyName,
                    mappedBy: mappedByProperty,
                  },
                );
              }
            }
          }
        }
        const allowedConstraintFields = this.getAllowedConstraintFields(body);
        const bodyUniques =
          body.uniques && allowedConstraintFields
            ? this.normalizeConstraintGroups(
                body.uniques,
                oldMetadata,
                body,
                allowedConstraintFields,
              )
            : body.uniques;
        const bodyIndexes =
          body.indexes && allowedConstraintFields
            ? this.normalizeConstraintGroups(
                body.indexes,
                oldMetadata,
                body,
                allowedConstraintFields,
              )
            : body.indexes;

        let schemaDecision: any = null;
        if (oldMetadata) {
          const afterMetadata = {
            name: body.name ?? exists.name,
            columns: body.columns ?? oldMetadata?.columns ?? [],
            relations: body.relations ?? oldMetadata?.relations ?? [],
            uniques: bodyUniques ?? exists.uniques ?? oldMetadata?.uniques,
            indexes: bodyIndexes ?? exists.indexes ?? oldMetadata?.indexes,
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
        if ('uniques' in body) updateData.uniques = bodyUniques;
        if ('indexes' in body) updateData.indexes = bodyIndexes;
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
          const existingRelations =
            preloadedRelations ??
            (
              await this.queryBuilderService.find({
                table: 'relation_definition',
                where: {
                  sourceTable: queryId,
                },
              })
            ).data;
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
            const relId = rel._id || rel.id;
            const existingRel = relId
              ? existingRelations.find(
                  (r: any) => String(r._id) === String(relId),
                )
              : null;
            if (existingRel && existingRel.type !== rel.type) {
              throw new ValidationException(
                `Cannot change relation type from '${existingRel.type}' to '${rel.type}' for property '${rel.propertyName}'. Please delete the old relation and create a new one.`,
                { relationName: rel.propertyName },
              );
            }
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
              throw new ValidationException(
                `Target table not found for relation ${rel.propertyName}`,
                { relationName: rel.propertyName },
              );
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
            if (resolvedTargetTableName) {
              affectedTableNames.add(resolvedTargetTableName);
            }
            let updateResolvedMappedBy = existingRel?.mappedBy || null;
            const mappedByProperty = getRelationMappedByProperty(rel);
            if (mappedByProperty) {
              const { data: owningRels } = await this.queryBuilderService.find({
                table: 'relation_definition',
                where: {
                  sourceTable: targetTableObjectId,
                  propertyName: mappedByProperty,
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
            const ownsMongoReference =
              (rel.type === 'many-to-one' || rel.type === 'one-to-one') &&
              !updateResolvedMappedBy;
            if (ownsMongoReference) {
              relationData.foreignKeyColumn =
                existingRel?.foreignKeyColumn ||
                rel.foreignKeyColumn ||
                rel.propertyName;
            } else if (updateResolvedMappedBy) {
              const owningRel = await this.queryBuilderService.findOne({
                table: 'relation_definition',
                where: { _id: updateResolvedMappedBy },
              });
              relationData.foreignKeyColumn =
                owningRel?.foreignKeyColumn || owningRel?.propertyName || null;
            } else {
              relationData.foreignKeyColumn = null;
            }
            const targetRelName = resolvedTargetTableName;
            if (rel.type === 'many-to-many' && targetRelName) {
              if (updateResolvedMappedBy) {
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
              } else if (existingRel?.junctionTableName) {
                relationData.junctionTableName = existingRel.junctionTableName;
                relationData.junctionSourceColumn =
                  existingRel.junctionSourceColumn;
                relationData.junctionTargetColumn =
                  existingRel.junctionTargetColumn;
              } else {
                const junction = getSqlJunctionPhysicalNames({
                  sourceTable: exists.name,
                  propertyName: rel.propertyName,
                  targetTable: targetRelName,
                });
                relationData.junctionTableName = junction.junctionTableName;
                relationData.junctionSourceColumn =
                  junction.junctionSourceColumn;
                relationData.junctionTargetColumn =
                  junction.junctionTargetColumn;
              }
            }
            let relObjectId;
            if (relId) {
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
              if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
                inverseData.foreignKeyColumn =
                  relationData.foreignKeyColumn ||
                  rel.foreignKeyColumn ||
                  rel.propertyName;
              }
              if (rel.type === 'many-to-many' && targetRelName) {
                const junction = getSqlJunctionPhysicalNames({
                  sourceTable: exists.name,
                  propertyName: rel.propertyName,
                  targetTable: targetRelName,
                });
                inverseData.junctionTableName = junction.junctionTableName;
                inverseData.junctionSourceColumn =
                  junction.junctionTargetColumn;
                inverseData.junctionTargetColumn =
                  junction.junctionSourceColumn;
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
              const newJunction = getSqlJunctionPhysicalNames({
                sourceTable: body.name,
                propertyName: rel.propertyName,
                targetTable: rel.targetTableName,
              }).junctionTableName;
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
          await ensureMongoSingleRecord({
            mongoService: this.mongoService,
            tableName: exists.name,
            columns: finalMetadata?.columns || [],
            collapseExtraRows: true,
          });
        }

        if (body.graphqlEnabled !== undefined) {
          const pkField = DatabaseConfigService.getPkField();
          await syncMongoGqlDefinition({
            mongoService: this.mongoService,
            queryBuilderService: this.queryBuilderService,
            tableId: exists[pkField],
            isEnabled: body.graphqlEnabled === true,
            isSystem: exists.isSystem || false,
          });
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

}
