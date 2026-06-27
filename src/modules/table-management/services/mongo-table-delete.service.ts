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

export class MongoTableDeleteService extends MongoTableHandlerService {
  async delete(id: string | number, context?: TDynamicContext) {
    const affectedTableNames = new Set<string>();
    return await this.runWithSchemaLock(`mongo:delete:${id}`, async () => {
      this.assertNotAborted();
      try {
        const tableId = typeof id === 'string' ? new ObjectId(id) : id;
        const exists = await this.queryBuilderService.findOne({
          table: 'enfyra_table',
          where: { _id: tableId },
        });
        if (!exists) {
          throw new ResourceNotFoundException('enfyra_table', String(id));
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
          table: 'enfyra_route',
          where: {
            mainTable: tableId,
          },
        });
        for (const route of routes) {
          await this.queryBuilderService.delete('enfyra_route', route._id);
        }
        const { data: relations } = await this.queryBuilderService.find({
          table: 'enfyra_relation',
          where: {
            sourceTable: tableId,
          },
        });
        const { data: targetRelations } = await this.queryBuilderService.find({
          table: 'enfyra_relation',
          where: {
            targetTable: tableId,
          },
        });
        const allRelations = [...relations, ...targetRelations];
        const droppedJunctions = new Set<string>();
        for (const rel of allRelations) {
          this.assertNotAborted();
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
          await this.queryBuilderService.delete('enfyra_relation', rel._id);
        }
        const { data: columns } = await this.queryBuilderService.find({
          table: 'enfyra_column',
          where: {
            table: tableId,
          },
        });
        for (const col of columns) {
          this.assertNotAborted();
          await this.queryBuilderService.delete('enfyra_column', col._id);
        }
        this.assertNotAborted();
        await this.queryBuilderService.delete('enfyra_table', tableId);
        this.assertNotAborted();
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

}
