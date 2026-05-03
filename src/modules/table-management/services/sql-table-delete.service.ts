import { Logger } from '../../../shared/logger';
import type { Knex } from 'knex';
import { getIoAbortSignal } from '@enfyra/kernel';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
} from '@enfyra/kernel';
import {
  SqlSchemaMigrationService,
  SchemaMigrationLockService,
} from '../../../engines/knex';
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
import { TCreateTableBody } from '../types/table-handler.types';
import {
  getRelationTargetTableId,
  relationTargetTableMapKey,
} from '../utils/relation-target-id.util';
import { TableManagementValidationService } from './table-validation.service';
import { SqlTableMetadataBuilderService } from './sql-table-metadata-builder.service';
import { SqlTableMetadataWriterService } from './sql-table-metadata-writer.service';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { ensureSqlTableRouteArtifacts } from './table-route-artifacts.service';
import {
  ensureSqlM2mJunctionTables,
  ensureSqlSingleRecord,
  syncSqlGqlDefinition,
} from './table-post-migration.service';
import { SqlTableHandlerService } from './sql-table-handler-base.service';

export class SqlTableDeleteService extends SqlTableHandlerService {
  async delete(id: string | number, context?: TDynamicContext) {
    return await this.runWithSchemaLock(`table:delete:${id}`, () =>
      this.deleteTableInternal(id, context),
    );
  }
  private async deleteTableInternal(
    id: string | number,
    context?: TDynamicContext,
  ) {
    const knex = this.queryBuilderService.getKnex();
    const affectedTableNames = new Set<string>();
    return await knex.transaction(async (trx: Knex.Transaction) => {
      const abortSignal = getIoAbortSignal();
      if (abortSignal) {
        const onAbort = () => {
          if (!trx.isCompleted()) trx.rollback().catch(() => {});
        };
        if (abortSignal.aborted) throw new Error('Operation aborted');
        abortSignal.addEventListener('abort', onAbort, { once: true });
      }
      try {
        const exists = await trx('table_definition').where({ id }).first();
        if (!exists) {
          throw new ResourceNotFoundException('table_definition', String(id));
        }
        if (exists.isSystem) {
          throw new ValidationException('Cannot delete system table', {
            tableId: id,
            tableName: exists.name,
          });
        }
        const tableName = exists.name;
        const decision = await this.policyService.checkSchemaMigration({
          operation: 'delete',
          tableName,
          currentUser: context?.$user,
          requestContext: context,
        });
        if (isPolicyDeny(decision)) {
          throw new ValidationException(decision.message, decision.details);
        }
        const allRelations = await trx('relation_definition')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .select('*');
        const targetRelations = await trx('relation_definition')
          .where({ targetTableId: id })
          .select('*');
        for (const rel of targetRelations) {
          if (['one-to-many', 'many-to-one', 'one-to-one'].includes(rel.type)) {
            const sourceTable = await trx('table_definition')
              .where({ id: rel.sourceTableId })
              .first();
            if (sourceTable) {
              const { getForeignKeyColumnName } =
                await import('@enfyra/kernel');
              const fkColumn = getForeignKeyColumnName(tableName);
              const columnExists = await trx.schema.hasColumn(
                sourceTable.name,
                fkColumn,
              );
              if (columnExists) {
                try {
                  const dbType = this.queryBuilderService.getDatabaseType();
                  let constraintName: string | null = null;
                  if (dbType === 'postgres') {
                    const result = await trx.raw(
                      `
                      SELECT tc.constraint_name
                      FROM information_schema.table_constraints AS tc
                      JOIN information_schema.key_column_usage AS kcu
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                      WHERE tc.constraint_type = 'FOREIGN KEY'
                        AND tc.table_schema = 'public'
                        AND tc.table_name = ?
                        AND kcu.column_name = ?
                    `,
                      [sourceTable.name, fkColumn],
                    );
                    constraintName = result.rows[0]?.constraint_name || null;
                  } else if (dbType === 'mysql') {
                    const result = await trx.raw(
                      `
                      SELECT CONSTRAINT_NAME as constraint_name
                      FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                      WHERE TABLE_SCHEMA = DATABASE()
                        AND TABLE_NAME = ?
                        AND COLUMN_NAME = ?
                        AND REFERENCED_TABLE_NAME IS NOT NULL
                    `,
                      [sourceTable.name, fkColumn],
                    );
                    constraintName = result[0][0]?.constraint_name || null;
                  }
                  if (constraintName) {
                    const qt =
                      dbType === 'mysql'
                        ? (id: string) => `\`${id}\``
                        : (id: string) => `"${id}"`;
                    await trx.raw(
                      `ALTER TABLE ${qt(sourceTable.name)} DROP CONSTRAINT ${qt(constraintName)}`,
                    );
                  } else {
                  }
                } catch (error: any) {}
                try {
                  await trx.schema.alterTable(sourceTable.name, (table: any) => {
                    table.dropColumn(fkColumn);
                  });
                } catch (error: any) {}
              }
            }
          }
        }
        try {
          const dbType = this.queryBuilderService.getDatabaseType();
          let allFkConstraints;
          if (dbType === 'postgres') {
            const result = await trx.raw(
              `
              SELECT
                tc.table_name,
                kcu.column_name,
                tc.constraint_name,
                ccu.table_name AS referenced_table_name,
                ccu.column_name AS referenced_column_name
              FROM information_schema.table_constraints AS tc
              JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
              JOIN information_schema.constraint_column_usage AS ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
              WHERE tc.constraint_type = 'FOREIGN KEY'
                AND tc.table_schema = 'public'
                AND ccu.table_name = ?
            `,
              [tableName],
            );
            allFkConstraints = result.rows || [];
          } else {
            const result = await trx.raw(
              `
              SELECT
                TABLE_NAME as table_name,
                COLUMN_NAME as column_name,
                CONSTRAINT_NAME as constraint_name,
                REFERENCED_TABLE_NAME as referenced_table_name,
                REFERENCED_COLUMN_NAME as referenced_column_name
              FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
              WHERE TABLE_SCHEMA = DATABASE()
              AND REFERENCED_TABLE_NAME = ?
              AND REFERENCED_COLUMN_NAME IS NOT NULL
            `,
              [tableName],
            );
            allFkConstraints = result[0] || [];
          }
          if (allFkConstraints && allFkConstraints.length > 0) {
            for (const fk of allFkConstraints) {
              try {
                const qt =
                  dbType === 'mysql'
                    ? (id: string) => `\`${id}\``
                    : (id: string) => `"${id}"`;
                await trx.raw(
                  `ALTER TABLE ${qt(fk.table_name)} DROP CONSTRAINT ${qt(fk.constraint_name)}`,
                );
              } catch (error: any) {}
              try {
                await trx.schema.alterTable(fk.table_name, (table: any) => {
                  table.dropColumn(fk.column_name);
                });
              } catch (error: any) {}
            }
          } else {
          }
        } catch (error: any) {}
        for (const rel of targetRelations) {
          const sourceTable = await trx('table_definition')
            .where({ id: rel.sourceTableId })
            .select('name')
            .first();
          if (sourceTable?.name) affectedTableNames.add(sourceTable.name);
        }
        await trx('relation_definition').where({ targetTableId: id }).delete();
        await trx('table_definition').where({ id }).delete();
        await this.schemaMigrationService.dropTable(
          tableName,
          allRelations,
          trx,
        );
        exists.affectedTables = [...affectedTableNames];
        return exists;
      } catch (error: any) {
        if (trx && !trx.isCompleted()) {
          try {
            await trx.rollback();
          } catch (rollbackError: any) {
            this.logger.error(
              `Failed to rollback transaction: ${rollbackError.message}`,
            );
          }
        }
        this.loggingService.error('Table deletion failed', {
          context: 'delete',
          error: error.message,
          stack: error.stack,
          tableId: id,
        });
        throw new DatabaseException(
          `Failed to delete table: ${error.message}`,
          {
            tableId: id,
            operation: 'delete',
          },
        );
      }
    });
}

}
