import { getIoAbortSignal } from '@enfyra/kernel';
import type { Knex } from 'knex';
import {
  DatabaseException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../domain/exceptions';
import { isPolicyDeny, isPolicyPreview } from '../../../domain/policy';
import { generateDropForeignKeySQL } from '../../../engines/knex/utils/migration/sql-dialect';
import type { TDynamicContext } from '../../../shared/types';
import { SqlTableHandlerService } from './sql-table-handler-base.service';

export class SqlTableDeleteService extends SqlTableHandlerService {
  async delete(id: string | number, context?: TDynamicContext) {
    return await this.runWithSchemaLock(
      `table:delete:${id}`,
      () => this.deleteTableInternal(id, context),
      (context as any)?.$onLockAcquired,
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
        const exists = await trx('enfyra_table').where({ id }).first();
        if (!exists) {
          throw new ResourceNotFoundException('enfyra_table', String(id));
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
        if (isPolicyPreview(decision)) {
          return { _preview: true, ...decision.details };
        }
        const allRelations = await trx('enfyra_relation')
          .where({ sourceTableId: id })
          .orWhere({ targetTableId: id })
          .select('*');
        await this.removeReferencingForeignKeys(trx, tableName);
        for (const rel of allRelations.filter(
          (relation) => String(relation.targetTableId) === String(id),
        )) {
          const sourceTable = await trx('enfyra_table')
            .where({ id: rel.sourceTableId })
            .select('name')
            .first();
          if (sourceTable?.name) affectedTableNames.add(sourceTable.name);
        }
        await this.schemaMigrationService.dropTable(
          tableName,
          allRelations,
          trx,
        );
        await trx('enfyra_relation').where({ targetTableId: id }).delete();
        await trx('enfyra_table').where({ id }).delete();
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

  private async removeReferencingForeignKeys(
    trx: Knex.Transaction,
    tableName: string,
  ): Promise<void> {
    const dbType = this.queryBuilderService.getDatabaseType() as
      | 'mysql'
      | 'postgres';
    const result = await trx.raw(
      dbType === 'postgres'
        ? `
          SELECT rel.relname AS table_name,
                 con.conname AS constraint_name,
                 att.attname AS column_name
          FROM pg_constraint con
          INNER JOIN pg_class rel ON rel.oid = con.conrelid
          INNER JOIN pg_class referenced ON referenced.oid = con.confrelid
          INNER JOIN pg_attribute att
            ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
          WHERE referenced.relname = ? AND con.contype = 'f'
        `
        : `
          SELECT TABLE_NAME AS table_name,
                 CONSTRAINT_NAME AS constraint_name,
                 COLUMN_NAME AS column_name
          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
          WHERE TABLE_SCHEMA = DATABASE()
            AND REFERENCED_TABLE_NAME = ?
            AND REFERENCED_COLUMN_NAME IS NOT NULL
        `,
      [tableName],
    );
    const foreignKeys = dbType === 'postgres' ? result.rows : result[0];
    const columnsByConstraint = new Map<
      string,
      { tableName: string; constraintName: string; columns: string[] }
    >();
    for (const foreignKey of foreignKeys ?? []) {
      const key = `${foreignKey.table_name}:${foreignKey.constraint_name}`;
      const current = columnsByConstraint.get(key);
      if (current) {
        current.columns.push(foreignKey.column_name);
        continue;
      }
      columnsByConstraint.set(key, {
        tableName: foreignKey.table_name,
        constraintName: foreignKey.constraint_name,
        columns: [foreignKey.column_name],
      });
    }
    for (const foreignKey of columnsByConstraint.values()) {
      await trx.raw(
        generateDropForeignKeySQL(
          foreignKey.tableName,
          foreignKey.constraintName,
          dbType,
        ),
      );
      await trx.schema.alterTable(foreignKey.tableName, (table: any) => {
        for (const columnName of foreignKey.columns) {
          table.dropColumn(columnName);
        }
      });
    }
  }
}
