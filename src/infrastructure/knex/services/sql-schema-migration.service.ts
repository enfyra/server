import { Logger } from '../../../shared/logger';
import { KnexService } from '../knex.service';
import { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import {
  getForeignKeyColumnName,
  getShortFkName,
  getShortIndexName,
  getShortPkName,
} from '../../knex/utils/sql-schema-naming.util';
import { addColumnToTable } from '../utils/migration/column-operations';
import { dropAllForeignKeysReferencingTable } from '../utils/migration/foreign-key-operations';
import {
  generateSQLFromDiff,
  generateBatchSQL,
  JournalContext,
} from '../utils/migration/sql-diff-generator';
import { SqlSchemaDiffService } from './sql-schema-diff.service';
import { MigrationJournalService } from './migration-journal.service';

export class SqlSchemaMigrationService {
  private readonly logger = new Logger(SqlSchemaMigrationService.name);
  private readonly knexService: KnexService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly migrationJournalService: MigrationJournalService;
  private readonly schemaDiffService: SqlSchemaDiffService;

  constructor(deps: {
    knexService: KnexService;
    metadataCacheService: MetadataCacheService;
    queryBuilderService: QueryBuilderService;
    migrationJournalService: MigrationJournalService;
    sqlSchemaDiffService: SqlSchemaDiffService;
  }) {
    this.knexService = deps.knexService;
    this.metadataCacheService = deps.metadataCacheService;
    this.queryBuilderService = deps.queryBuilderService;
    this.migrationJournalService = deps.migrationJournalService;
    this.schemaDiffService = deps.sqlSchemaDiffService;
  }

  private async getPrimaryKeyType(
    targetTableName: string,
  ): Promise<'uuid' | 'varchar' | 'integer'> {
    try {
      const knex = this.knexService.getKnex();
      const dbType = this.queryBuilderService.getDatabaseType();
      if (dbType === 'postgres') {
        const result = await knex.raw(
          `
          SELECT data_type, udt_name, character_maximum_length
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = ?
            AND column_name = 'id'
        `,
          [targetTableName],
        );
        if (result.rows && result.rows.length > 0) {
          const colType = result.rows[0].data_type?.toLowerCase() || '';
          const udtName = result.rows[0].udt_name?.toLowerCase() || '';
          if (udtName === 'uuid' || colType === 'uuid') {
            return 'uuid';
          }
          if (
            colType === 'character varying' ||
            colType === 'varchar' ||
            colType === 'character'
          ) {
            return 'varchar';
          }
          if (
            colType === 'integer' ||
            colType === 'bigint' ||
            colType === 'serial' ||
            colType === 'bigserial'
          ) {
            return 'integer';
          }
        }
      } else if (dbType === 'mysql') {
        const result = await knex.raw(
          `
          SELECT DATA_TYPE, COLUMN_TYPE
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            AND COLUMN_NAME = 'id'
        `,
          [targetTableName],
        );
        if (result[0] && result[0].length > 0) {
          const colType = result[0][0].DATA_TYPE?.toLowerCase() || '';
          if (colType === 'varchar' || colType === 'char') {
            return 'varchar';
          }
          if (
            colType === 'int' ||
            colType === 'bigint' ||
            colType === 'integer'
          ) {
            return 'integer';
          }
        }
      }
      const targetMetadata =
        await this.metadataCacheService.lookupTableByName(targetTableName);
      if (!targetMetadata) {
        this.logger.warn(
          `Could not find metadata for table ${targetTableName}, defaulting to integer`,
        );
        return 'integer';
      }
      const pkColumn = targetMetadata.columns.find((c) => c.isPrimary);
      if (!pkColumn) {
        this.logger.warn(
          `No primary key found in table ${targetTableName}, defaulting to integer`,
        );
        return 'integer';
      }
      const type = pkColumn.type?.toLowerCase() || '';
      if (type === 'uuid' || type === 'uuidv4' || type.includes('uuid')) {
        return 'uuid';
      }
      if (type === 'string' || type === 'varchar' || type === 'char') {
        return 'varchar';
      }
      return 'integer';
    } catch (error) {
      this.logger.warn(
        `Error getting primary key type for ${targetTableName}: ${error.message}, defaulting to integer`,
      );
      return 'integer';
    }
  }

  private isDeadlockError(error: any): boolean {
    const errorCode = error?.code || error?.errno || '';
    const errorMessage = error?.message || '';
    const sqlState = error?.sqlState || '';
    return (
      errorCode === '40P01' ||
      errorCode === '40001' ||
      sqlState === '40P01' ||
      sqlState === '40001' ||
      errorCode === '1213' ||
      errorCode === '1205' ||
      errorCode === 1213 ||
      errorCode === 1205 ||
      errorMessage.toLowerCase().includes('deadlock') ||
      errorMessage.toLowerCase().includes('lock wait timeout') ||
      errorMessage.toLowerCase().includes('try restarting transaction')
    );
  }

  private async setLockTimeout(
    knex: any,
    dbType: string,
    timeoutSeconds: number = 5,
  ): Promise<void> {
    if (dbType === 'postgres') {
      await knex.raw(`SET LOCAL lock_timeout = '${timeoutSeconds}s'`);
    } else if (dbType === 'mysql') {
      await knex.raw(
        `SET SESSION innodb_lock_wait_timeout = ${timeoutSeconds}`,
      );
    }
  }

  private createFKColumn(
    table: any,
    columnName: string,
    pkType: 'uuid' | 'varchar' | 'integer',
    dbType: string,
  ): any {
    if (pkType === 'uuid') {
      if (dbType === 'postgres') {
        return table.uuid(columnName);
      } else {
        return table.string(columnName, 36);
      }
    } else if (pkType === 'varchar') {
      return table.string(columnName, 36);
    } else {
      return table.integer(columnName).unsigned();
    }
  }

  private applyNullability(
    column: any,
    isNullable: boolean | number | undefined,
  ): any {
    if (isNullable === false || isNullable === 0) {
      return column.notNullable();
    } else {
      return column.nullable();
    }
  }

  async createTable(
    tableMetadata: any,
  ): Promise<{ autoGeneratedIndexes: string[][] }> {
    const knex = this.knexService.getKnex();
    const tableName = tableMetadata.name;
    if (await knex.schema.hasTable(tableName)) {
      this.logger.warn(`Table ${tableName} already exists, skipping creation`);
      return;
    }

    const autoGeneratedIndexes: string[][] = [];

    try {
      const targetPkTypes = new Map<string, 'uuid' | 'varchar' | 'integer'>();
      if (tableMetadata.relations) {
        for (const rel of tableMetadata.relations) {
          if (['many-to-one', 'one-to-one'].includes(rel.type)) {
            const targetTableName = rel.targetTableName || rel.targetTable;
            if (targetTableName && !targetPkTypes.has(targetTableName)) {
              targetPkTypes.set(
                targetTableName,
                await this.getPrimaryKeyType(targetTableName),
              );
            }
          }
        }
      }
      await knex.schema.createTable(tableName, (table) => {
        for (const col of tableMetadata.columns || []) {
          addColumnToTable(table, col);
        }
        if (tableMetadata.relations) {
          for (const rel of tableMetadata.relations) {
            if (!['many-to-one', 'one-to-one'].includes(rel.type)) {
              continue;
            }
            const targetTableName = rel.targetTableName || rel.targetTable;
            if (!targetTableName) {
              throw new Error(
                `Relation '${rel.propertyName}' must have targetTableName or targetTable`,
              );
            }
            const systemTables = [
              'table_definition',
              'column_definition',
              'relation_definition',
              'route_definition',
            ];
            if (systemTables.includes(targetTableName)) {
              throw new Error(
                `Relation '${rel.propertyName}' (${rel.type}) from table '${tableMetadata.name}' cannot target system table '${targetTableName}'. This indicates an invalid targetTableId: ${rel.targetTableId}. Please verify the target table ID is correct.`,
              );
            }
            const fkColumn = getForeignKeyColumnName(rel.propertyName);
            const targetPkType =
              targetPkTypes.get(targetTableName) || 'integer';
            const dbType = this.queryBuilderService.getDatabaseType();
            const fkCol = this.createFKColumn(
              table,
              fkColumn,
              targetPkType,
              dbType,
            );
            this.applyNullability(fkCol, rel.isNullable);
            table.index([fkColumn, 'id'], `idx_${tableName}_${fkColumn}`);
            autoGeneratedIndexes.push([rel.propertyName]);
          }
        }
        table.timestamp('createdAt').defaultTo(knex.fn.now());
        table.timestamp('updatedAt').defaultTo(knex.fn.now());

        const relationFkMap = new Map<string, string>();
        for (const rel of tableMetadata.relations || []) {
          if (['many-to-one', 'one-to-one'].includes(rel.type)) {
            const fkCol =
              rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
            relationFkMap.set(rel.propertyName, fkCol);
          }
        }
        const translateToColumnName = (colName: string): string => {
          return relationFkMap.get(colName) || colName;
        };

        if (tableMetadata.uniques?.length > 0) {
          for (const uniqueGroup of tableMetadata.uniques) {
            const translatedGroup = uniqueGroup.map(translateToColumnName);
            table.unique(translatedGroup);
          }
        }
        if (tableMetadata.indexes?.length > 0) {
          for (const indexGroup of tableMetadata.indexes) {
            const translatedGroup = indexGroup.map(translateToColumnName);
            const physicalCols = translatedGroup.includes('id')
              ? translatedGroup
              : [...translatedGroup, 'id'];
            table.index(
              physicalCols,
              `idx_${tableName}_${translatedGroup.join('_')}`,
            );
          }
        }

        table.index(['createdAt', 'id'], `idx_${tableName}_createdAt`);
        table.index(['updatedAt', 'id'], `idx_${tableName}_updatedAt`);
        autoGeneratedIndexes.push(['createdAt']);
        autoGeneratedIndexes.push(['updatedAt']);

        const timestampFields = (tableMetadata.columns || []).filter(
          (col: any) =>
            (col.type === 'datetime' ||
              col.type === 'timestamp' ||
              col.type === 'date') &&
            !['createdAt', 'updatedAt'].includes(col.name),
        );
        for (const field of timestampFields) {
          table.index([field.name, 'id'], `idx_${tableName}_${field.name}`);
          autoGeneratedIndexes.push([field.name]);
        }
      });
      for (const rel of tableMetadata.relations || []) {
        if (!['many-to-one', 'one-to-one'].includes(rel.type)) {
          continue;
        }
        const targetTable = rel.targetTableName || rel.targetTable;
        if (!targetTable) {
          this.logger.warn(
            `Skipping FK constraint for relation ${rel.propertyName}: missing targetTableName`,
          );
          continue;
        }
        if (typeof targetTable !== 'string' || targetTable.trim() === '') {
          this.logger.error(
            `Invalid targetTableName for relation ${rel.propertyName}: ${targetTable}. Skipping FK constraint.`,
          );
          continue;
        }
        const fkColumn = getForeignKeyColumnName(rel.propertyName);
        const targetTableExists = await knex.schema.hasTable(targetTable);
        if (!targetTableExists) {
          this.logger.error(
            `Skipping FK constraint ${fkColumn} -> ${targetTable}: target table does not exist. This may indicate invalid relation metadata.`,
          );
          continue;
        }
        const maxRetries = 3;
        try {
          let lastError: any = null;
          for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
              if (attempt > 0) {
                const backoffMs = Math.min(
                  1000 * Math.pow(2, attempt - 1),
                  5000,
                );
                await new Promise((resolve) => setTimeout(resolve, backoffMs));
              }
              const dbType = this.queryBuilderService.getDatabaseType();
              await this.setLockTimeout(knex, dbType, 5);
              const timeoutPromise = new Promise((_, reject) => {
                setTimeout(
                  () =>
                    reject(
                      new Error(
                        'FK constraint creation timeout after 10 seconds',
                      ),
                    ),
                  10000,
                );
              });
              const createFkPromise = knex.schema.alterTable(
                tableName,
                (table) => {
                  const onDelete =
                    rel.isNullable === false ? 'RESTRICT' : 'SET NULL';
                  table
                    .foreign(fkColumn)
                    .references('id')
                    .inTable(targetTable)
                    .onDelete(onDelete)
                    .onUpdate('CASCADE');
                },
              );
              await Promise.race([createFkPromise, timeoutPromise]);
              lastError = null;
              break;
            } catch (attemptError: any) {
              lastError = attemptError;
              if (
                this.isDeadlockError(attemptError) &&
                attempt < maxRetries - 1
              ) {
                this.logger.warn(
                  `Deadlock detected on FK constraint ${fkColumn} -> ${targetTable} (attempt ${attempt + 1}/${maxRetries}). Retrying...`,
                );
                continue;
              }
              if (attempt === maxRetries - 1) {
                throw attemptError;
              }
            }
          }
          if (lastError) {
            throw lastError;
          }
        } catch (error: any) {
          const errorMessage = error?.message || '';
          if (this.isDeadlockError(error)) {
            this.logger.error(
              `Deadlock occurred while creating FK constraint ${fkColumn} -> ${targetTable} after ${maxRetries} attempts: ${errorMessage}`,
            );
          } else {
            this.logger.error(
              `Failed to add FK constraint ${fkColumn} -> ${targetTable}: ${errorMessage}`,
            );
          }
          throw error;
        }
      }
      for (const rel of tableMetadata.relations || []) {
        if (rel.type === 'one-to-many') {
          if (!rel.mappedBy) {
            throw new Error(
              `One-to-many relation '${rel.propertyName}' in table '${tableName}' MUST have mappedBy`,
            );
          }
          const targetTable = rel.targetTableName || rel.targetTable;
          if (!targetTable) {
            throw new Error(
              `One-to-many relation '${rel.propertyName}' in table '${tableName}' MUST have targetTableName or targetTable`,
            );
          }
          const sourceTable = tableName;
          const fkColumn = getForeignKeyColumnName(rel.mappedBy);
          const sourcePkType = await this.getPrimaryKeyType(sourceTable);
          const dbType = this.queryBuilderService.getDatabaseType();
          const maxRetries = 3;
          let lastError: any = null;
          for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
              if (attempt > 0) {
                const backoffMs = Math.min(
                  1000 * Math.pow(2, attempt - 1),
                  5000,
                );
                await new Promise((resolve) => setTimeout(resolve, backoffMs));
              }
              await this.setLockTimeout(knex, dbType, 5);
              await knex.schema.alterTable(targetTable, (table) => {
                const fkCol = this.createFKColumn(
                  table,
                  fkColumn,
                  sourcePkType,
                  dbType,
                );
                this.applyNullability(fkCol, rel.isNullable);
                table.index([fkColumn, 'id'], `idx_${targetTable}_${fkColumn}`);
                const onDelete =
                  rel.isNullable === false || rel.isNullable === 0
                    ? 'RESTRICT'
                    : 'SET NULL';
                table
                  .foreign(fkColumn)
                  .references('id')
                  .inTable(sourceTable)
                  .onDelete(onDelete)
                  .onUpdate('CASCADE');
              });
              lastError = null;
              break;
            } catch (attemptError: any) {
              lastError = attemptError;
              if (
                this.isDeadlockError(attemptError) &&
                attempt < maxRetries - 1
              ) {
                this.logger.warn(
                  `Deadlock detected on O2M FK ${fkColumn} in ${targetTable} (attempt ${attempt + 1}/${maxRetries}). Retrying...`,
                );
                continue;
              }
              if (attempt === maxRetries - 1) {
                const errorMessage = attemptError?.message || '';
                if (this.isDeadlockError(attemptError)) {
                  this.logger.error(
                    `Deadlock occurred while creating O2M FK ${fkColumn} in ${targetTable} after ${maxRetries} attempts: ${errorMessage}`,
                  );
                } else {
                  this.logger.error(
                    `Failed to add O2M FK column ${fkColumn} to ${targetTable}: ${errorMessage}`,
                  );
                }
                throw attemptError;
              }
            }
          }
          if (lastError) {
            throw lastError;
          }
        }
      }
      for (const rel of tableMetadata.relations || []) {
        if (rel.type === 'many-to-many') {
          if (!rel.junctionTableName) {
            this.logger.warn(
              `M2M relation '${rel.propertyName}' missing junctionTableName, skipping junction table creation`,
            );
            continue;
          }
          if (!rel.junctionSourceColumn || !rel.junctionTargetColumn) {
            this.logger.warn(
              `M2M relation '${rel.propertyName}' missing junction column names, skipping`,
            );
            continue;
          }
          const junctionTableName = rel.junctionTableName;
          const junctionSourceColumn = rel.junctionSourceColumn;
          const junctionTargetColumn = rel.junctionTargetColumn;
          const sourceTable = tableName;
          const targetTable = rel.targetTableName || rel.targetTable;
          const junctionExists = await knex.schema.hasTable(junctionTableName);
          if (junctionExists) {
            continue;
          }
          const sourcePkType = await this.getPrimaryKeyType(sourceTable);
          const targetPkType = await this.getPrimaryKeyType(targetTable);
          const dbType = this.queryBuilderService.getDatabaseType();
          try {
            await knex.schema.createTable(junctionTableName, (table) => {
              const sourceCol = this.createFKColumn(
                table,
                junctionSourceColumn,
                sourcePkType,
                dbType,
              );
              sourceCol.notNullable();
              const targetCol = this.createFKColumn(
                table,
                junctionTargetColumn,
                targetPkType,
                dbType,
              );
              targetCol.notNullable();

              const pkName = getShortPkName(junctionTableName);
              table.primary(
                [junctionSourceColumn, junctionTargetColumn],
                pkName,
              );

              const sourceFkName = getShortFkName(
                tableName,
                rel.propertyName,
                'src',
              );
              table
                .foreign(junctionSourceColumn)
                .references('id')
                .inTable(sourceTable)
                .onDelete('CASCADE')
                .onUpdate('CASCADE')
                .withKeyName(sourceFkName);

              const targetFkName = getShortFkName(
                tableName,
                rel.propertyName,
                'tgt',
              );
              table
                .foreign(junctionTargetColumn)
                .references('id')
                .inTable(targetTable)
                .onDelete('CASCADE')
                .onUpdate('CASCADE')
                .withKeyName(targetFkName);

              const sourceIndexName = getShortIndexName(
                tableName,
                rel.propertyName,
                'src',
              );
              const targetIndexName = getShortIndexName(
                tableName,
                rel.propertyName,
                'tgt',
              );
              const reverseIndexName = getShortIndexName(
                tableName,
                rel.propertyName,
                'rev',
              );

              table.index([junctionSourceColumn], sourceIndexName);
              table.index([junctionTargetColumn], targetIndexName);
              table.index(
                [junctionTargetColumn, junctionSourceColumn],
                reverseIndexName,
              );
            });
          } catch (error) {
            this.logger.error(
              `Failed to create junction table ${junctionTableName}: ${error.message}`,
            );
            throw error;
          }
        }
      }
    } catch (error) {
      this.logger.error(
        `Failed to create table ${tableName}: ${error.message}`,
      );
      throw error;
    }

    return { autoGeneratedIndexes };
  }

  async updateMetadataIndexesForTable(
    tableName: string,
    userDefinedIndexes: string[][],
    autoGeneratedIndexes: string[][],
  ): Promise<void> {
    await this.schemaDiffService.updateMetadataIndexes(
      tableName,
      userDefinedIndexes,
      autoGeneratedIndexes,
    );
  }

  async updateTable(
    tableName: string,
    oldMetadata: any,
    newMetadata: any,
    trx?: any,
  ): Promise<{
    pendingMetadataUpdate?: { tableName: string; diff: any };
    journalUuid?: string;
  }> {
    const knex = this.knexService.getKnex();
    if (!(await knex.schema.hasTable(tableName))) {
      this.logger.warn(`Table ${tableName} does not exist, creating...`);
      await this.createTable(newMetadata);
      return {};
    }
    const schemaDiff = await this.schemaDiffService.generateSchemaDiff(
      oldMetadata,
      newMetadata,
    );
    const dbType = this.queryBuilderService.getDatabaseType();

    const upStatements = await generateSQLFromDiff(
      knex,
      tableName,
      schemaDiff,
      dbType as 'mysql' | 'postgres' | 'sqlite',
      this.metadataCacheService,
    );
    const upScript = generateBatchSQL(upStatements);

    const reverseDiff = await this.schemaDiffService.generateSchemaDiff(
      newMetadata,
      oldMetadata,
    );
    const downStatements = await generateSQLFromDiff(
      knex,
      tableName,
      reverseDiff,
      dbType as 'mysql' | 'postgres' | 'sqlite',
      this.metadataCacheService,
    );
    const downScript = generateBatchSQL(downStatements);

    let journalUuid: string | undefined;
    try {
      journalUuid = await this.migrationJournalService.record({
        tableName,
        operation: 'update',
        upScript,
        downScript,
        beforeSnapshot: oldMetadata,
      });
      await this.migrationJournalService.markRunning(journalUuid);
    } catch (journalErr: any) {
      this.logger.warn(
        `Journal record failed (non-fatal): ${journalErr.message}`,
      );
    }

    const journalContext: JournalContext | undefined = journalUuid
      ? {
          uuid: journalUuid,
          markFailed: (err) =>
            this.migrationJournalService.markFailed(journalUuid!, err),
          executeRollback: (uuid) => this.migrationJournalService.executeRollback(uuid),
        }
      : undefined;

    try {
      await this.schemaDiffService.executeSchemaDiff(
        tableName,
        schemaDiff,
        trx,
        journalContext,
      );
      await this.compareMetadataWithActualSchema(tableName, newMetadata);

      return {
        pendingMetadataUpdate: { tableName, diff: schemaDiff },
        journalUuid,
      };
    } catch (error) {
      if (journalUuid) {
        await this.migrationJournalService
          .markFailed(journalUuid, error.message || 'Unknown error')
          .catch(() => {});
      }
      throw error;
    }
  }

  async applyPendingMetadataUpdate(
    pending: { tableName: string; diff: any } | undefined,
    trx?: any,
  ): Promise<void> {
    if (!pending) return;
    await this.updateMetadataFields(pending.tableName, pending.diff, trx);
  }

  async markJournalCompleted(uuid: string): Promise<void> {
    await this.migrationJournalService.markCompleted(uuid);
  }

  async rollbackJournal(uuid: string): Promise<void> {
    await this.migrationJournalService.executeRollback(uuid);
  }

  private async updateMetadataFields(
    tableName: string,
    diff: any,
    trx?: any,
  ): Promise<void> {
    if (!diff.metadataUpdate) return;

    const query = trx || this.knexService.getKnex();
    const updateData: any = {};

    if (diff.metadataUpdate.uniques !== undefined) {
      updateData.uniques = JSON.stringify(diff.metadataUpdate.uniques);
    }

    if (diff.metadataUpdate.indexes !== undefined) {
      updateData.indexes = JSON.stringify(diff.metadataUpdate.indexes);
    }

    if (Object.keys(updateData).length > 0) {
      try {
        await query('table_definition')
          .where('name', tableName)
          .update(updateData);
      } catch (error) {
        this.logger.error(
          `  Failed to update metadata fields for ${tableName}: ${error.message}`,
        );
      }
    }
  }

  async compareMetadataWithActualSchema(
    tableName: string,
    metadata: any,
  ): Promise<void> {
    try {
      const cachedMetadata =
        await this.metadataCacheService.lookupTableByName(tableName);
      if (!cachedMetadata) {
        return;
      }
      const inputColNames = new Set(
        metadata.columns?.map((c: any) => c.name) || [],
      );
      const cachedColNames = new Set(
        cachedMetadata.columns?.map((c: any) => c.name) || [],
      );
      const missingInCache = [...inputColNames].filter(
        (name) => !cachedColNames.has(name),
      );
      const extraInCache = [...cachedColNames].filter(
        (name) => !inputColNames.has(name),
      );
      if (missingInCache.length > 0) {
        this.logger.warn(
          `  Columns in input but not in cache: ${missingInCache.join(', ')}`,
        );
      }
      if (extraInCache.length > 0) {
        this.logger.warn(
          `  Columns in cache but not in input: ${extraInCache.join(', ')}`,
        );
      }
    } catch (error: any) {
      this.logger.error(
        `Failed to compare schema for ${tableName}: ${error?.message || error}`,
      );
    }
  }

  async dropColumnDirectly(
    tableName: string,
    columnName: string,
  ): Promise<void> {
    const knex = this.knexService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType() as
      | 'mysql'
      | 'postgres'
      | 'sqlite';
    if (!(await knex.schema.hasTable(tableName))) {
      throw new Error(`Table ${tableName} does not exist`);
    }
    const hasColumn = await knex.schema.hasColumn(tableName, columnName);
    if (!hasColumn) {
      this.logger.warn(
        `Column ${tableName}.${columnName} does not exist, skipping drop`,
      );
      return;
    }
    try {
      if (dbType === 'mysql') {
        await knex.raw(`ALTER TABLE ?? DROP COLUMN ??`, [
          tableName,
          columnName,
        ]);
      } else if (dbType === 'postgres') {
        await knex.raw(`ALTER TABLE ?? DROP COLUMN ??`, [
          tableName,
          columnName,
        ]);
      } else {
        await knex.schema.table(tableName, (table) => {
          table.dropColumn(columnName);
        });
      }
      await this.removeColumnFromMetadataIndexes(tableName, columnName);
    } catch (error) {
      this.logger.error(
        `Failed to drop column ${tableName}.${columnName}:`,
        error.message,
      );
      throw error;
    }
  }

  private async removeColumnFromMetadataIndexes(
    tableName: string,
    columnName: string,
  ): Promise<void> {
    const knex = this.knexService.getKnex();
    try {
      const row = await knex('table_definition')
        .where('name', tableName)
        .first();
      if (!row?.indexes) return;
      const indexes: string[][] = JSON.parse(row.indexes);
      const cleaned = indexes.filter((group) => !group.includes(columnName));
      if (cleaned.length === indexes.length) return;
      await knex('table_definition')
        .where('name', tableName)
        .update({ indexes: JSON.stringify(cleaned) });
    } catch (error) {
      this.logger.warn(
        `  Failed to clean up metadata indexes for ${tableName}.${columnName}: ${error.message}`,
      );
    }
  }

  async dropTable(
    tableName: string,
    relations?: any[],
    trx?: any,
  ): Promise<void> {
    const db = trx || this.knexService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType() as
      | 'mysql'
      | 'postgres'
      | 'sqlite';
    const qt = (id: string) => {
      if (dbType === 'mysql') return `\`${id}\``;
      return `"${id}"`;
    };
    let tableExists = false;
    try {
      if (dbType === 'postgres') {
        const result = await db.raw(
          `SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?)`,
          [tableName],
        );
        tableExists = result.rows[0]?.exists || false;
      } else if (dbType === 'mysql') {
        const result = await db.raw(
          `SELECT COUNT(*) as count FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?`,
          [tableName],
        );
        tableExists = result[0][0]?.count > 0;
      } else {
        const result = await db.raw(
          `SELECT name FROM sqlite_master WHERE type='table' AND name=?`,
          [tableName],
        );
        tableExists = result.length > 0;
      }
    } catch (error) {
      this.logger.error(`Error checking table existence: ${error.message}`);
      tableExists = false;
    }
    if (!tableExists) {
      this.logger.warn(`Table ${tableName} does not exist, skipping drop`);
      return;
    }
    let relationsToCheck = relations;
    if (!relationsToCheck) {
      const metadata =
        await this.metadataCacheService.lookupTableByName(tableName);
      if (metadata && metadata.relations) {
        relationsToCheck = metadata.relations;
      }
    }
    if (relationsToCheck && relationsToCheck.length > 0) {
      const m2mRelations = relationsToCheck.filter(
        (rel: any) => rel.type === 'many-to-many',
      );
      for (const rel of m2mRelations) {
        if (rel.junctionTableName) {
          try {
            await db.raw(`DROP TABLE IF EXISTS ${qt(rel.junctionTableName)}`);
          } catch (error) {
            this.logger.error(
              `Failed to drop junction table ${rel.junctionTableName}: ${error.message}`,
            );
          }
        }
      }
    }
    if (!trx) {
      await dropAllForeignKeysReferencingTable(db, tableName, dbType);
    }
    try {
      await db.raw(`DROP TABLE IF EXISTS ${qt(tableName)}`);
    } catch (error) {
      this.logger.error(`Failed to drop table ${tableName}: ${error.message}`);
      throw error;
    }
  }
}
