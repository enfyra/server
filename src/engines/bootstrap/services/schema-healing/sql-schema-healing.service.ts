import {
  QueryBuilderService,
  getForeignKeyColumnName,
  getShortFkConstraintName,
} from '@enfyra/kernel';
import type { Knex } from 'knex';
import { MetadataCacheService } from '../../../cache';
import { buildSqlJunctionTableContract } from '../../../knex/utils/sql-physical-schema-contract';
import { hasSqlValuesOutsideEnumOptions } from '../../../knex/utils/sql-enum.util';
import { getCurrentDatabaseSchema } from '../../../knex/utils/provision/schema-comparison';
import { applySqlColumnModifications } from '../../../../shared/utils/provision-schema-migration';
import type { ColumnModifyDef } from '../../../../shared/types/schema-migration.types';
import { normalizeEnumOptionsValue } from '../../../../shared/utils/json-field-normalizer.util';
import type { SchemaHealingSnapshot } from '../../types/schema-healing.types';
import {
  diffJunctionMetadata,
  getTargetJunctionContract,
} from '../../utils/schema-healing-junction.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';

export class SqlSchemaHealingService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly log: (message: string) => void;
  private readonly warn: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
    systemCoreTableResolver: SystemCoreTableResolver;
    log: (message: string) => void;
    warn: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.log = deps.log;
    this.warn = deps.warn;
  }

  async repairSqlMetadataEnumColumns(): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (!(await knex.schema.hasTable(coreNames.table))) return 0;
    if (!(await knex.schema.hasTable(coreNames.column))) return 0;

    const enumColumns = await knex(`${coreNames.column} as column_def`)
      .join(
        `${coreNames.table} as table_def`,
        'table_def.id',
        'column_def.tableId',
      )
      .where('column_def.type', 'enum')
      .select({
        tableName: 'table_def.name',
        columnName: 'column_def.name',
        options: 'column_def.options',
        isNullable: 'column_def.isNullable',
        defaultValue: 'column_def.defaultValue',
      });
    const repairs: Array<{
      tableName: string;
      modification: ColumnModifyDef;
    }> = [];

    for (const metadata of enumColumns) {
      const tableName = String(metadata.tableName ?? '');
      const columnName = String(metadata.columnName ?? '');
      const options = normalizeEnumOptionsValue(metadata.options);
      if (
        tableName &&
        columnName &&
        (!Array.isArray(options) ||
          options.length === 0 ||
          options.some((option) => typeof option !== 'string') ||
          new Set(options).size !== options.length)
      ) {
        this.warn(
          `${tableName}.${columnName} enum healing skipped: metadata options are missing or invalid`,
        );
        continue;
      }
      if (
        !tableName ||
        !columnName ||
        !Array.isArray(options) ||
        !(await knex.schema.hasTable(tableName)) ||
        !(await knex.schema.hasColumn(tableName, columnName))
      ) {
        continue;
      }

      const current = await getCurrentDatabaseSchema(knex, tableName);
      const physical = current.columns.find(
        (column) => column.name === columnName,
      );
      if (!physical) continue;
      if (
        physical.type === 'enum' &&
        JSON.stringify(physical.enumValues ?? []) === JSON.stringify(options)
      ) {
        continue;
      }

      if (
        await hasSqlValuesOutsideEnumOptions(
          knex,
          tableName,
          columnName,
          options,
        )
      ) {
        throw new Error(
          `Cannot heal ${tableName}.${columnName} enum: unsupported persisted values`,
        );
      }

      repairs.push({
        tableName,
        modification: {
          from: {
            name: columnName,
            type: physical.type,
            options: physical.enumValues ?? null,
            isNullable: physical.isNullable,
            defaultValue: physical.defaultValue,
          },
          to: {
            name: columnName,
            type: 'enum',
            options,
            isNullable: this.toBoolean(metadata.isNullable),
            defaultValue: this.parseStoredJson(metadata.defaultValue),
          },
        },
      });
    }

    for (const repair of repairs) {
      await applySqlColumnModifications(
        knex,
        repair.tableName,
        [repair.modification],
        String(knex.client.config.client),
      );
    }

    return repairs.length;
  }

  private parseStoredJson(value: unknown): unknown {
    if (typeof value !== 'string') return value;
    try {
      return JSON.parse(value);
    } catch {
      return value;
    }
  }

  private toBoolean(value: unknown): boolean {
    return value === true || value === 1 || value === '1';
  }

  async repairSqlRelationPhysicalMappings(): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    const coreNames = await this.systemCoreTableResolver.getNames();
    const rows = await knex(`${coreNames.relation} as r`)
      .leftJoin(
        `${coreNames.table} as sourceTable`,
        'r.sourceTableId',
        'sourceTable.id',
      )
      .select('r.*', 'sourceTable.name as sourceTableName');
    let repaired = 0;

    for (const rel of rows) {
      if (!this.isSqlOwningRelation(rel)) continue;

      const foreignKeyColumn =
        rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      const referencedColumn = rel.referencedColumn || 'id';
      const constraintName =
        rel.constraintName ||
        (await this.findSqlForeignKeyConstraintName(
          knex,
          rel.sourceTableName,
          foreignKeyColumn,
        )) ||
        getShortFkConstraintName(rel.sourceTableName, foreignKeyColumn, 'src');
      const updateData: any = {};

      if (!rel.foreignKeyColumn) updateData.foreignKeyColumn = foreignKeyColumn;
      if (!rel.referencedColumn) updateData.referencedColumn = referencedColumn;
      if (!rel.constraintName) updateData.constraintName = constraintName;
      if (Object.keys(updateData).length === 0) continue;

      await knex(coreNames.relation).where({ id: rel.id }).update(updateData);
      repaired++;
    }

    return repaired;
  }

  async healSqlJunctionContracts(
    snapshot: SchemaHealingSnapshot,
  ): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    const coreNames = await this.systemCoreTableResolver.getNames();
    const rows = await knex(`${coreNames.relation} as r`)
      .leftJoin(
        `${coreNames.table} as sourceTable`,
        'r.sourceTableId',
        'sourceTable.id',
      )
      .leftJoin(
        `${coreNames.table} as targetTable`,
        'r.targetTableId',
        'targetTable.id',
      )
      .select(
        'r.*',
        'sourceTable.name as sourceTableName',
        'targetTable.name as targetTableName',
      );
    const byMappedById = new Map<string, any[]>();
    for (const rel of rows) {
      if (!rel.mappedById) continue;
      const key = String(rel.mappedById);
      const list = byMappedById.get(key) || [];
      list.push(rel);
      byMappedById.set(key, list);
    }

    let repaired = 0;
    for (const rel of rows) {
      if (rel.type !== 'many-to-many' || rel.mappedById) continue;
      if (!rel.sourceTableName || !rel.targetTableName || !rel.propertyName) {
        continue;
      }

      const target = getTargetJunctionContract(snapshot, {
        sourceTable: rel.sourceTableName,
        propertyName: rel.propertyName,
        targetTable: rel.targetTableName,
      });
      const oldJunctionTableName = rel.junctionTableName || null;
      await this.ensureSqlJunctionPhysicalTable(knex, {
        oldJunctionTableName,
        oldJunctionSourceColumn: rel.junctionSourceColumn || null,
        oldJunctionTargetColumn: rel.junctionTargetColumn || null,
        sourceTable: rel.sourceTableName,
        targetTable: rel.targetTableName,
        sourcePropertyName: rel.propertyName,
        junctionTableName: target.junctionTableName,
        junctionSourceColumn: target.junctionSourceColumn,
        junctionTargetColumn: target.junctionTargetColumn,
      });

      const owningUpdate = diffJunctionMetadata(rel, target);
      if (Object.keys(owningUpdate).length > 0) {
        await knex(coreNames.relation)
          .where({ id: rel.id })
          .update(owningUpdate);
        repaired++;
      }

      for (const inverseRel of byMappedById.get(String(rel.id)) || []) {
        const inverseStandard = {
          junctionTableName: target.junctionTableName,
          junctionSourceColumn: target.junctionTargetColumn,
          junctionTargetColumn: target.junctionSourceColumn,
        };
        const inverseUpdate = diffJunctionMetadata(inverseRel, inverseStandard);
        if (Object.keys(inverseUpdate).length === 0) continue;
        await knex(coreNames.relation)
          .where({ id: inverseRel.id })
          .update(inverseUpdate);
        repaired++;
      }
    }

    return repaired;
  }

  private async ensureSqlJunctionPhysicalTable(
    knex: Knex,
    input: {
      oldJunctionTableName: string | null;
      oldJunctionSourceColumn: string | null;
      oldJunctionTargetColumn: string | null;
      sourceTable: string;
      targetTable: string;
      sourcePropertyName: string;
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
    },
  ): Promise<void> {
    const legacyCandidates = this.getSqlLegacyJunctionCandidates(input);
    let standardExists = await knex.schema.hasTable(input.junctionTableName);
    let renamedLegacyTableName: string | null = null;

    if (!standardExists) {
      for (const candidate of legacyCandidates) {
        if (!(await knex.schema.hasTable(candidate.tableName))) continue;

        await knex.schema.renameTable(
          candidate.tableName,
          input.junctionTableName,
        );
        this.log(
          `Renamed junction table '${candidate.tableName}' to '${input.junctionTableName}'`,
        );
        await this.ensureSqlJunctionColumns(knex, {
          junctionTableName: input.junctionTableName,
          junctionSourceColumn: input.junctionSourceColumn,
          junctionTargetColumn: input.junctionTargetColumn,
          oldJunctionSourceColumn: candidate.sourceColumn,
          oldJunctionTargetColumn: candidate.targetColumn,
        });
        standardExists = true;
        renamedLegacyTableName = candidate.tableName;
        break;
      }
    }

    if (standardExists) {
      if (!renamedLegacyTableName) {
        await this.ensureSqlJunctionColumns(knex, input);
      }
      for (const candidate of legacyCandidates) {
        if (candidate.tableName === renamedLegacyTableName) continue;
        if (!(await knex.schema.hasTable(candidate.tableName))) continue;
        await this.mergeAndDropSqlLegacyJunctionTable(knex, {
          ...input,
          legacyTableName: candidate.tableName,
          legacySourceColumn: candidate.sourceColumn,
          legacyTargetColumn: candidate.targetColumn,
        });
      }
      return;
    }

    const sourceExists = await knex.schema.hasTable(input.sourceTable);
    const targetExists = await knex.schema.hasTable(input.targetTable);
    if (!sourceExists || !targetExists) return;

    const junction = buildSqlJunctionTableContract({
      tableName: input.junctionTableName,
      sourceTable: input.sourceTable,
      targetTable: input.targetTable,
      sourceColumn: input.junctionSourceColumn,
      targetColumn: input.junctionTargetColumn,
      sourcePropertyName: input.sourcePropertyName,
    });
    const sourcePkType = await this.getSqlPrimaryKeyType(input.sourceTable);
    const targetPkType = await this.getSqlPrimaryKeyType(input.targetTable);
    const dbType = this.queryBuilderService.getDatabaseType?.() || 'postgres';

    await knex.schema.createTable(junction.tableName, (table) => {
      this.addSqlJunctionColumn(
        table,
        junction.sourceColumn,
        sourcePkType,
        dbType,
      ).notNullable();
      this.addSqlJunctionColumn(
        table,
        junction.targetColumn,
        targetPkType,
        dbType,
      ).notNullable();
      table.primary(
        [junction.sourceColumn, junction.targetColumn],
        junction.primaryKeyName,
      );
      table
        .foreign(junction.sourceColumn)
        .references('id')
        .inTable(junction.sourceTable)
        .onDelete(junction.onDelete)
        .onUpdate(junction.onUpdate)
        .withKeyName(junction.sourceForeignKeyName);
      table
        .foreign(junction.targetColumn)
        .references('id')
        .inTable(junction.targetTable)
        .onDelete(junction.onDelete)
        .onUpdate(junction.onUpdate)
        .withKeyName(junction.targetForeignKeyName);
      table.index([junction.sourceColumn], junction.sourceIndexName);
      table.index([junction.targetColumn], junction.targetIndexName);
      table.index(
        [junction.targetColumn, junction.sourceColumn],
        junction.reverseIndexName,
      );
    });
    this.log(`Created missing junction table '${junction.tableName}'`);
  }

  private getSqlLegacyJunctionCandidates(input: {
    oldJunctionTableName: string | null;
    oldJunctionSourceColumn: string | null;
    oldJunctionTargetColumn: string | null;
    sourceTable: string;
    targetTable: string;
    sourcePropertyName: string;
    junctionTableName: string;
  }): Array<{
    tableName: string;
    sourceColumn: string | null;
    targetColumn: string | null;
  }> {
    const candidates = [
      {
        tableName: input.oldJunctionTableName,
        sourceColumn: input.oldJunctionSourceColumn,
        targetColumn: input.oldJunctionTargetColumn,
      },
      {
        tableName: `${input.sourceTable}_${input.sourcePropertyName}_${input.targetTable}`,
        sourceColumn: `${input.sourceTable}Id`,
        targetColumn: `${input.targetTable}Id`,
      },
    ];
    const seen = new Set<string>();
    return candidates.filter(
      (
        candidate,
      ): candidate is {
        tableName: string;
        sourceColumn: string | null;
        targetColumn: string | null;
      } => {
        if (
          !candidate.tableName ||
          candidate.tableName === input.junctionTableName ||
          seen.has(candidate.tableName)
        ) {
          return false;
        }
        seen.add(candidate.tableName);
        return true;
      },
    );
  }

  private async mergeAndDropSqlLegacyJunctionTable(
    knex: Knex,
    input: {
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
      legacyTableName: string;
      legacySourceColumn: string | null;
      legacyTargetColumn: string | null;
      sourceTable: string;
      targetTable: string;
    },
  ): Promise<void> {
    const legacyColumns = await this.resolveSqlLegacyJunctionColumns(
      knex,
      input,
    );
    const rows = await knex(input.legacyTableName).select(
      legacyColumns.sourceColumn,
      legacyColumns.targetColumn,
    );
    const values = rows.map((row: any) => ({
      [input.junctionSourceColumn]:
        row[input.junctionSourceColumn] ?? row[legacyColumns.sourceColumn],
      [input.junctionTargetColumn]:
        row[input.junctionTargetColumn] ?? row[legacyColumns.targetColumn],
    }));
    const unmappable = values.filter(
      (row: any) =>
        row[input.junctionSourceColumn] == null ||
        row[input.junctionTargetColumn] == null,
    );
    if (unmappable.length > 0) {
      throw new Error(
        `Junction healing blocked: ${unmappable.length} unmappable row(s) in '${input.legacyTableName}'. ` +
          `Legacy table will NOT be dropped until all source rows are mappable.`,
      );
    }
    if (values.length > 0) {
      await knex(input.junctionTableName)
        .insert(values)
        .onConflict([input.junctionSourceColumn, input.junctionTargetColumn])
        .ignore();
    }
    if (typeof knex.schema.dropTableIfExists === 'function') {
      await knex.schema.dropTableIfExists(input.legacyTableName);
    } else {
      await knex.schema.dropTable(input.legacyTableName);
    }
    this.log(
      `Merged and removed legacy junction table '${input.legacyTableName}' into '${input.junctionTableName}'`,
    );
  }

  private async resolveSqlLegacyJunctionColumns(
    knex: Knex,
    input: {
      legacyTableName: string;
      legacySourceColumn: string | null;
      legacyTargetColumn: string | null;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
      sourceTable: string;
      targetTable: string;
    },
  ): Promise<{ sourceColumn: string; targetColumn: string }> {
    const columnInfo = await knex(input.legacyTableName).columnInfo();
    const available = new Set(Object.keys(columnInfo || {}));
    const sourceCandidates = [
      input.legacySourceColumn,
      `${input.sourceTable}Id`,
      input.junctionSourceColumn,
      'sourceId',
    ].filter((column): column is string => Boolean(column));
    const targetCandidates = [
      input.legacyTargetColumn,
      `${input.targetTable}Id`,
      input.junctionTargetColumn,
      'targetId',
    ].filter((column): column is string => Boolean(column));
    const sourceColumn = sourceCandidates.find((column) =>
      available.has(column),
    );
    const targetColumn = targetCandidates.find(
      (column) => available.has(column) && column !== sourceColumn,
    );
    if (!sourceColumn || !targetColumn) {
      throw new Error(
        `Junction healing blocked: cannot map legacy columns in '${input.legacyTableName}' to ` +
          `${input.junctionSourceColumn}/${input.junctionTargetColumn}.`,
      );
    }
    return { sourceColumn, targetColumn };
  }

  private async ensureSqlJunctionColumns(
    knex: Knex,
    input: {
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
      oldJunctionSourceColumn: string | null;
      oldJunctionTargetColumn: string | null;
    },
  ): Promise<void> {
    await this.renameSqlJunctionColumnIfNeeded(
      knex,
      input.junctionTableName,
      input.oldJunctionSourceColumn,
      input.junctionSourceColumn,
    );
    await this.renameSqlJunctionColumnIfNeeded(
      knex,
      input.junctionTableName,
      input.oldJunctionTargetColumn,
      input.junctionTargetColumn,
    );
  }

  private async renameSqlJunctionColumnIfNeeded(
    knex: Knex,
    tableName: string,
    oldColumn: string | null,
    newColumn: string,
  ): Promise<void> {
    if (!oldColumn || oldColumn === newColumn) return;
    const oldExists = await knex.schema.hasColumn(tableName, oldColumn);
    const newExists = await knex.schema.hasColumn(tableName, newColumn);
    if (!oldExists || newExists) return;
    await knex.schema.alterTable(tableName, (table) => {
      table.renameColumn(oldColumn, newColumn);
    });
    this.log(
      `Renamed junction column '${tableName}.${oldColumn}' to '${newColumn}'`,
    );
  }

  private addSqlJunctionColumn(
    table: Knex.CreateTableBuilder,
    columnName: string,
    pkType: 'uuid' | 'varchar' | 'integer',
    dbType: string,
  ): Knex.ColumnBuilder {
    if (pkType === 'uuid') {
      return table.uuid(columnName);
    }
    if (pkType === 'varchar') {
      return table.string(columnName, 255);
    }
    return dbType === 'mysql'
      ? table.integer(columnName).unsigned()
      : table.integer(columnName);
  }

  async getSqlPrimaryKeyType(
    tableName: string,
  ): Promise<'uuid' | 'varchar' | 'integer'> {
    const table =
      await this.metadataCacheService.lookupTableByName?.(tableName);
    const primaryColumn = table?.columns?.find(
      (column: any) => column.isPrimary,
    );
    const type = String(primaryColumn?.type || '').toLowerCase();
    if (type === 'uuid' || type === 'uuidv4' || type.includes('uuid')) {
      return 'uuid';
    }
    if (type === 'varchar' || type === 'string' || type === 'char') {
      return 'varchar';
    }
    const physicalType = await this.getPhysicalSqlPrimaryKeyType(tableName);
    if (physicalType) {
      return physicalType;
    }
    return 'integer';
  }

  private async getPhysicalSqlPrimaryKeyType(
    tableName: string,
  ): Promise<'uuid' | 'varchar' | 'integer' | null> {
    const knex = this.queryBuilderService.getKnex?.();
    if (!knex) return null;
    const dbType = this.queryBuilderService.getDatabaseType?.() || 'postgres';
    try {
      if (dbType === 'mysql') {
        const result = await knex.raw(
          `
          SELECT DATA_TYPE, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            AND COLUMN_NAME = 'id'
        `,
          [tableName],
        );
        const row = result?.[0]?.[0];
        return this.normalizePhysicalSqlPrimaryKeyType({
          dataType: row?.DATA_TYPE,
          columnType: row?.COLUMN_TYPE,
          maxLength: row?.CHARACTER_MAXIMUM_LENGTH,
        });
      }
      if (dbType === 'postgres') {
        const result = await knex.raw(
          `
          SELECT data_type, udt_name, character_maximum_length
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = ?
            AND column_name = 'id'
        `,
          [tableName],
        );
        const row = result?.rows?.[0];
        return this.normalizePhysicalSqlPrimaryKeyType({
          dataType: row?.data_type,
          udtName: row?.udt_name,
          maxLength: row?.character_maximum_length,
        });
      }
    } catch (error: any) {
      this.warn(
        `Could not inspect primary key type for ${tableName}: ${error?.message || error}`,
      );
    }
    return null;
  }

  private normalizePhysicalSqlPrimaryKeyType(input: {
    dataType?: string | null;
    columnType?: string | null;
    udtName?: string | null;
    maxLength?: number | string | null;
  }): 'uuid' | 'varchar' | 'integer' | null {
    const dataType = String(input.dataType || '').toLowerCase();
    const columnType = String(input.columnType || '').toLowerCase();
    const udtName = String(input.udtName || '').toLowerCase();
    const maxLength =
      input.maxLength === undefined || input.maxLength === null
        ? null
        : Number(input.maxLength);
    if (dataType === 'uuid' || udtName === 'uuid') {
      return 'uuid';
    }
    if (
      (dataType === 'char' ||
        dataType === 'character' ||
        dataType === 'varchar' ||
        dataType === 'character varying') &&
      (maxLength === 36 || columnType.includes('(36)'))
    ) {
      return 'uuid';
    }
    if (
      dataType === 'char' ||
      dataType === 'character' ||
      dataType === 'varchar' ||
      dataType === 'character varying' ||
      dataType === 'text'
    ) {
      return 'varchar';
    }
    if (
      dataType === 'int' ||
      dataType === 'integer' ||
      dataType === 'bigint' ||
      dataType === 'smallint' ||
      dataType === 'mediumint'
    ) {
      return 'integer';
    }
    return null;
  }

  private isSqlOwningRelation(rel: any): boolean {
    return (
      rel.type === 'many-to-one' ||
      (rel.type === 'one-to-one' && !rel.mappedById)
    );
  }

  private async findSqlForeignKeyConstraintName(
    knex: Knex,
    tableName: string,
    columnName: string,
  ): Promise<string | null> {
    const client = String((knex.client.config as any).client || '');
    if (client === 'pg') {
      const result = await knex.raw(
        `
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = current_schema()
          AND tc.table_name = ?
          AND kcu.column_name = ?
        LIMIT 1
      `,
        [tableName, columnName],
      );
      return result.rows?.[0]?.constraint_name || null;
    }
    if (client === 'mysql2') {
      const result = await knex.raw(
        `
        SELECT CONSTRAINT_NAME AS constraint_name
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = ?
          AND COLUMN_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
        LIMIT 1
      `,
        [tableName, columnName],
      );
      return result[0]?.[0]?.constraint_name || null;
    }
    return null;
  }
}
