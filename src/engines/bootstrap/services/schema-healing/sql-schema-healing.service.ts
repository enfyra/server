import {
  QueryBuilderService,
  getForeignKeyColumnName,
  getShortFkConstraintName,
} from '@enfyra/kernel';
import type { Knex } from 'knex';
import { MetadataCacheService } from '../../../cache';
import { buildSqlJunctionTableContract } from '../../../knex/utils/sql-physical-schema-contract';
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
    const standardExists = await knex.schema.hasTable(input.junctionTableName);
    if (standardExists) {
      await this.ensureSqlJunctionColumns(knex, input);
      return;
    }

    if (
      input.oldJunctionTableName &&
      input.oldJunctionTableName !== input.junctionTableName &&
      (await knex.schema.hasTable(input.oldJunctionTableName))
    ) {
      await knex.schema.renameTable(
        input.oldJunctionTableName,
        input.junctionTableName,
      );
      this.log(
        `Renamed junction table '${input.oldJunctionTableName}' to '${input.junctionTableName}'`,
      );
      await this.ensureSqlJunctionColumns(knex, input);
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
      return dbType === 'postgres'
        ? table.uuid(columnName)
        : table.string(columnName, 36);
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
