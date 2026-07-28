import { QueryBuilderService } from '@enfyra/kernel';
import type { TableRenameDef } from '../../../../shared/types/schema-migration.types';
import { SYSTEM_TABLES } from '../../../../shared/utils/system-tables.constants';
import { getValidTableRenames } from '../../utils/metadata-migration.util';
import { getMissingMetadataRowValues } from '../../utils/metadata-row-merge.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';
import { MetadataOverlapIdentityService } from './metadata-overlap-identity.service';

export class MetadataSqlOverlapReconciler {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly overlapIdentity: MetadataOverlapIdentityService;
  private readonly verbose: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    overlapIdentity: MetadataOverlapIdentityService;
    verbose: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.overlapIdentity = deps.overlapIdentity;
    this.verbose = deps.verbose;
  }

  async dropLegacyRenamedSqlTables(renames: TableRenameDef[]): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const legacyNames = getValidTableRenames(renames)
      .filter((rename) => rename.from !== rename.to)
      .map((rename) => rename.from)
      .reverse();
    if (legacyNames.length === 0) return;

    const existing: string[] = [];
    for (const tableName of legacyNames) {
      if (await knex.schema.hasTable(tableName)) existing.push(tableName);
    }
    if (existing.length === 0) return;

    const client = String(knex.client.config.client).toLowerCase();
    if (client.includes('pg') || client.includes('postgres')) {
      for (const tableName of existing) {
        try {
          await knex.schema.dropTableIfExists(tableName);
        } catch (error: any) {
          if (error?.code !== '2BP01') throw error;
          await knex.raw('DROP TABLE IF EXISTS ?? CASCADE', [tableName]);
        }
      }
    } else {
      await knex.transaction(async (trx: any) => {
        await trx.raw('SET FOREIGN_KEY_CHECKS = 0');
        try {
          for (const tableName of existing) {
            await trx.schema.dropTableIfExists(tableName);
          }
        } finally {
          await trx.raw('SET FOREIGN_KEY_CHECKS = 1');
        }
      });
    }

    this.verbose(`  Removed ${existing.length} reconciled legacy SQL table(s)`);
  }

  async findSqlTableRecord(
    tableStore: string,
    tableName: string,
  ): Promise<any | null> {
    const knex = this.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(tableStore))) return null;
    return knex(tableStore).where({ name: tableName }).first();
  }

  private async getSqlOverlapColumns(
    oldTable: string,
    newTable: string,
  ): Promise<string[]> {
    const knex = this.queryBuilderService.getKnex();
    const [oldInfo, newInfo] = await Promise.all([
      knex(oldTable).columnInfo(),
      knex(newTable).columnInfo(),
    ]);
    return Object.keys(oldInfo).filter((column) => column in newInfo);
  }

  private async getSqlMergedColumns(
    oldTable: string,
    newTable: string,
  ): Promise<string[]> {
    const knex = this.queryBuilderService.getKnex();
    const [oldInfo, newInfo] = await Promise.all([
      knex(oldTable).columnInfo(),
      knex(newTable).columnInfo(),
    ]);
    const missingColumns = Object.keys(oldInfo).filter(
      (column) => !(column in newInfo),
    );
    if (missingColumns.length > 0) {
      await this.addMissingSqlColumns(newTable, oldInfo, missingColumns);
    }
    const refreshedNewInfo = await knex(newTable).columnInfo();
    return Object.keys(oldInfo).filter((column) => column in refreshedNewInfo);
  }

  private async addMissingSqlColumns(
    tableName: string,
    sourceInfo: Record<string, any>,
    columns: string[],
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    await knex.schema.alterTable(tableName, (table: any) => {
      for (const column of columns) {
        table.specificType(
          column,
          this.getPortableSqlColumnType(sourceInfo[column]),
        );
      }
    });
    this.verbose(
      `  Added ${columns.length} legacy column(s) to ${tableName} before overlap merge`,
    );
  }

  private getPortableSqlColumnType(columnInfo: any): string {
    const type = String(columnInfo?.type || '').toLowerCase();
    const maxLength = Number(
      columnInfo?.maxLength || columnInfo?.characterMaximumLength || 0,
    );

    if (!type) return 'text';
    if (type.includes('bigint')) return 'bigint';
    if (type.includes('int')) return 'integer';
    if (type.includes('bool') || type === 'tinyint(1)') return 'boolean';
    if (type.includes('double')) return 'double precision';
    if (type.includes('float')) return 'float';
    if (type.includes('decimal') || type.includes('numeric')) return 'decimal';
    if (type.includes('jsonb')) return 'jsonb';
    if (type.includes('json')) return 'json';
    if (type.includes('timestamp')) return 'timestamp';
    if (type === 'date') return 'date';
    if (type.includes('time')) return 'time';
    if (type.includes('uuid')) return 'uuid';
    if (type.includes('text')) return 'text';
    if (type.includes('char'))
      return `varchar(${maxLength > 0 ? maxLength : 255})`;
    return 'text';
  }

  async reconcileSqlCoreTableOverlap(rename: TableRenameDef): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const columns = await this.getSqlMergedColumns(rename.from, rename.to);
    const [legacyRows, canonicalRows] = await Promise.all([
      knex(rename.from).select(columns),
      knex(rename.to).select(columns),
    ]);

    const canonicalKeys = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      this.overlapIdentity.trackCanonicalCoreTableId(rename, row);
      const key = this.overlapIdentity.getOverlapRowKey(rename, row, columns, {
        remapCoreOwnerIds: false,
      });
      if (key !== null && key !== undefined) canonicalKeys.add(key);
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
    }
    const occupiedIds = new Set(
      canonicalRows
        .map((row: any) => row?.id)
        .filter((id: any) => id !== undefined && id !== null)
        .map((id: any) => String(id)),
    );
    let conflictCount = 0;
    let skippedCount = 0;
    const rowsToInsert = legacyRows.filter((row: any) => {
      const key = this.overlapIdentity.getOverlapRowKey(rename, row, columns);
      if (key === null || key === undefined) {
        skippedCount += 1;
        return false;
      }
      if (canonicalKeys.has(key)) {
        const canonicalRow = this.overlapIdentity.findRowByOverlapKey(
          rename,
          canonicalRows,
          key,
          columns,
        );
        if (
          canonicalRow &&
          this.overlapIdentity.coreRowsConflict(
            rename,
            row,
            canonicalRow,
            columns,
          )
        ) {
          conflictCount += 1;
        }
        this.overlapIdentity.trackExistingCoreRowRemap(
          rename,
          row,
          canonicalRows,
        );
        return false;
      }
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        return false;
      }
      return true;
    });

    let insertedCount = 0;
    for (const row of rowsToInsert) {
      const projected = this.overlapIdentity.projectCoreRowToColumns(
        rename,
        row,
        columns,
      );
      if (
        projected?.id !== undefined &&
        projected?.id !== null &&
        occupiedIds.has(String(projected.id))
      ) {
        delete projected.id;
      }
      await knex(rename.to).insert(projected);
      insertedCount += 1;
      await this.overlapIdentity.trackInsertedSqlCoreRowRemap(
        rename,
        row,
        projected,
      );
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        projected,
      );
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      const insertedId =
        projected?.id ??
        (projected?.name
          ? (await knex(rename.to).where({ name: projected.name }).first())?.id
          : undefined);
      if (insertedId !== undefined && insertedId !== null) {
        occupiedIds.add(String(insertedId));
      }
    }

    if (insertedCount > 0) {
      this.verbose(
        `  Copied ${insertedCount} missing core metadata row(s) from ${rename.from} to ${rename.to}`,
      );
    }
    if (conflictCount > 0 || skippedCount > 0) {
      throw new Error(
        `SQL core overlap reconciliation blocked for ${rename.from} → ${rename.to}: ${conflictCount} conflicting row(s), ${skippedCount} unidentifiable row(s)`,
      );
    }
  }

  async reconcileSqlTableOverlap(rename: TableRenameDef): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const columns = await this.getSqlMergedColumns(rename.from, rename.to);
    const [legacyRows, canonicalRows] = await Promise.all([
      knex(rename.from).select(columns),
      knex(rename.to).select(columns),
    ]);
    const canonicalKeys = new Set<string>();
    const occupiedIds = new Set<string>();
    const canonicalMappedByKeys = new Set<string>();
    for (const row of canonicalRows) {
      const key = this.overlapIdentity.getOverlapRowKey(rename, row, columns, {
        remapCoreOwnerIds: false,
      });
      if (key) canonicalKeys.add(key);
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey) canonicalMappedByKeys.add(mappedByKey);
      if (row?.id !== undefined && row.id !== null) {
        occupiedIds.add(String(row.id));
      }
    }

    let insertedCount = 0;
    let conflictCount = 0;
    let skippedCount = 0;
    let updatedCount = 0;
    for (const row of legacyRows) {
      const key = this.overlapIdentity.getOverlapRowKey(rename, row, columns);
      if (!key) {
        skippedCount += 1;
        continue;
      }
      if (canonicalKeys.has(key)) {
        const canonicalRow = this.overlapIdentity.findRowByOverlapKey(
          rename,
          canonicalRows,
          key,
          columns,
        );
        if (
          canonicalRow &&
          this.overlapIdentity.rowsConflict(row, canonicalRow, columns)
        ) {
          conflictCount += 1;
        }
        if (canonicalRow) {
          const missingValues = getMissingMetadataRowValues(
            row,
            canonicalRow,
            columns,
          );
          const filter = this.overlapIdentity.getRowIdentityFilter(
            rename,
            canonicalRow,
          );
          if (filter && Object.keys(missingValues).length > 0) {
            await knex(rename.to).where(filter).update(missingValues);
            Object.assign(canonicalRow, missingValues);
            updatedCount += 1;
          }
        }
        continue;
      }
      const mappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        row,
      );
      if (mappedByKey && canonicalMappedByKeys.has(mappedByKey)) {
        conflictCount += 1;
        continue;
      }
      const projected = this.overlapIdentity.projectRowToColumns(row, columns);
      if (
        projected?.id !== undefined &&
        projected?.id !== null &&
        occupiedIds.has(String(projected.id))
      ) {
        delete projected.id;
      }
      await knex(rename.to).insert(projected);
      insertedCount += 1;
      canonicalKeys.add(key);
      const insertedMappedByKey = this.overlapIdentity.getRelationMappedByKey(
        rename,
        projected,
      );
      if (insertedMappedByKey) canonicalMappedByKeys.add(insertedMappedByKey);
      if (projected?.id !== undefined && projected.id !== null) {
        occupiedIds.add(String(projected.id));
      }
    }
    this.verbose(
      `  SQL table overlap reconciled for ${rename.from} → ${rename.to}: copied ${insertedCount}, updated ${updatedCount}, conflicts ${conflictCount}, skipped ${skippedCount}`,
    );
    if (conflictCount > 0 || skippedCount > 0) {
      throw new Error(
        `SQL overlap reconciliation blocked for ${rename.from} → ${rename.to}: ` +
          `${conflictCount} conflicting row(s), ${skippedCount} unidentifiable row(s). ` +
          `Legacy store will NOT be dropped until all rows are proven copied or equivalent.`,
      );
    }
  }

  async reconcileSqlTableMetadataRows(
    tableStore: string,
    sourceRow: any,
    targetRow: any,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    const coreNames = await this.systemCoreTableResolver.getNames();
    const sourceId = sourceRow.id;
    const targetId = targetRow.id;
    const tableColumns = [
      ...new Set([...Object.keys(sourceRow), ...Object.keys(targetRow)]),
    ];
    const tableUpdate = getMissingMetadataRowValues(
      sourceRow,
      targetRow,
      tableColumns,
    );
    if (Object.keys(tableUpdate).length > 0) {
      await knex(tableStore).where({ id: targetId }).update(tableUpdate);
    }

    if (await knex.schema.hasTable(coreNames.column)) {
      const [sourceColumns, targetColumns] = await Promise.all([
        knex(coreNames.column).where({ tableId: sourceId }).select('*'),
        knex(coreNames.column).where({ tableId: targetId }).select('*'),
      ]);
      const targetByName = new Map<string, any>(
        targetColumns.map((column: any) => [column.name, column]),
      );
      const hasColumnRule = await knex.schema.hasTable(
        SYSTEM_TABLES.columnRule,
      );
      const hasFieldPermission = await knex.schema.hasTable(
        SYSTEM_TABLES.fieldPermission,
      );
      for (const sourceColumn of sourceColumns) {
        const targetColumn = targetByName.get(sourceColumn.name);
        if (!targetColumn) {
          await knex(coreNames.column)
            .where({ id: sourceColumn.id })
            .update({ tableId: targetId });
          continue;
        }
        const update = getMissingMetadataRowValues(sourceColumn, targetColumn, [
          ...new Set([
            ...Object.keys(sourceColumn),
            ...Object.keys(targetColumn),
          ]),
        ]);
        if (Object.keys(update).length > 0) {
          await knex(coreNames.column)
            .where({ id: targetColumn.id })
            .update(update);
        }
        if (hasColumnRule) {
          await knex(SYSTEM_TABLES.columnRule)
            .where({ columnId: sourceColumn.id })
            .update({ columnId: targetColumn.id });
        }
        if (hasFieldPermission) {
          await knex(SYSTEM_TABLES.fieldPermission)
            .where({ columnId: sourceColumn.id })
            .update({ columnId: targetColumn.id });
        }
        await knex(coreNames.column).where({ id: sourceColumn.id }).delete();
      }
    }

    if (await knex.schema.hasTable(coreNames.relation)) {
      await knex(coreNames.relation)
        .where({ targetTableId: sourceId })
        .update({ targetTableId: targetId });
      const [sourceRelations, targetRelations] = await Promise.all([
        knex(coreNames.relation).where({ sourceTableId: sourceId }).select('*'),
        knex(coreNames.relation).where({ sourceTableId: targetId }).select('*'),
      ]);
      const targetByProperty = new Map<string, any>(
        targetRelations.map((relation: any) => [
          relation.propertyName,
          relation,
        ]),
      );
      const hasFieldPermission = await knex.schema.hasTable(
        SYSTEM_TABLES.fieldPermission,
      );
      for (const sourceRelation of sourceRelations) {
        const targetRelation = targetByProperty.get(
          sourceRelation.propertyName,
        );
        if (!targetRelation) {
          await knex(coreNames.relation)
            .where({ id: sourceRelation.id })
            .update({ sourceTableId: targetId });
          continue;
        }
        const update = getMissingMetadataRowValues(
          sourceRelation,
          targetRelation,
          [
            ...new Set([
              ...Object.keys(sourceRelation),
              ...Object.keys(targetRelation),
            ]),
          ],
        );
        delete update.sourceTableId;
        delete update.targetTableId;
        delete update.mappedById;
        if (Object.keys(update).length > 0) {
          await knex(coreNames.relation)
            .where({ id: targetRelation.id })
            .update(update);
        }
        if (hasFieldPermission) {
          await knex(SYSTEM_TABLES.fieldPermission)
            .where({ relationId: sourceRelation.id })
            .update({ relationId: targetRelation.id });
        }
        const mappedDependents = await knex(coreNames.relation)
          .where({ mappedById: sourceRelation.id })
          .select('*');
        for (const dependent of mappedDependents) {
          const canonicalDependent = await knex(coreNames.relation)
            .where({ mappedById: targetRelation.id })
            .where({ propertyName: dependent.propertyName })
            .first();
          if (canonicalDependent) {
            if (hasFieldPermission) {
              await knex(SYSTEM_TABLES.fieldPermission)
                .where({ relationId: dependent.id })
                .update({ relationId: canonicalDependent.id });
            }
            await knex(coreNames.relation).where({ id: dependent.id }).delete();
          } else {
            await knex(coreNames.relation)
              .where({ id: dependent.id })
              .update({ mappedById: targetRelation.id });
          }
        }
        await knex(coreNames.relation)
          .where({ id: sourceRelation.id })
          .delete();
      }
    }

    if (await knex.schema.hasTable(SYSTEM_TABLES.route)) {
      await knex(SYSTEM_TABLES.route)
        .where({ mainTableId: sourceId })
        .update({ mainTableId: targetId });
    }
    if (await knex.schema.hasTable(SYSTEM_TABLES.graphql)) {
      const sourceGraphql = await knex(SYSTEM_TABLES.graphql)
        .where({ tableId: sourceId })
        .first();
      const targetGraphql = await knex(SYSTEM_TABLES.graphql)
        .where({ tableId: targetId })
        .first();
      if (sourceGraphql && targetGraphql) {
        const update = getMissingMetadataRowValues(
          sourceGraphql,
          targetGraphql,
          [
            ...new Set([
              ...Object.keys(sourceGraphql),
              ...Object.keys(targetGraphql),
            ]),
          ],
        );
        delete update.tableId;
        if (Object.keys(update).length > 0) {
          await knex(SYSTEM_TABLES.graphql)
            .where({ id: targetGraphql.id })
            .update(update);
        }
        await knex(SYSTEM_TABLES.graphql)
          .where({ id: sourceGraphql.id })
          .delete();
      } else if (sourceGraphql) {
        await knex(SYSTEM_TABLES.graphql)
          .where({ id: sourceGraphql.id })
          .update({ tableId: targetId });
      }
    }

    await knex(tableStore).where({ id: sourceId }).delete();
    this.verbose(
      `  Reconciled table metadata overlap: ${sourceRow.name} → ${targetRow.name}`,
    );
  }

  async renameSqlTableMetadataRow(
    tableStore: string,
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const knex = this.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(tableStore))) return;
    const sourceRow = tableId
      ? await knex(tableStore).where({ id: tableId }).first()
      : await knex(tableStore).where({ name: rename.from }).first();
    const targetRow = await knex(tableStore).where({ name: rename.to }).first();
    if (targetRow) {
      if (sourceRow && String(sourceRow.id) !== String(targetRow.id)) {
        await this.reconcileSqlTableMetadataRows(
          tableStore,
          sourceRow,
          targetRow,
        );
      }
      return;
    }
    if (!sourceRow) return;
    const query = tableId
      ? knex(tableStore).where({ id: tableId })
      : knex(tableStore).where({ name: rename.from });
    await query.update({ name: rename.to });
  }

  async updateSqlCanonicalRoutePath(
    rename: TableRenameDef,
    tableId?: any,
  ): Promise<void> {
    const routeTable = await this.detectSqlRouteTable();
    if (!routeTable) return;

    const knex = this.queryBuilderService.getKnex();
    const query = knex(routeTable).where({ path: `/${rename.from}` });
    if (tableId) query.andWhere({ mainTableId: tableId });
    await query.update({ path: `/${rename.to}` });
  }

  private async detectSqlRouteTable(): Promise<string | null> {
    const knex = this.queryBuilderService.getKnex();
    if (await knex.schema.hasTable(SYSTEM_TABLES.route))
      return SYSTEM_TABLES.route;
    if (await knex.schema.hasTable('route_definition'))
      return 'route_definition';
    return null;
  }
}
