import { QueryBuilderService } from '@enfyra/kernel';
import type { Knex } from 'knex';
import { normalizeMongoPrimaryKeyColumn } from '../../../../modules/table-management/utils/mongo-primary-key.util';
import { addColumnToTable } from '../../../knex/utils/migration/column-operations';
import type { SchemaHealingSnapshot } from '../../types/schema-healing.types';
import {
  buildExpectedRelations,
  RELATION_DEFAULTS,
} from '../../utils/metadata-comparison.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';

export class SystemMetadataHealingService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
  }

  async repairSqlSystemPhysicalColumns(): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    if (!knex?.schema?.hasTable) return 0;
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (!(await knex.schema.hasTable(coreNames.table))) return 0;
    if (!(await knex.schema.hasTable(coreNames.column))) return 0;

    const systemTables = await knex(coreNames.table)
      .where({ isSystem: true })
      .select('id', 'name');
    let repaired = 0;

    for (const tableDef of systemTables) {
      if (!tableDef?.id || !tableDef?.name) continue;
      if (!(await knex.schema.hasTable(tableDef.name))) continue;

      const columns = await knex(coreNames.column)
        .where({ tableId: tableDef.id })
        .select('*');
      for (const column of columns) {
        if (!column?.name || column.isPrimary) continue;
        if (await knex.schema.hasColumn(tableDef.name, column.name)) continue;
        await knex.schema.alterTable(
          tableDef.name,
          (table: Knex.TableBuilder) => {
            addColumnToTable(
              table as any,
              column,
              this.queryBuilderService.getDatabaseType(),
            );
          },
        );
        repaired++;
      }
    }

    return repaired;
  }

  async repairSqlSystemPhysicalColumnsFromSnapshot(
    snapshot: SchemaHealingSnapshot,
  ): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    if (!knex?.schema?.hasTable) return 0;

    let repaired = 0;

    for (const tableDef of Object.values(snapshot)) {
      const tableName = tableDef?.name;
      if (!tableDef?.isSystem || !tableName || !tableDef.columns?.length) {
        continue;
      }
      if (!(await knex.schema.hasTable(tableName))) continue;

      for (const column of tableDef.columns) {
        if (!column?.name || column.isPrimary) continue;
        if (await knex.schema.hasColumn(tableName, column.name)) continue;
        await knex.schema.alterTable(tableName, (table: Knex.TableBuilder) => {
          addColumnToTable(
            table as any,
            column,
            this.queryBuilderService.getDatabaseType(),
          );
        });
        repaired++;
      }
    }

    return repaired;
  }

  async repairSqlSystemColumnMetadataFromSnapshot(
    snapshot: Record<
      string,
      { name?: string; isSystem?: boolean; columns?: any[] }
    >,
  ): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    if (!knex?.schema?.hasTable) return 0;
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (!(await knex.schema.hasTable(coreNames.table))) return 0;
    if (!(await knex.schema.hasTable(coreNames.column))) return 0;

    let repaired = 0;
    for (const tableDef of Object.values(snapshot)) {
      const tableName = tableDef?.name;
      if (!tableDef?.isSystem || !tableName || !tableDef.columns?.length) {
        continue;
      }

      const tableRecord = await knex(coreNames.table)
        .where({ name: tableName })
        .first();
      if (!tableRecord?.id) continue;

      const existingColumns = await knex(coreNames.column)
        .where({ tableId: tableRecord.id })
        .select('name');
      const existingNames = new Set(
        existingColumns.map((column: any) => column.name),
      );

      for (const column of tableDef.columns) {
        if (!column?.name || existingNames.has(column.name)) continue;
        await knex(coreNames.column).insert({
          name: column.name,
          type: column.type,
          isPrimary: column.isPrimary || false,
          isGenerated: column.isGenerated || false,
          isNullable: column.isNullable ?? true,
          isSystem: column.isSystem || false,
          isUpdatable: column.isUpdatable ?? true,
          isPublished: column.isPublished ?? true,
          isEncrypted: column.isEncrypted ?? false,
          defaultValue: JSON.stringify(column.defaultValue ?? null),
          options: JSON.stringify(column.options || null),
          description: column.description,
          placeholder: column.placeholder,
          tableId: tableRecord.id,
        });
        repaired++;
      }
    }

    return repaired;
  }

  async repairMongoSystemColumnMetadataFromSnapshot(
    snapshot: Record<
      string,
      { name?: string; isSystem?: boolean; columns?: any[] }
    >,
  ): Promise<number> {
    const db = this.queryBuilderService.getMongoDb?.();
    if (!db) return 0;
    const coreNames = await this.systemCoreTableResolver.getNames();

    const tableCollection = db.collection(coreNames.table);
    const columnCollection = db.collection(coreNames.column);
    let repaired = 0;

    for (const tableDef of Object.values(snapshot)) {
      const tableName = tableDef?.name;
      if (!tableDef?.isSystem || !tableName || !tableDef.columns?.length) {
        continue;
      }

      const tableRecord = await tableCollection.findOne({ name: tableName });
      if (!tableRecord?._id) continue;

      const existingColumns = await columnCollection
        .find({ table: tableRecord._id }, { projection: { name: 1 } })
        .toArray();
      const existingNames = new Set(
        existingColumns.map((column: any) => column.name),
      );

      for (const snapshotColumn of tableDef.columns) {
        const column = normalizeMongoPrimaryKeyColumn(snapshotColumn);
        if (!column?.name || existingNames.has(column.name)) continue;
        await columnCollection.insertOne({
          name: column.name,
          type: column.type,
          isPrimary: column.isPrimary || false,
          isGenerated: column.isGenerated || false,
          isNullable: column.isNullable ?? true,
          isSystem: column.isSystem || false,
          isUpdatable: column.isUpdatable ?? true,
          isPublished: column.isPublished ?? true,
          isEncrypted: column.isEncrypted ?? false,
          defaultValue: column.defaultValue ?? null,
          options: column.options || null,
          description: column.description,
          placeholder: column.placeholder,
          table: tableRecord._id,
          createdAt: new Date(),
          updatedAt: new Date(),
        });
        existingNames.add(column.name);
        repaired++;
      }
    }

    return repaired;
  }

  buildDisplayMetadataUpdate(
    current: Record<string, any>,
    target: Record<string, any>,
    fields: string[],
    defaults: Record<string, any> = {},
  ): Record<string, unknown> {
    const update: Record<string, unknown> = {};
    for (const field of fields) {
      const currentValue = current[field] ?? defaults[field] ?? null;
      const targetValue = target[field] ?? defaults[field] ?? null;
      if (currentValue !== targetValue) update[field] = targetValue;
    }
    return update;
  }

  async repairSqlSystemDisplayMetadataFromSnapshot(
    snapshot: Record<string, any>,
  ): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    if (!knex?.schema?.hasTable) return 0;
    const coreNames = await this.systemCoreTableResolver.getNames();
    if (!(await knex.schema.hasTable(coreNames.table))) return 0;
    if (!(await knex.schema.hasTable(coreNames.column))) return 0;
    if (!(await knex.schema.hasTable(coreNames.relation))) return 0;

    const expectedRelations = buildExpectedRelations(snapshot);
    const expectedByTable = new Map<string, Map<string, Record<string, any>>>();
    for (const [key, relation] of expectedRelations) {
      const dot = key.indexOf('.');
      const tableName = key.slice(0, dot);
      const propertyName = key.slice(dot + 1);
      if (!expectedByTable.has(tableName)) {
        expectedByTable.set(tableName, new Map());
      }
      expectedByTable.get(tableName)!.set(propertyName, relation);
    }

    let repaired = 0;
    for (const tableDef of Object.values(snapshot)) {
      if (!tableDef?.isSystem || !tableDef?.name) continue;
      const tableRecord = await knex(coreNames.table)
        .where({ name: tableDef.name })
        .first();
      if (!tableRecord?.id) continue;

      const tableUpdate = this.buildDisplayMetadataUpdate(
        tableRecord,
        tableDef,
        ['description'],
      );
      if (Object.keys(tableUpdate).length > 0) {
        await knex(coreNames.table)
          .where({ id: tableRecord.id })
          .update(tableUpdate);
        repaired++;
      }

      const columns = await knex(coreNames.column)
        .where({ tableId: tableRecord.id })
        .select('id', 'name', 'description', 'placeholder');
      const columnsByName = new Map<string, any>(
        columns.map((column: any) => [column.name, column]),
      );
      for (const targetColumn of tableDef.columns ?? []) {
        const currentColumn = columnsByName.get(targetColumn.name);
        if (!currentColumn?.id) continue;
        const columnUpdate = this.buildDisplayMetadataUpdate(
          currentColumn,
          targetColumn,
          ['description', 'placeholder'],
        );
        if (Object.keys(columnUpdate).length === 0) continue;
        await knex(coreNames.column)
          .where({ id: currentColumn.id })
          .update(columnUpdate);
        repaired++;
      }

      const relations = await knex(coreNames.relation)
        .where({ sourceTableId: tableRecord.id })
        .select(
          'id',
          'propertyName',
          'description',
          'isSystem',
          'isUpdatable',
          'isPublished',
        );
      const relationsByProperty = new Map<string, any>(
        relations.map((relation: any) => [relation.propertyName, relation]),
      );
      const tableExpectedRelations = expectedByTable.get(tableDef.name);
      if (!tableExpectedRelations) continue;
      for (const [propertyName, targetRelation] of tableExpectedRelations) {
        const currentRelation = relationsByProperty.get(propertyName);
        if (!currentRelation?.id) continue;
        const relationUpdate = this.buildDisplayMetadataUpdate(
          currentRelation,
          targetRelation,
          ['description', 'isSystem', 'isUpdatable', 'isPublished'],
          RELATION_DEFAULTS,
        );
        if (Object.keys(relationUpdate).length === 0) continue;
        await knex(coreNames.relation)
          .where({ id: currentRelation.id })
          .update(relationUpdate);
        repaired++;
      }
    }
    return repaired;
  }

  async repairMongoSystemDisplayMetadataFromSnapshot(
    snapshot: Record<string, any>,
  ): Promise<number> {
    const db = this.queryBuilderService.getMongoDb?.();
    if (!db) return 0;
    const coreNames = await this.systemCoreTableResolver.getNames();
    const tableCollection = db.collection(coreNames.table);
    const columnCollection = db.collection(coreNames.column);
    const relationCollection = db.collection(coreNames.relation);

    const expectedRelations = buildExpectedRelations(snapshot);
    const expectedByTable = new Map<string, Map<string, Record<string, any>>>();
    for (const [key, relation] of expectedRelations) {
      const dot = key.indexOf('.');
      const tableName = key.slice(0, dot);
      const propertyName = key.slice(dot + 1);
      if (!expectedByTable.has(tableName)) {
        expectedByTable.set(tableName, new Map());
      }
      expectedByTable.get(tableName)!.set(propertyName, relation);
    }

    let repaired = 0;
    for (const tableDef of Object.values(snapshot)) {
      if (!tableDef?.isSystem || !tableDef?.name) continue;
      const tableRecord = await tableCollection.findOne({
        name: tableDef.name,
      });
      if (!tableRecord?._id) continue;

      const tableUpdate = this.buildDisplayMetadataUpdate(
        tableRecord,
        tableDef,
        ['description'],
      );
      if (Object.keys(tableUpdate).length > 0) {
        await tableCollection.updateOne(
          { _id: tableRecord._id },
          { $set: tableUpdate },
        );
        repaired++;
      }

      const columns = await columnCollection
        .find({ table: tableRecord._id })
        .toArray();
      const columnsByName = new Map<string, any>(
        columns.map((column: any) => [column.name, column]),
      );
      for (const targetColumn of tableDef.columns ?? []) {
        const currentColumn = columnsByName.get(targetColumn.name);
        if (!currentColumn?._id) continue;
        const columnUpdate = this.buildDisplayMetadataUpdate(
          currentColumn,
          targetColumn,
          ['description', 'placeholder'],
        );
        if (Object.keys(columnUpdate).length === 0) continue;
        await columnCollection.updateOne(
          { _id: currentColumn._id },
          { $set: columnUpdate },
        );
        repaired++;
      }

      const relations = await relationCollection
        .find({ sourceTable: tableRecord._id })
        .toArray();
      const relationsByProperty = new Map<string, any>(
        relations.map((relation: any) => [relation.propertyName, relation]),
      );
      const tableExpectedRelations = expectedByTable.get(tableDef.name);
      if (!tableExpectedRelations) continue;
      for (const [propertyName, targetRelation] of tableExpectedRelations) {
        const currentRelation = relationsByProperty.get(propertyName);
        if (!currentRelation?._id) continue;
        const relationUpdate = this.buildDisplayMetadataUpdate(
          currentRelation,
          targetRelation,
          ['description', 'isSystem', 'isUpdatable', 'isPublished'],
          RELATION_DEFAULTS,
        );
        if (Object.keys(relationUpdate).length === 0) continue;
        await relationCollection.updateOne(
          { _id: currentRelation._id },
          { $set: relationUpdate },
        );
        repaired++;
      }
    }
    return repaired;
  }
}
