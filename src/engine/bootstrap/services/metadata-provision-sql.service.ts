import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { SqlSchemaMigrationService } from '../../../engine/knex/services/sql-schema-migration.service';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
} from '../../../domain/query-dsl/utils/sql-schema-naming.util';
import { loadRelationRenameMap } from '../../../domain/bootstrap/utils/load-relation-rename-map';
import { parseSnapshotToSchema } from '../../../engine/knex/utils/provision/schema-parser';
import { syncTable } from '../../../engine/knex/utils/provision/sync-table';
import { syncJunctionTables } from '../../../engine/knex/utils/provision/junction-tables';
import { createAllTables } from '../../../engine/knex/utils/provision/table-builder';

export class MetadataProvisionSqlService {
  private readonly logger = new Logger(MetadataProvisionSqlService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly schemaMigrationService: SqlSchemaMigrationService;
  private readonly dbType: string;
  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    databaseConfigService: DatabaseConfigService;
    sqlSchemaMigrationService: SqlSchemaMigrationService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.databaseConfigService = deps.databaseConfigService;
    this.schemaMigrationService = deps.sqlSchemaMigrationService;
    this.dbType = this.databaseConfigService.getDbType();
  }
  private async insertAndGetId(
    trx: any,
    tableName: string,
    data: any,
  ): Promise<number> {
    if (this.dbType === 'postgres') {
      const [result] = await trx(tableName).insert(data).returning('id');
      return result.id;
    } else {
      const [id] = await trx(tableName).insert(data);
      return id;
    }
  }
  private async ensureCoreTables(): Promise<void> {
    const qb = this.queryBuilderService.getConnection();
    const coreTables = [
      'table_definition',
      'column_definition',
      'relation_definition',
    ];

    for (const tableName of coreTables) {
      const exists = await qb.schema.hasTable(tableName);
      if (!exists) {
        this.logger.log(`Creating core table: ${tableName}`);
        if (tableName === 'table_definition') {
          await qb.schema.createTable(tableName, (table) => {
            table.increments('id').primary();
            table.string('name').notNullable().unique();
            table.boolean('isSystem').notNullable().defaultTo(false);
            table.boolean('isSingleRecord').notNullable().defaultTo(false);
            table.json('uniques').nullable();
            table.json('indexes').nullable();
            table.string('alias').nullable().unique();
            table.text('description').nullable();
            table.json('metadata').nullable();
            table.timestamp('createdAt').defaultTo(qb.fn.now());
            table.timestamp('updatedAt').defaultTo(qb.fn.now());
          });
        } else if (tableName === 'column_definition') {
          await qb.schema.createTable(tableName, (table) => {
            table.increments('id').primary();
            table
              .integer('tableId')
              .notNullable()
              .unsigned()
              .references('id')
              .inTable('table_definition')
              .onDelete('CASCADE');
            table.string('name').notNullable();
            table.string('type').notNullable();
            table.boolean('isPrimary').notNullable().defaultTo(false);
            table.boolean('isGenerated').notNullable().defaultTo(false);
            table.boolean('isNullable').notNullable().defaultTo(true);
            table.boolean('isSystem').notNullable().defaultTo(false);
            table.boolean('isUpdatable').notNullable().defaultTo(true);
            table.boolean('isPublished').notNullable().defaultTo(true);
            table.text('defaultValue').nullable();
            table.text('options').nullable();
            table.text('description').nullable();
            table.text('placeholder').nullable();
            table.unique(['tableId', 'name']);
            table.timestamp('createdAt').defaultTo(qb.fn.now());
            table.timestamp('updatedAt').defaultTo(qb.fn.now());
          });
        } else if (tableName === 'relation_definition') {
          await qb.schema.createTable(tableName, (table) => {
            table.increments('id').primary();
            table
              .integer('sourceTableId')
              .notNullable()
              .unsigned()
              .references('id')
              .inTable('table_definition')
              .onDelete('CASCADE');
            table
              .integer('targetTableId')
              .nullable()
              .unsigned()
              .references('id')
              .inTable('table_definition')
              .onDelete('SET NULL');
            table
              .integer('mappedById')
              .nullable()
              .unsigned()
              .references('id')
              .inTable('relation_definition')
              .onDelete('CASCADE');
            table.string('type').notNullable();
            table.string('propertyName').notNullable();
            table.boolean('isNullable').notNullable().defaultTo(true);
            table.string('onDelete').notNullable().defaultTo('SET NULL');
            table.boolean('isSystem').notNullable().defaultTo(false);
            table.boolean('isPublished').notNullable().defaultTo(true);
            table.text('description').nullable();
            table.string('junctionTableName').nullable();
            table.string('junctionSourceColumn').nullable();
            table.string('junctionTargetColumn').nullable();
            table.json('metadata').nullable();
            table.boolean('isUpdatable').notNullable().defaultTo(true);
            table.unique(['sourceTableId', 'propertyName']);
            table.unique(['mappedById']);
            table.timestamp('createdAt').defaultTo(qb.fn.now());
            table.timestamp('updatedAt').defaultTo(qb.fn.now());
          });
        }
      }
    }
  }

  async createInitMetadata(snapshot: any): Promise<void> {
    const qb = this.queryBuilderService.getConnection();
    await this.ensureCoreTables();
    await qb.transaction(async (trx) => {
      const tableNameToId: Record<string, number> = {};
      this.logger.log('Phase 1: Processing table definitions...');
      const tableEntries = Object.entries(snapshot);
      let existingTables: any[] = [];
      try {
        existingTables = await trx('table_definition').select('*');
      } catch (error: any) {
        if (error.code !== 'ER_NO_SUCH_TABLE') {
          throw error;
        }
      }
      const existingTableMap = new Map<string, any>(
        existingTables.map((t: any) => [t.name, t]),
      );
      for (const [name, defRaw] of tableEntries) {
        const def = defRaw as any;
        if (!def.name) {
          this.logger.error(
            `Table definition has no 'name' property: ${JSON.stringify(Object.keys(def))}`,
          );
          continue;
        }
        const exist = existingTableMap.get(def.name);
        if (exist) {
          tableNameToId[name] = exist.id;
          const { columns: _c, relations: _r, ...rest } = def;
          if (this.detectTableChanges(rest, exist)) {
            await trx('table_definition')
              .where('id', exist.id)
              .update({
                isSystem: rest.isSystem,
                isSingleRecord: rest.isSingleRecord || false,
                alias: rest.alias,
                description: rest.description,
                uniques: JSON.stringify(rest.uniques || []),
                indexes: JSON.stringify(rest.indexes || []),
              });
          }
        } else {
          const { columns: _c, relations: _r, ...rest } = def;
          if (!rest.name) {
            this.logger.error(
              `Table definition missing 'name' field: ${JSON.stringify(rest)}`,
            );
            continue;
          }
          const insertedId = await this.insertAndGetId(
            trx,
            'table_definition',
            {
              name: rest.name,
              isSystem: rest.isSystem || false,
              isSingleRecord: rest.isSingleRecord || false,
              alias: rest.alias,
              description: rest.description,
              uniques: JSON.stringify(rest.uniques || []),
              indexes: JSON.stringify(rest.indexes || []),
            },
          );
          tableNameToId[name] = insertedId;
        }
      }
      this.logger.log(
        `Phase 1 done: ${Object.keys(tableNameToId).length} tables`,
      );

      this.logger.log('Phase 2: Processing column definitions...');
      let allColumns: any[] = [];
      try {
        allColumns = await trx('column_definition').select('*');
      } catch (error: any) {
        if (error.code !== 'ER_NO_SUCH_TABLE') {
          throw error;
        }
      }
      const columnsByTable = new Map<number, Map<string, any>>();
      for (const col of allColumns) {
        if (!columnsByTable.has(col.tableId))
          columnsByTable.set(col.tableId, new Map());
        columnsByTable.get(col.tableId)!.set(col.name, col);
      }
      for (const [name, defRaw] of tableEntries) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;
        const existingColumnsMap = columnsByTable.get(tableId) || new Map();
        for (const snapshotCol of def.columns || []) {
          const existingCol = existingColumnsMap.get(snapshotCol.name);
          if (!existingCol) {
            await trx('column_definition').insert({
              name: snapshotCol.name,
              type: snapshotCol.type,
              isPrimary: snapshotCol.isPrimary || false,
              isGenerated: snapshotCol.isGenerated || false,
              isNullable: snapshotCol.isNullable ?? true,
              isSystem: snapshotCol.isSystem || false,
              isUpdatable: snapshotCol.isUpdatable ?? true,
              isPublished: snapshotCol.isPublished ?? true,
              defaultValue: JSON.stringify(snapshotCol.defaultValue ?? null),
              options: JSON.stringify(snapshotCol.options || null),
              description: snapshotCol.description,
              placeholder: snapshotCol.placeholder,
              tableId,
            });
          } else if (this.detectColumnChanges(snapshotCol, existingCol)) {
            await trx('column_definition')
              .where('id', existingCol.id)
              .update({
                type: snapshotCol.type,
                isNullable: snapshotCol.isNullable ?? true,
                isPrimary: snapshotCol.isPrimary || false,
                isGenerated: snapshotCol.isGenerated || false,
                defaultValue: JSON.stringify(snapshotCol.defaultValue ?? null),
                options: JSON.stringify(snapshotCol.options || null),
                isUpdatable: snapshotCol.isUpdatable ?? true,
                isPublished: snapshotCol.isPublished ?? true,
              });
          }
        }
      }
      this.logger.log('Phase 2 done');

      this.logger.log('Phase 3: Processing relation definitions...');
      let allRelations: any[] = [];
      try {
        allRelations = await trx('relation_definition').select('*');
      } catch (error: any) {
        if (error.code !== 'ER_NO_SUCH_TABLE') {
          throw error;
        }
      }
      const relationsBySourceTable = new Map<number, any[]>();
      for (const rel of allRelations) {
        if (!relationsBySourceTable.has(rel.sourceTableId))
          relationsBySourceTable.set(rel.sourceTableId, []);
        relationsBySourceTable.get(rel.sourceTableId)!.push(rel);
      }
      const relationRenameMap = loadRelationRenameMap();
      const relationIdMap = new Map<string, number>();

      const owningRelations: Array<{
        tableName: string;
        tableId: number;
        relation: any;
      }> = [];
      const inverseRelations: Array<{
        tableName: string;
        tableId: number;
        relation: any;
        owningTableName: string;
        owningPropertyName: string;
      }> = [];

      for (const [name, defRaw] of tableEntries) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;
        for (const rel of def.relations || []) {
          if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
          const targetId = tableNameToId[rel.targetTable];
          if (!targetId) continue;
          if (rel.inversePropertyName) {
            if (rel.type !== 'one-to-many') {
              owningRelations.push({ tableName: name, tableId, relation: rel });
            }
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') inverseType = 'one-to-many';
            else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
            const inverseRelation: any = {
              propertyName: rel.inversePropertyName,
              type: inverseType,
              targetTable: name,
              isSystem: rel.isSystem,
              isNullable: rel.isNullable,
              isUpdatable: rel.isUpdatable,
            };
            if (inverseType === 'many-to-many') {
              inverseRelation.junctionTableName = getJunctionTableName(
                name,
                rel.propertyName,
                rel.targetTable,
              );
            }
            inverseRelations.push({
              tableName: rel.targetTable,
              tableId: targetId,
              relation: inverseRelation,
              owningTableName: name,
              owningPropertyName: rel.propertyName,
            });
          } else {
            owningRelations.push({ tableName: name, tableId, relation: rel });
          }
        }
      }

      const upsertRelation = async (
        tableName: string,
        tableId: number,
        rel: any,
        mappedById: number | null,
        _isInverse: boolean,
      ) => {
        const targetId = tableNameToId[rel.targetTable];
        if (!targetId) return;
        const existingRels = relationsBySourceTable.get(tableId) || [];
        let existingRel = existingRels.find(
          (r: any) => r.propertyName === rel.propertyName,
        );
        if (!existingRel && relationRenameMap[tableName]?.[rel.propertyName]) {
          const oldName = relationRenameMap[tableName][rel.propertyName];
          existingRel = existingRels.find(
            (r: any) => r.propertyName === oldName,
          );
        }
        if (existingRel) {
          const junctionChanged =
            rel.type === 'many-to-many' &&
            ((rel.junctionSourceColumn &&
              rel.junctionSourceColumn !== existingRel.junctionSourceColumn) ||
              (rel.junctionTargetColumn &&
                rel.junctionTargetColumn !== existingRel.junctionTargetColumn));
          const needsUpdate =
            rel.propertyName !== existingRel.propertyName ||
            (rel.isNullable !== undefined &&
              rel.isNullable !== existingRel.isNullable) ||
            mappedById !== existingRel.mappedById ||
            (rel.type !== undefined && rel.type !== existingRel.type) ||
            (targetId !== undefined &&
              targetId !== existingRel.targetTableId) ||
            (rel.isUpdatable !== undefined &&
              rel.isUpdatable !== existingRel.isUpdatable) ||
            junctionChanged;
          if (needsUpdate) {
            const updateData: any = {
              propertyName: rel.propertyName,
              mappedById,
            };
            if (rel.isNullable !== undefined)
              updateData.isNullable = rel.isNullable;
            if (rel.isSystem !== undefined) updateData.isSystem = rel.isSystem;
            if (rel.isUpdatable !== undefined)
              updateData.isUpdatable = rel.isUpdatable;
            if (rel.type !== undefined) updateData.type = rel.type;
            if (targetId !== undefined) updateData.targetTableId = targetId;
            if (rel.type === 'many-to-many') {
              updateData.junctionTableName =
                rel.junctionTableName ||
                existingRel.junctionTableName ||
                getJunctionTableName(
                  tableName,
                  rel.propertyName,
                  rel.targetTable,
                );
              updateData.junctionSourceColumn =
                rel.junctionSourceColumn ||
                existingRel.junctionSourceColumn ||
                getForeignKeyColumnName(tableName);
              updateData.junctionTargetColumn =
                rel.junctionTargetColumn ||
                existingRel.junctionTargetColumn ||
                getForeignKeyColumnName(rel.targetTable);
            }
            await trx('relation_definition')
              .where('id', existingRel.id)
              .update(updateData);
          }
          return existingRel.id;
        } else {
          const insertData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            mappedById,
            isNullable: rel.isNullable !== false,
            isSystem: rel.isSystem || false,
            isUpdatable: rel.isUpdatable !== false,
            description: rel.description,
            sourceTableId: tableId,
            targetTableId: targetId,
          };
          if (rel.type === 'many-to-many') {
            insertData.junctionTableName =
              rel.junctionTableName ||
              getJunctionTableName(
                tableName,
                rel.propertyName,
                rel.targetTable,
              );
            insertData.junctionSourceColumn =
              rel.junctionSourceColumn || getForeignKeyColumnName(tableName);
            insertData.junctionTargetColumn =
              rel.junctionTargetColumn ||
              getForeignKeyColumnName(rel.targetTable);
          }
          const id = await this.insertAndGetId(
            trx,
            'relation_definition',
            insertData,
          );
          const newRel = { ...insertData, id };
          if (!relationsBySourceTable.has(tableId))
            relationsBySourceTable.set(tableId, []);
          relationsBySourceTable.get(tableId)!.push(newRel);
          return id;
        }
      };

      for (const { tableName, tableId, relation: rel } of owningRelations) {
        const id = await upsertRelation(tableName, tableId, rel, null, false);
        if (id) relationIdMap.set(`${tableName}.${rel.propertyName}`, id);
      }

      const processedInverseKeys = new Set<string>();
      for (const {
        tableName,
        tableId,
        relation: rel,
        owningTableName,
        owningPropertyName,
      } of inverseRelations) {
        const inverseKey = `${tableName}.${rel.propertyName}`;
        const reverseKey = `${owningTableName}.${owningPropertyName}`;
        if (processedInverseKeys.has(reverseKey)) continue;
        processedInverseKeys.add(inverseKey);
        const snapshotRelId =
          relationIdMap.get(`${owningTableName}.${owningPropertyName}`) || null;
        if (rel.type === 'many-to-one') {
          const generatedId = await upsertRelation(
            tableName,
            tableId,
            rel,
            null,
            false,
          );
          if (generatedId)
            relationIdMap.set(`${tableName}.${rel.propertyName}`, generatedId);
          if (snapshotRelId && generatedId) {
            await trx('relation_definition')
              .where('id', snapshotRelId)
              .update({ mappedById: generatedId });
          } else if (!snapshotRelId && generatedId) {
            const reverseRelType = 'one-to-many';
            const originalRel = allRelations.find(
              (r: any) =>
                r.sourceTableId === tableNameToId[owningTableName] &&
                r.propertyName === owningPropertyName &&
                r.targetTableId === tableId,
            );
            if (!originalRel) {
              await upsertRelation(
                owningTableName,
                tableNameToId[owningTableName]!,
                {
                  propertyName: owningPropertyName,
                  type: reverseRelType,
                  targetTable: tableName,
                },
                generatedId,
                true,
              );
            }
          }
        } else {
          if (rel.type === 'many-to-many' && snapshotRelId) {
            const owningRel =
              allRelations.find((r: any) => r.id === snapshotRelId) ||
              (await trx('relation_definition')
                .where('id', snapshotRelId)
                .first());
            if (owningRel) {
              rel.junctionSourceColumn = owningRel.junctionTargetColumn;
              rel.junctionTargetColumn = owningRel.junctionSourceColumn;
            }
          }
          await upsertRelation(tableName, tableId, rel, snapshotRelId, true);
        }
      }
      this.logger.log('SQL metadata sync completed');
    });
    this.logger.log('Phase 4: Syncing physical schema from metadata...');
    await this.syncPhysicalSchemaFromMetadata(snapshot);
    this.logger.log('Physical schema sync completed');
  }
  private async syncPhysicalSchemaFromMetadata(snapshot: any): Promise<void> {
    const qb = this.queryBuilderService.getConnection();
    const schemas = parseSnapshotToSchema(snapshot);

    await createAllTables(qb, schemas, this.dbType);

    for (const schema of schemas) {
      await syncTable(qb, schema, schemas);
    }

    await syncJunctionTables(qb, schemas);
  }

  private addColumnToTable(tableBuilder: any, col: any): void {
    let column: any;
    const knexType = this.getKnexColumnType(col);
    switch (knexType) {
      case 'integer':
        column = tableBuilder.integer(col.name);
        break;
      case 'bigint':
        column = tableBuilder.bigInteger(col.name);
        break;
      case 'string':
        column = tableBuilder.string(col.name, 255);
        break;
      case 'text':
        column = tableBuilder.text(col.name);
        break;
      case 'boolean':
        column = tableBuilder.boolean(col.name);
        break;
      case 'uuid':
        column = tableBuilder.uuid(col.name);
        if (col.isGenerated && col.isPrimary) {
          column = column.defaultTo(
            this.queryBuilderService.getConnection().raw('(UUID())'),
          );
        }
        break;
      case 'timestamp':
      case 'datetime':
        column = tableBuilder.timestamp(col.name);
        break;
      case 'simple-json':
        column = tableBuilder.text(col.name, 'longtext');
        break;
      case 'enum':
        column = tableBuilder.enum(col.name, col.options || []);
        break;
      case 'decimal':
        column = tableBuilder.decimal(
          col.name,
          col.precision || 10,
          col.scale || 2,
        );
        break;
      case 'float':
        column = tableBuilder.float(col.name);
        break;
      default:
        column = tableBuilder.specificType(col.name, col.type);
    }

    if (col.isPrimary) {
      column = column.primary();
    }
    if (col.isNullable === false && !col.isGenerated) {
      column = column.notNullable();
    }
    if (col.defaultValue !== null && col.defaultValue !== undefined) {
      if (col.defaultValue === 'now') {
        if (col.type === 'timestamp' || col.type === 'datetime') {
          column = column.defaultTo(
            this.queryBuilderService.getConnection().raw('CURRENT_TIMESTAMP'),
          );
        } else if (col.type === 'date') {
          column = column.defaultTo('2099-12-31');
        }
      } else {
        column = column.defaultTo(col.defaultValue);
      }
    }
    if (col.isUnique) {
      column.unique();
    }
  }

  private getKnexColumnType(col: any): string {
    const typeMap: Record<string, string> = {
      varchar: 'string',
      int: 'integer',
      bigint: 'bigint',
      text: 'text',
      boolean: 'boolean',
      uuid: 'uuid',
      timestamp: 'timestamp',
      datetime: 'datetime',
      'simple-json': 'simple-json',
      enum: 'enum',
      'array-select': 'simple-json',
      decimal: 'decimal',
      float: 'float',
      date: 'date',
      code: 'text',
      richtext: 'text',
    };
    return typeMap[col.type] || col.type;
  }
  private detectTableChanges(snapshotTable: any, existingTable: any): boolean {
    const parseJson = (val: any) => {
      if (typeof val === 'string') {
        try {
          return JSON.parse(val);
        } catch {
          return val;
        }
      }
      return val;
    };
    const snapshotIsSingleRecord = snapshotTable.isSingleRecord || false;
    if (snapshotIsSingleRecord !== (existingTable.isSingleRecord || false)) {
      return true;
    }
    const hasChanges =
      snapshotTable.isSystem !== existingTable.isSystem ||
      snapshotTable.alias !== existingTable.alias ||
      snapshotTable.description !== existingTable.description ||
      JSON.stringify(snapshotTable.uniques) !==
        JSON.stringify(parseJson(existingTable.uniques)) ||
      JSON.stringify(snapshotTable.indexes) !==
        JSON.stringify(parseJson(existingTable.indexes));
    return hasChanges;
  }
  private detectColumnChanges(snapshotCol: any, existingCol: any): boolean {
    const parseJson = (val: any) => {
      if (typeof val === 'string') {
        try {
          return JSON.parse(val);
        } catch {
          return val;
        }
      }
      return val;
    };
    const hasChanges =
      snapshotCol.type !== existingCol.type ||
      snapshotCol.isNullable !== existingCol.isNullable ||
      snapshotCol.isPrimary !== existingCol.isPrimary ||
      snapshotCol.isGenerated !== existingCol.isGenerated ||
      JSON.stringify(snapshotCol.defaultValue) !==
        JSON.stringify(parseJson(existingCol.defaultValue)) ||
      JSON.stringify(snapshotCol.options) !==
        JSON.stringify(parseJson(existingCol.options)) ||
      snapshotCol.isUpdatable !== existingCol.isUpdatable ||
      (snapshotCol.isPublished ?? true) !== (existingCol.isPublished ?? true);
    return hasChanges;
  }
}
