import { Logger } from '../../../shared/logger';
import {
  QueryBuilderService,
  getJunctionTableName,
  getForeignKeyColumnName,
} from '@enfyra/kernel';
import { DatabaseConfigService } from '../../../shared/services';
import {
  SqlSchemaMigrationService,
  parseSnapshotToSchema,
  syncTable,
  syncJunctionTables,
  createAllTables,
  supportsSqlColumnDefault,
} from '../../knex';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { SystemCoreTableResolver } from './system-core-table-resolver.service';

export class MetadataProvisionSqlService {
  private readonly logger = new Logger(MetadataProvisionSqlService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly schemaMigrationService: SqlSchemaMigrationService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly dbType: string;
  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    databaseConfigService: DatabaseConfigService;
    sqlSchemaMigrationService: SqlSchemaMigrationService;
    systemCoreTableResolver: SystemCoreTableResolver;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.databaseConfigService = deps.databaseConfigService;
    this.schemaMigrationService = deps.sqlSchemaMigrationService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
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
    const coreNames = await this.systemCoreTableResolver.getNames();
    const coreTables = [coreNames.table, coreNames.column, coreNames.relation];

    for (const tableName of coreTables) {
      const exists = await qb.schema.hasTable(tableName);
      if (!exists) {
        this.verbose(`Creating core table: ${tableName}`);
        if (tableName === coreNames.table) {
          await qb.schema.createTable(tableName, (table: any) => {
            table.increments('id').primary();
            table.string('name').notNullable().unique();
            table.boolean('isSystem').notNullable().defaultTo(false);
            table.boolean('isSingleRecord').notNullable().defaultTo(false);
            table.json('uniques').nullable();
            table.json('indexes').nullable();
            table.string('alias').nullable().unique();
            table.text('description').nullable();
            table.json('metadata').nullable();
            table.boolean('validateBody').notNullable().defaultTo(true);
            table.timestamp('createdAt').defaultTo(qb.fn.now());
            table.timestamp('updatedAt').defaultTo(qb.fn.now());
          });
        } else if (tableName === coreNames.column) {
          await qb.schema.createTable(tableName, (table: any) => {
            table.increments('id').primary();
            table
              .integer('tableId')
              .notNullable()
              .unsigned()
              .references('id')
              .inTable(coreNames.table)
              .onDelete('CASCADE');
            table.string('name').notNullable();
            table.string('type').notNullable();
            table.boolean('isPrimary').notNullable().defaultTo(false);
            table.boolean('isGenerated').notNullable().defaultTo(false);
            table.boolean('isNullable').notNullable().defaultTo(true);
            table.boolean('isSystem').notNullable().defaultTo(false);
            table.boolean('isUpdatable').notNullable().defaultTo(true);
            table.boolean('isPublished').notNullable().defaultTo(true);
            table.boolean('isEncrypted').notNullable().defaultTo(false);
            table.text('defaultValue').nullable();
            table.text('options').nullable();
            table.text('description').nullable();
            table.text('placeholder').nullable();
            table.unique(['tableId', 'name']);
            table.timestamp('createdAt').defaultTo(qb.fn.now());
            table.timestamp('updatedAt').defaultTo(qb.fn.now());
          });
        } else if (tableName === coreNames.relation) {
          await qb.schema.createTable(tableName, (table: any) => {
            table.increments('id').primary();
            table
              .integer('sourceTableId')
              .notNullable()
              .unsigned()
              .references('id')
              .inTable(coreNames.table)
              .onDelete('CASCADE');
            table
              .integer('targetTableId')
              .nullable()
              .unsigned()
              .references('id')
              .inTable(coreNames.table)
              .onDelete('SET NULL');
            table
              .integer('mappedById')
              .nullable()
              .unsigned()
              .references('id')
              .inTable(coreNames.relation)
              .onDelete('CASCADE');
            table.string('type').notNullable();
            table.string('propertyName').notNullable();
            table.boolean('isNullable').notNullable().defaultTo(true);
            table.string('onDelete').notNullable().defaultTo('SET NULL');
            table.boolean('isSystem').notNullable().defaultTo(false);
            table.boolean('isPublished').notNullable().defaultTo(true);
            table.text('description').nullable();
            table.string('foreignKeyColumn').nullable();
            table.string('referencedColumn').nullable();
            table.string('constraintName').nullable();
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
      } else if (tableName === coreNames.table) {
        await this.ensureTableDefinitionPhysicalColumns(qb, coreNames.table);
      } else if (tableName === coreNames.relation) {
        await this.ensureRelationDefinitionPhysicalColumns(
          qb,
          coreNames.relation,
        );
      }
    }
  }

  private async ensureTableDefinitionPhysicalColumns(
    qb: any,
    tableName: string,
  ): Promise<void> {
    if (!(await qb.schema.hasColumn(tableName, 'metadata'))) {
      await qb.schema.alterTable(tableName, (table: any) => {
        table.json('metadata').nullable();
      });
    }
    if (!(await qb.schema.hasColumn(tableName, 'validateBody'))) {
      await qb.schema.alterTable(tableName, (table: any) => {
        table.boolean('validateBody').notNullable().defaultTo(true);
      });
    }
  }

  private async ensureRelationDefinitionPhysicalColumns(
    qb: any,
    relationTableName: string,
  ): Promise<void> {
    const columns = ['foreignKeyColumn', 'referencedColumn', 'constraintName'];
    for (const columnName of columns) {
      const hasColumn = await qb.schema.hasColumn(
        relationTableName,
        columnName,
      );
      if (hasColumn) continue;

      await qb.schema.alterTable(relationTableName, (table: any) => {
        table.string(columnName).nullable();
      });
    }
  }

  async createInitMetadata(snapshot: any): Promise<void> {
    const qb = this.queryBuilderService.getConnection();
    await this.ensureCoreTables();
    const coreNames = await this.systemCoreTableResolver.getNames();
    let hasExistingMetadata = false;
    await qb.transaction(async (trx: any) => {
      const tableNameToId: Record<string, number> = {};
      this.verbose('Phase 1: Processing table definitions...');
      const tableEntries = Object.entries(snapshot);
      let existingTables: any[] = [];
      try {
        existingTables = await trx(coreNames.table).select('*');
      } catch (error: any) {
        if (error.code !== 'ER_NO_SUCH_TABLE') {
          throw error;
        }
      }
      hasExistingMetadata = existingTables.length > 0;
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
        } else {
          const { columns: _c, relations: _r, ...rest } = def;
          if (!rest.name) {
            this.logger.error(
              `Table definition missing 'name' field: ${JSON.stringify(rest)}`,
            );
            continue;
          }
          const insertedId = await this.insertAndGetId(trx, coreNames.table, {
            name: rest.name,
            isSystem: rest.isSystem || false,
            isSingleRecord: rest.isSingleRecord || false,
            alias: rest.alias,
            description: rest.description,
            uniques: JSON.stringify(rest.uniques || []),
            indexes: JSON.stringify(rest.indexes || []),
            metadata: JSON.stringify(rest.metadata ?? null),
            validateBody: rest.validateBody ?? true,
          });
          tableNameToId[name] = insertedId;
        }
      }
      this.verbose(`Phase 1 done: ${Object.keys(tableNameToId).length} tables`);

      this.verbose('Phase 2: Processing column definitions...');
      let allColumns: any[] = [];
      try {
        allColumns = await trx(coreNames.column).select('*');
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
            await trx(coreNames.column).insert({
              name: snapshotCol.name,
              type: snapshotCol.type,
              isPrimary: snapshotCol.isPrimary || false,
              isGenerated: snapshotCol.isGenerated || false,
              isNullable: snapshotCol.isNullable ?? true,
              isSystem: snapshotCol.isSystem || false,
              isUpdatable: snapshotCol.isUpdatable ?? true,
              isPublished: snapshotCol.isPublished ?? true,
              isEncrypted: snapshotCol.isEncrypted ?? false,
              defaultValue: JSON.stringify(snapshotCol.defaultValue ?? null),
              options: JSON.stringify(snapshotCol.options || null),
              description: snapshotCol.description,
              placeholder: snapshotCol.placeholder,
              tableId,
            });
          }
        }
      }
      this.verbose('Phase 2 done');

      this.verbose('Phase 3: Processing relation definitions...');
      let allRelations: any[] = [];
      try {
        allRelations = await trx(coreNames.relation).select('*');
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
        sourceRelation: any;
        owningTableName: string;
        owningPropertyName: string;
      }> = [];
      const generatedInverseKeys = new Set<string>();

      for (const [name, defRaw] of tableEntries) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;
        for (const rel of def.relations || []) {
          if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
          const targetId = tableNameToId[rel.targetTable];
          if (!targetId) continue;
          const currentKey = `${name}.${rel.propertyName}`;
          if (generatedInverseKeys.has(currentKey)) continue;
          if (rel.inversePropertyName) {
            if (rel.type !== 'one-to-many') {
              owningRelations.push({ tableName: name, tableId, relation: rel });
            }
            const inverseKey = `${rel.targetTable}.${rel.inversePropertyName}`;
            generatedInverseKeys.add(inverseKey);
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') inverseType = 'one-to-many';
            else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
            const declaredInverse = (
              (snapshot[rel.targetTable] as any)?.relations || []
            ).find(
              (candidate: any) =>
                candidate.propertyName === rel.inversePropertyName,
            );
            const inverseRelation: any = {
              isSystem: rel.isSystem,
              isNullable: rel.isNullable,
              isUpdatable: rel.isUpdatable,
              ...declaredInverse,
              propertyName: rel.inversePropertyName,
              type: inverseType,
              targetTable: name,
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
              sourceRelation: rel,
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
        const existingRel = existingRels.find(
          (r: any) => r.propertyName === rel.propertyName,
        );
        if (existingRel) {
          return existingRel.id;
        } else {
          const insertData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            mappedById,
            isNullable: rel.isNullable !== false,
            isSystem: rel.isSystem || false,
            isUpdatable: rel.isUpdatable !== false,
            isPublished: rel.isPublished !== false,
            onDelete: rel.onDelete || 'SET NULL',
            description: rel.description ?? null,
            sourceTableId: tableId,
            targetTableId: targetId,
          };
          if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
            insertData.foreignKeyColumn =
              rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
            insertData.referencedColumn = rel.referencedColumn || 'id';
            insertData.constraintName = rel.constraintName || null;
          }
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
            coreNames.relation,
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
        sourceRelation,
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
            await trx(coreNames.relation)
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
                  ...sourceRelation,
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
              (await trx(coreNames.relation)
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
      this.verbose('SQL metadata sync completed');
    });
    this.verbose('Phase 4: Syncing physical schema from metadata...');
    await this.syncPhysicalSchemaFromMetadata(snapshot, {
      skipJunctionTables: hasExistingMetadata,
    });
    this.verbose('Physical schema sync completed');
  }
  private async syncPhysicalSchemaFromMetadata(
    snapshot: any,
    options: { skipJunctionTables?: boolean } = {},
  ): Promise<void> {
    const qb = this.queryBuilderService.getConnection();
    const schemas = parseSnapshotToSchema(snapshot);
    const physicalSchemas = options.skipJunctionTables
      ? schemas.map((schema) => ({ ...schema, junctionTables: [] }))
      : schemas;

    await createAllTables(qb, physicalSchemas, this.dbType);

    for (const schema of physicalSchemas) {
      await syncTable(qb, schema, physicalSchemas, { additiveOnly: true });
    }

    if (!options.skipJunctionTables) {
      await syncJunctionTables(qb, physicalSchemas);
    }
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
    if (
      col.defaultValue !== null &&
      col.defaultValue !== undefined &&
      supportsSqlColumnDefault(col, this.dbType)
    ) {
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
  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
