import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SqlSchemaMigrationService } from '../../../infrastructure/knex/services/sql-schema-migration.service';
import { getJunctionTableName, getForeignKeyColumnName } from '../../../infrastructure/knex/utils/naming-helpers';

@Injectable()
export class CoreInitSqlService {
  private readonly logger = new Logger(CoreInitSqlService.name);
  private readonly dbType: string;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly configService: ConfigService,
    @Inject(forwardRef(() => SqlSchemaMigrationService))
    private readonly schemaMigrationService: SqlSchemaMigrationService,
  ) {
    this.dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
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

  async createInitMetadata(snapshot: any): Promise<void> {
    const qb = this.queryBuilder.getConnection();

    await qb.transaction(async (trx) => {
      const tableNameToId: Record<string, number> = {};

      // Phase 1: Insert/Update table definitions
      this.logger.log('Phase 1: Processing table definitions...');
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;

        const exist = await trx('table_definition')
          .where('name', def.name)
          .first();

        if (exist) {
          tableNameToId[name] = exist.id;

          const { columns, relations, ...rest } = def;
          const hasTableChanges = this.detectTableChanges(rest, exist);

          if (hasTableChanges) {
            await trx('table_definition')
              .where('id', exist.id)
              .update({
                isSystem: rest.isSystem,
                isSingleRecord: name === 'setting_definition' ? true : (rest.isSingleRecord || false),
                alias: rest.alias,
                description: rest.description,
                uniques: JSON.stringify(rest.uniques || []),
                indexes: JSON.stringify(rest.indexes || []),
              });
            this.logger.log(`Updated table ${name}`);
          } else {
            this.logger.log(`Skip ${name}, no changes`);
          }
        } else {
          const { columns, relations, ...rest } = def;

          const insertedId = await this.insertAndGetId(trx, 'table_definition', {
            name: rest.name,
            isSystem: rest.isSystem || false,
            isSingleRecord: name === 'setting_definition' ? true : (rest.isSingleRecord || false),
            alias: rest.alias,
            description: rest.description,
            uniques: JSON.stringify(rest.uniques || []),
            indexes: JSON.stringify(rest.indexes || []),
          });

          tableNameToId[name] = insertedId;
          this.logger.log(`Created table metadata: ${name}`);
        }
      }

      // Phase 2: Process column definitions
      this.logger.log('Phase 2: Processing column definitions...');
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;

        const existingColumns = await trx('column_definition')
          .where('tableId', tableId)
          .select('*');

        const existingColumnsMap = new Map(
          existingColumns.map((col) => [col.name, col]),
        );

        for (const snapshotCol of def.columns || []) {
          const existingCol = existingColumnsMap.get(snapshotCol.name) as any;

          if (!existingCol) {
            await trx('column_definition').insert({
              name: snapshotCol.name,
              type: snapshotCol.type,
              isPrimary: snapshotCol.isPrimary || false,
              isGenerated: snapshotCol.isGenerated || false,
              isNullable: snapshotCol.isNullable ?? true,
              isSystem: snapshotCol.isSystem || false,
              isUpdatable: snapshotCol.isUpdatable ?? true,
              isHidden: snapshotCol.isHidden || false,
              defaultValue: JSON.stringify(snapshotCol.defaultValue ?? null),
              options: JSON.stringify(snapshotCol.options || null),
              description: snapshotCol.description,
              placeholder: snapshotCol.placeholder,
              tableId: tableId,
            });
            this.logger.log(`Added column ${snapshotCol.name} for ${name}`);
          } else {
            const hasChanges = this.detectColumnChanges(snapshotCol, existingCol);
            if (hasChanges) {
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
                  isHidden: snapshotCol.isHidden || false,
                });
              this.logger.log(`Updated column ${snapshotCol.name} for ${name}`);
            }
          }
        }

        const snapshotColumnNames = new Set((def.columns || []).map(col => col.name));
        const columnsToRemove = existingColumns.filter(col =>
          !snapshotColumnNames.has(col.name)
        );

        for (const colToRemove of columnsToRemove) {
          await trx('column_definition').where('id', colToRemove.id).delete();
          this.logger.log(`Removed column ${colToRemove.name} from ${name}`);
        }
      }

      // Phase 3: Process relation definitions + auto-generate inverse relations
      this.logger.log('Phase 3: Processing relation definitions...');

      const allRelationsToProcess: Array<{
        tableName: string;
        tableId: number;
        relation: any;
        isInverse: boolean;
      }> = [];

      // First pass: collect direct relations from snapshot
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;

        for (const rel of def.relations || []) {
          if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
          const targetId = tableNameToId[rel.targetTable];
          if (!targetId) continue;

          allRelationsToProcess.push({
            tableName: name,
            tableId,
            relation: rel,
            isInverse: false,
          });

          // Auto-generate inverse relation if inversePropertyName exists
          if (rel.inversePropertyName) {
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') {
              inverseType = 'one-to-many';
            } else if (rel.type === 'one-to-many') {
              inverseType = 'many-to-one';
            }

            const inverseRelation: any = {
              propertyName: rel.inversePropertyName,
              type: inverseType,
              targetTable: name,
              inversePropertyName: rel.propertyName,
              isSystem: rel.isSystem,
              isNullable: rel.isNullable,
            };

            if (inverseType === 'many-to-many') {
              const junctionTableName = getJunctionTableName(name, rel.propertyName, rel.targetTable);
              inverseRelation.junctionTableName = junctionTableName;
              inverseRelation.junctionSourceColumn = getForeignKeyColumnName(rel.targetTable);
              inverseRelation.junctionTargetColumn = getForeignKeyColumnName(name);
            }

            allRelationsToProcess.push({
              tableName: rel.targetTable,
              tableId: targetId,
              relation: inverseRelation,
              isInverse: true,
            });
          }
        }
      }

      // Process all relations (including inverse)
      for (const { tableName, tableId, relation: rel, isInverse } of allRelationsToProcess) {
        const targetId = tableNameToId[rel.targetTable];
        if (!targetId) continue;

        const existingRel = await trx('relation_definition')
          .where('sourceTableId', tableId)
          .where('propertyName', rel.propertyName)
          .first();

        if (existingRel) {
          const needsUpdate =
            (rel.isNullable !== undefined && rel.isNullable !== existingRel.isNullable) ||
            (rel.inversePropertyName !== existingRel.inversePropertyName) ||
            (rel.type !== undefined && rel.type !== existingRel.type) ||
            (targetId !== undefined && targetId !== existingRel.targetTableId);

          if (needsUpdate) {
            const updateData: any = {};
            if (rel.isNullable !== undefined) updateData.isNullable = rel.isNullable;
            // Always update inversePropertyName (even if undefined/null) to handle removal
            updateData.inversePropertyName = rel.inversePropertyName || null;
            if (rel.isSystem !== undefined) updateData.isSystem = rel.isSystem;
            if (rel.type !== undefined) updateData.type = rel.type;
            if (targetId !== undefined) updateData.targetTableId = targetId;

            if (rel.type === 'many-to-many') {
              const junctionTableName = rel.junctionTableName ||
                getJunctionTableName(tableName, rel.propertyName, rel.targetTable);
              updateData.junctionTableName = junctionTableName;
              updateData.junctionSourceColumn = rel.junctionSourceColumn ||
                getForeignKeyColumnName(tableName);
              updateData.junctionTargetColumn = rel.junctionTargetColumn ||
                getForeignKeyColumnName(rel.targetTable);
            }

            await trx('relation_definition')
              .where('id', existingRel.id)
              .update(updateData);

            this.logger.log(`Updated relation ${rel.propertyName} for ${tableName}${isInverse ? ' (inverse)' : ''}`);
          }
        } else {
          const insertData: any = {
            propertyName: rel.propertyName,
            type: rel.type,
            inversePropertyName: rel.inversePropertyName,
            isNullable: rel.isNullable !== false,
            isSystem: rel.isSystem || false,
            description: rel.description,
            sourceTableId: tableId,
            targetTableId: targetId,
          };

          if (rel.type === 'many-to-many') {
            const junctionTableName = rel.junctionTableName ||
              getJunctionTableName(tableName, rel.propertyName, rel.targetTable);
            insertData.junctionTableName = junctionTableName;
            insertData.junctionSourceColumn = rel.junctionSourceColumn ||
              getForeignKeyColumnName(tableName);
            insertData.junctionTargetColumn = rel.junctionTargetColumn ||
              getForeignKeyColumnName(rel.targetTable);
          }

          await trx('relation_definition').insert(insertData);
          this.logger.log(`Added relation ${rel.propertyName} for ${tableName}${isInverse ? ' (inverse)' : ''}`);
        }
      }

      // Clean up orphaned relations
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;

        const snapshotRelationKeys = new Set(
          (def.relations || [])
            .filter(rel => !!rel.propertyName)
            .map(rel => rel.propertyName)
        );

        for (const [otherName, otherDefRaw] of Object.entries(snapshot)) {
          const otherDef = otherDefRaw as any;
          for (const rel of otherDef.relations || []) {
            if (rel.inversePropertyName && rel.targetTable === name) {
              snapshotRelationKeys.add(rel.inversePropertyName);
            }
          }
        }

        const existingRelations = await trx('relation_definition')
          .where('sourceTableId', tableId)
          .select('*');

        const relationsToRemove = existingRelations.filter(
          rel => !snapshotRelationKeys.has(rel.propertyName)
        );

        for (const relToRemove of relationsToRemove) {
          await trx('relation_definition').where('id', relToRemove.id).delete();
          this.logger.log(`Removed relation ${relToRemove.propertyName} from ${name}`);
        }
      }

      this.logger.log('SQL metadata sync completed');
    });

    // Phase 4: Sync physical database schema from metadata changes
    this.logger.log('Phase 4: Syncing physical schema from metadata...');
    await this.syncPhysicalSchemaFromMetadata(snapshot);
    this.logger.log('Physical schema sync completed');
  }

  private async syncPhysicalSchemaFromMetadata(snapshot: any): Promise<void> {
    const qb = this.queryBuilder.getConnection();

    // For each table in snapshot, check if columns need physical ALTER TABLE
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;

      // Check if physical table exists
      const tableExists = await qb.schema.hasTable(def.name);
      if (!tableExists) {
        this.logger.log(`Physical table ${def.name} does not exist, skipping schema sync`);
        continue;
      }

      // Get metadata from DB
      const tableRecord = await qb('table_definition').where('name', def.name).first();
      if (!tableRecord) continue;

      const columns = await qb('column_definition').where('tableId', tableRecord.id).select('*');

      // For each column, check if type changed and needs ALTER TABLE
      for (const col of columns) {
        try {
          const columnInfo = await qb(def.name).columnInfo();
          const physicalCol = columnInfo[col.name];

          if (!physicalCol) {
            this.logger.log(`Column ${col.name} not found in ${def.name}, skipping`);
            continue;
          }

          // Check if enum type with options
          if (col.type === 'enum' && col.options) {
            const options = typeof col.options === 'string' ? JSON.parse(col.options) : col.options;

            if (Array.isArray(options) && options.length > 0) {
              const enumValues = options.map(v => `'${v}'`).join(', ');

              // Build ALTER TABLE query based on database type
              let alterSQL: string;
              if (this.dbType === 'mysql') {
                const nullable = col.isNullable ? 'NULL' : 'NOT NULL';
                let defaultClause = '';
                if (col.defaultValue !== undefined && col.defaultValue !== null) {
                  if (col.type === 'boolean') {
                    let defVal: any = col.defaultValue;
                    if (typeof defVal === 'number') defVal = defVal === 1;
                    else if (typeof defVal === 'string') {
                      const t = defVal.trim().toLowerCase();
                      if (t === '1' || t === 'true') defVal = true;
                      else if (t === '0' || t === 'false') defVal = false;
                    }
                    defaultClause = ` DEFAULT ${defVal ? 1 : 0}`;
                  } else {
                    defaultClause = ` DEFAULT '${col.defaultValue}'`;
                  }
                }
                alterSQL = `ALTER TABLE \`${def.name}\` MODIFY COLUMN \`${col.name}\` ENUM(${enumValues}) ${nullable}${defaultClause}`;
              } else if (this.dbType === 'postgres') {
                // PostgreSQL needs to drop and recreate the type
                alterSQL = `ALTER TABLE "${def.name}" ALTER COLUMN "${col.name}" TYPE VARCHAR(255)`;
              } else {
                continue;
              }

              this.logger.log(`Running ALTER TABLE for ${def.name}.${col.name}: ${alterSQL}`);
              await qb.raw(alterSQL);
              this.logger.log(`✅ Updated ${def.name}.${col.name} to enum with values: ${enumValues}`);
            }
          }
        } catch (error) {
          this.logger.warn(`Failed to ALTER column ${col.name} in ${def.name}: ${error.message}`);
        }
      }
    }
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

    const snapshotIsSingleRecord = snapshotTable.name === 'setting_definition' ? true : (snapshotTable.isSingleRecord || false);
    if (snapshotIsSingleRecord !== (existingTable.isSingleRecord || false)) {
      return true;
    }

    const hasChanges =
      snapshotTable.isSystem !== existingTable.isSystem ||
      snapshotTable.alias !== existingTable.alias ||
      snapshotTable.description !== existingTable.description ||
      JSON.stringify(snapshotTable.uniques) !== JSON.stringify(parseJson(existingTable.uniques)) ||
      JSON.stringify(snapshotTable.indexes) !== JSON.stringify(parseJson(existingTable.indexes));

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
      JSON.stringify(snapshotCol.defaultValue) !== JSON.stringify(parseJson(existingCol.defaultValue)) ||
      JSON.stringify(snapshotCol.options) !== JSON.stringify(parseJson(existingCol.options)) ||
      snapshotCol.isUpdatable !== existingCol.isUpdatable;

    return hasChanges;
  }
}
