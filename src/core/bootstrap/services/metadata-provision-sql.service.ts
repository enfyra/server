import { Injectable, Logger, Inject, forwardRef } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { SqlSchemaMigrationService } from '../../../infrastructure/knex/services/sql-schema-migration.service';
import {
  getJunctionTableName,
  getForeignKeyColumnName,
} from '../../../infrastructure/knex/utils/sql-schema-naming.util';
import { loadRelationRenameMap } from '../utils/load-relation-rename-map';
import {
  applySqlSchemaMigrations,
  hasSchemaMigrations,
  loadSchemaMigration,
} from '../../../../scripts/utils/schema-migration';
import { parseSnapshotToSchema } from '../../../../scripts/utils/sql/schema-parser';
import { syncTable } from '../../../../scripts/utils/sql/migrations';
import { syncJunctionTables } from '../../../../scripts/utils/sql/junction-tables';

@Injectable()
export class MetadataProvisionSqlService {
  private readonly logger = new Logger(MetadataProvisionSqlService.name);
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
                isSingleRecord: rest.isSingleRecord || false,
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
          this.logger.log(`Created table metadata: ${name}`);
        }
      }
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
              isPublished: snapshotCol.isPublished ?? true,
              defaultValue: JSON.stringify(snapshotCol.defaultValue ?? null),
              options: JSON.stringify(snapshotCol.options || null),
              description: snapshotCol.description,
              placeholder: snapshotCol.placeholder,
              tableId: tableId,
            });
            this.logger.log(`Added column ${snapshotCol.name} for ${name}`);
          } else {
            const hasChanges = this.detectColumnChanges(
              snapshotCol,
              existingCol,
            );
            if (hasChanges) {
              await trx('column_definition')
                .where('id', existingCol.id)
                .update({
                  type: snapshotCol.type,
                  isNullable: snapshotCol.isNullable ?? true,
                  isPrimary: snapshotCol.isPrimary || false,
                  isGenerated: snapshotCol.isGenerated || false,
                  defaultValue: JSON.stringify(
                    snapshotCol.defaultValue ?? null,
                  ),
                  options: JSON.stringify(snapshotCol.options || null),
                  isUpdatable: snapshotCol.isUpdatable ?? true,
                  isPublished: snapshotCol.isPublished ?? true,
                });
              this.logger.log(`Updated column ${snapshotCol.name} for ${name}`);
            }
          }
        }
      }
      this.logger.log('Phase 3: Processing relation definitions...');
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
      for (const [name, defRaw] of Object.entries(snapshot)) {
        const def = defRaw as any;
        const tableId = tableNameToId[name];
        if (!tableId) continue;
        for (const rel of def.relations || []) {
          if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
          const targetId = tableNameToId[rel.targetTable];
          if (!targetId) continue;
          owningRelations.push({ tableName: name, tableId, relation: rel });
          if (rel.inversePropertyName) {
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
              const junctionTableName = getJunctionTableName(
                name,
                rel.propertyName,
                rel.targetTable,
              );
              const owningSourceColumn = rel.junctionSourceColumn || getForeignKeyColumnName(name);
              const owningTargetColumn = rel.junctionTargetColumn || getForeignKeyColumnName(rel.targetTable);
              inverseRelation.junctionTableName = junctionTableName;
              inverseRelation.junctionSourceColumn = owningTargetColumn;
              inverseRelation.junctionTargetColumn = owningSourceColumn;
            }
            inverseRelations.push({
              tableName: rel.targetTable,
              tableId: targetId,
              relation: inverseRelation,
              owningTableName: name,
              owningPropertyName: rel.propertyName,
            });
          }
        }
      }
      const relationRenameMap = loadRelationRenameMap();
      const relationIdMap = new Map<string, number>();
      const upsertRelation = async (
        tableName: string,
        tableId: number,
        rel: any,
        mappedById: number | null,
        isInverse: boolean,
      ) => {
        const targetId = tableNameToId[rel.targetTable];
        if (!targetId) return;
        let existingRel = await trx('relation_definition')
          .where('sourceTableId', tableId)
          .where('propertyName', rel.propertyName)
          .first();
        if (!existingRel && relationRenameMap[tableName]?.[rel.propertyName]) {
          const oldPropertyName =
            relationRenameMap[tableName][rel.propertyName];
          existingRel = await trx('relation_definition')
            .where('sourceTableId', tableId)
            .where('propertyName', oldPropertyName)
            .first();
          if (existingRel) {
            this.logger.log(
              `Relation rename: ${tableName}.${oldPropertyName} → ${rel.propertyName}, will update`,
            );
          }
        }
        if (existingRel) {
          const junctionChanged = rel.type === 'many-to-many' && (
            (rel.junctionSourceColumn && rel.junctionSourceColumn !== existingRel.junctionSourceColumn) ||
            (rel.junctionTargetColumn && rel.junctionTargetColumn !== existingRel.junctionTargetColumn)
          );
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
            const updateData: any = {};
            updateData.propertyName = rel.propertyName;
            if (rel.isNullable !== undefined)
              updateData.isNullable = rel.isNullable;
            updateData.mappedById = mappedById;
            if (rel.isSystem !== undefined) updateData.isSystem = rel.isSystem;
            if (rel.isUpdatable !== undefined)
              updateData.isUpdatable = rel.isUpdatable;
            if (rel.type !== undefined) updateData.type = rel.type;
            if (targetId !== undefined) updateData.targetTableId = targetId;
            if (rel.type === 'many-to-many') {
              const junctionTableName =
                rel.junctionTableName ||
                existingRel.junctionTableName ||
                getJunctionTableName(
                  tableName,
                  rel.propertyName,
                  rel.targetTable,
                );
              updateData.junctionTableName = junctionTableName;
              updateData.junctionSourceColumn =
                rel.junctionSourceColumn || existingRel.junctionSourceColumn || getForeignKeyColumnName(tableName);
              updateData.junctionTargetColumn =
                rel.junctionTargetColumn || existingRel.junctionTargetColumn || getForeignKeyColumnName(rel.targetTable);
            }
            await trx('relation_definition')
              .where('id', existingRel.id)
              .update(updateData);
            this.logger.log(
              `Updated relation ${rel.propertyName} for ${tableName}${isInverse ? ' (inverse)' : ''}`,
            );
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
            const junctionTableName =
              rel.junctionTableName ||
              getJunctionTableName(
                tableName,
                rel.propertyName,
                rel.targetTable,
              );
            insertData.junctionTableName = junctionTableName;
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
          this.logger.log(
            `Added relation ${rel.propertyName} for ${tableName}${isInverse ? ' (inverse)' : ''}`,
          );
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
          }
        } else {
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
    const qb = this.queryBuilder.getConnection();
    const schemaMigration = loadSchemaMigration();
    if (hasSchemaMigrations(schemaMigration)) {
      await applySqlSchemaMigrations(qb, schemaMigration as any);
    }

    const schemas = parseSnapshotToSchema(snapshot);
    for (const schema of schemas) {
      const exists = await qb.schema.hasTable(schema.tableName);
      if (exists) {
        await syncTable(qb, schema, schemas);
      }
    }
    await syncJunctionTables(qb, schemas);
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
