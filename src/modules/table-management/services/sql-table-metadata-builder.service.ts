import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { TCreateTableBody } from '../types/table-handler.types';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../../infrastructure/knex/utils/sql-schema-naming.util';

export class SqlTableMetadataBuilderService {
  private readonly logger = new Logger(SqlTableMetadataBuilderService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
  }

  async getFullTableMetadataInTransaction(
    trx: any,
    tableId: string | number,
  ): Promise<any> {
    const table = await trx('table_definition').where({ id: tableId }).first();
    if (!table) return null;
    if (table.uniques && typeof table.uniques === 'string') {
      try {
        table.uniques = JSON.parse(table.uniques);
      } catch (e: any) {
        table.uniques = [];
      }
    }
    if (table.indexes && typeof table.indexes === 'string') {
      try {
        table.indexes = JSON.parse(table.indexes);
      } catch (e: any) {
        table.indexes = [];
      }
    }
    table.columns = await trx('column_definition')
      .where({ tableId })
      .select('*');
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e: any) {}
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e: any) {}
      }
    }
    const relations = await trx('relation_definition')
      .where({ 'relation_definition.sourceTableId': tableId })
      .leftJoin(
        'table_definition',
        'relation_definition.targetTableId',
        'table_definition.id',
      )
      .select(
        'relation_definition.*',
        'table_definition.name as targetTableName',
      );
    for (const rel of relations) {
      rel.sourceTableName = table.name;
      if (!rel.targetTableName && rel.targetTableId) {
        const targetTable = await trx('table_definition')
          .where({ id: rel.targetTableId })
          .first();
        if (targetTable) {
          rel.targetTableName = targetTable.name;
        } else {
          this.logger.error(
            `Relation ${rel.propertyName} (${rel.type}) has invalid targetTableId: ${rel.targetTableId} - table not found`,
          );
        }
      }
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        if (!rel.targetTableName) {
          throw new Error(
            `Relation '${rel.propertyName}' (${rel.type}) from table '${table.name}' has invalid targetTableId: ${rel.targetTableId}. Target table not found.`,
          );
        }
        rel.foreignKeyColumn = getForeignKeyColumnName(rel.propertyName);
      }
    }
    table.relations = relations;
    return table;
  }

  constructAfterMetadata(
    exists: any,
    body: TCreateTableBody,
    oldMetadata: any,
    targetTablesMap: Map<number, string>,
  ): any {
    const metadata: any = {
      name: body.name ?? exists.name,
      uniques: body.uniques ?? oldMetadata?.uniques,
      indexes: body.indexes ?? oldMetadata?.indexes,
    };

    metadata.columns = (body.columns ?? oldMetadata?.columns ?? []).map(
      (col: any) => ({
        id: col.id,
        name: col.name,
        type: col.type,
        isPrimary: col.isPrimary || false,
        isGenerated: col.isGenerated || false,
        isNullable: col.isNullable ?? true,
        isSystem: col.isSystem || false,
        isUpdatable: col.isUpdatable ?? true,
        isPublished: col.isPublished ?? true,
        defaultValue: col.defaultValue ?? null,
        tableId: exists.id,
      }),
    );

    metadata.relations = (body.relations ?? oldMetadata?.relations ?? []).map(
      (rel: any) => {
        const targetTableId =
          typeof rel.targetTable === 'object'
            ? rel.targetTable.id
            : rel.targetTable;
        const targetTableName =
          targetTablesMap.get(targetTableId) ??
          oldMetadata?.relations?.find((r: any) => r.id === rel.id)
            ?.targetTableName ??
          '';
        const relation: any = {
          id: rel.id,
          propertyName: rel.propertyName,
          type: rel.type,
          targetTableId,
          targetTableName,
          sourceTableName: exists.name,
          mappedBy: rel.mappedBy || null,
          mappedById: null,
          isNullable: rel.isNullable ?? true,
          isSystem: rel.isSystem || false,
          isUpdatable: rel.isUpdatable ?? true,
          isPublished: rel.isPublished ?? true,
        };

        if (['many-to-one', 'one-to-one'].includes(rel.type)) {
          relation.foreignKeyColumn = getForeignKeyColumnName(rel.propertyName);
        }

        if (rel.type === 'many-to-many') {
          if (rel.id) {
            const existingRel = oldMetadata?.relations?.find(
              (r: any) => r.id === rel.id,
            );
            if (existingRel?.junctionTableName) {
              relation.junctionTableName = existingRel.junctionTableName;
              relation.junctionSourceColumn = existingRel.junctionSourceColumn;
              relation.junctionTargetColumn = existingRel.junctionTargetColumn;
            }
          }
          if (!relation.junctionTableName && targetTableName) {
            relation.junctionTableName = getJunctionTableName(
              exists.name,
              rel.propertyName,
              targetTableName,
            );
            const { sourceColumn, targetColumn } = getJunctionColumnNames(
              exists.name,
              rel.propertyName,
              targetTableName,
            );
            relation.junctionSourceColumn = sourceColumn;
            relation.junctionTargetColumn = targetColumn;
          }
        }

        return relation;
      },
    );

    return metadata;
  }
}
