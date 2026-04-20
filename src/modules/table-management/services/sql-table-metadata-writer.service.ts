import { Logger } from '../../../shared/logger';
import { TCreateTableBody } from '../types/table-handler.types';
import { getDeletedIds } from '../utils/get-deleted-ids';
import {
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../../infrastructure/knex/utils/sql-schema-naming.util';
import { ValidationException } from '../../../core/exceptions/custom-exceptions';

export class SqlTableMetadataWriterService {
  private readonly logger = new Logger(SqlTableMetadataWriterService.name);

  async writeTableMetadataUpdates(
    queryRunner: any,
    id: string | number,
    body: TCreateTableBody,
    exists: any,
    affectedTableNames: Set<string>,
  ): Promise<void> {
    await queryRunner('table_definition')
      .where({ id })
      .update({
        name: body.name,
        alias: body.alias,
        description: body.description,
        uniques: body.uniques ? JSON.stringify(body.uniques) : exists.uniques,
        indexes: body.indexes ? JSON.stringify(body.indexes) : exists.indexes,
        ...(body.isSingleRecord !== undefined && {
          isSingleRecord: body.isSingleRecord,
        }),
        ...(body.validateBody !== undefined && {
          validateBody: body.validateBody,
        }),
      });

    if (body.columns) {
      const existingColumns = await queryRunner('column_definition')
        .where({ tableId: id })
        .select('id');
      const deletedColumnIds = getDeletedIds(existingColumns, body.columns);
      if (deletedColumnIds.length > 0) {
        await queryRunner('column_definition')
          .whereIn('id', deletedColumnIds)
          .delete();
      }
      for (const col of body.columns) {
        if (
          col.name === 'id' ||
          col.name === 'createdAt' ||
          col.name === 'updatedAt'
        ) {
          continue;
        }
        const columnData = {
          name: col.name,
          type: col.type,
          isPrimary: col.isPrimary || false,
          isGenerated: col.isGenerated || false,
          isNullable: col.isNullable ?? true,
          isSystem: col.isSystem || false,
          isUpdatable: col.isUpdatable ?? true,
          isPublished: col.isPublished ?? true,
          defaultValue:
            col.defaultValue !== undefined
              ? JSON.stringify(col.defaultValue)
              : null,
          options:
            col.options !== undefined ? JSON.stringify(col.options) : null,
          description: col.description,
          placeholder: col.placeholder,
          metadata:
            col.metadata !== undefined ? JSON.stringify(col.metadata) : null,
          tableId: id,
        };
        if (col.id) {
          await queryRunner('column_definition')
            .where({ id: col.id })
            .update(columnData);
        } else {
          await queryRunner('column_definition').insert(columnData);
        }
      }
    }

    if (body.relations) {
      const existingRelations = await queryRunner('relation_definition')
        .where({ sourceTableId: id })
        .select('id');
      const deletedRelationIds = getDeletedIds(
        existingRelations,
        body.relations,
      );
      if (deletedRelationIds.length > 0) {
        const inverseRelations = await queryRunner('relation_definition')
          .whereIn('mappedById', deletedRelationIds)
          .select('sourceTableId');
        for (const inv of inverseRelations) {
          const invTable = await queryRunner('table_definition')
            .where({ id: inv.sourceTableId })
            .select('name')
            .first();
          if (invTable?.name) affectedTableNames.add(invTable.name);
        }
        await queryRunner('relation_definition')
          .whereIn('mappedById', deletedRelationIds)
          .delete();
        await queryRunner('relation_definition')
          .whereIn('id', deletedRelationIds)
          .delete();
      }
      for (const rel of body.relations) {
        if (!rel.id) continue;
        const existingRel = await queryRunner('relation_definition')
          .where({ id: rel.id })
          .first();
        if (existingRel?.mappedById) {
          const changed =
            (rel.type !== undefined && rel.type !== existingRel.type) ||
            (rel.targetTable !== undefined &&
              (typeof rel.targetTable === 'object'
                ? rel.targetTable.id
                : rel.targetTable) !== existingRel.targetTableId) ||
            (rel.mappedBy !== undefined && rel.mappedBy !== null) ||
            (rel.isNullable !== undefined &&
              rel.isNullable !== existingRel.isNullable);
          if (changed) {
            throw new ValidationException(
              `Inverse relation '${existingRel.propertyName}' can only have its propertyName modified`,
              { relationName: existingRel.propertyName },
            );
          }
        }
      }
      const targetTableIds = body.relations
        .map((rel: any) =>
          typeof rel.targetTable === 'object'
            ? rel.targetTable.id
            : rel.targetTable,
        )
        .filter((tid: any) => tid != null);
      const targetTablesMap = new Map<number, string>();
      if (targetTableIds.length > 0) {
        const targetTables = await queryRunner('table_definition')
          .select('id', 'name')
          .whereIn('id', targetTableIds);
        for (const table of targetTables) {
          targetTablesMap.set(table.id, table.name);
        }
      }
      for (const rel of body.relations) {
        const targetTableId =
          typeof rel.targetTable === 'object'
            ? rel.targetTable.id
            : rel.targetTable;
        if (rel.id) {
          const existingRel = await queryRunner('relation_definition')
            .where({ id: rel.id })
            .first();
          if (existingRel && existingRel.type !== rel.type) {
            throw new Error(
              `Cannot change relation type from '${existingRel.type}' to '${rel.type}' for property '${rel.propertyName}'. ` +
                `Please delete the old relation and create a new one.`,
            );
          }
        }
        let updateMappedById: number | null = null;
        if (rel.mappedBy) {
          const owningRel = await queryRunner('relation_definition')
            .where({ sourceTableId: targetTableId, propertyName: rel.mappedBy })
            .select('id')
            .first();
          updateMappedById = owningRel?.id || null;
        }
        const relationData: any = {
          propertyName: rel.propertyName,
          type: rel.type,
          targetTableId,
          mappedById: updateMappedById,
          isNullable: rel.isNullable ?? true,
          isSystem: rel.isSystem || false,
          isUpdatable: rel.isUpdatable ?? true,
          isPublished: rel.isPublished ?? true,
          description: rel.description,
          sourceTableId: id,
        };
        if (rel.type === 'many-to-many') {
          const targetTableName = targetTablesMap.get(targetTableId);
          if (!targetTableName) {
            throw new Error(`Target table with ID ${targetTableId} not found`);
          }
          if (rel.id) {
            const existingRel = await queryRunner('relation_definition')
              .where({ id: rel.id })
              .first();
            if (existingRel && existingRel.junctionTableName) {
              relationData.junctionTableName = existingRel.junctionTableName;
              relationData.junctionSourceColumn =
                existingRel.junctionSourceColumn;
              relationData.junctionTargetColumn =
                existingRel.junctionTargetColumn;
            } else {
              const junctionTableName = getJunctionTableName(
                exists.name,
                rel.propertyName,
                targetTableName,
              );
              const { sourceColumn, targetColumn } = getJunctionColumnNames(
                exists.name,
                rel.propertyName,
                targetTableName,
              );
              relationData.junctionTableName = junctionTableName;
              relationData.junctionSourceColumn = sourceColumn;
              relationData.junctionTargetColumn = targetColumn;
            }
          } else {
            const junctionTableName = getJunctionTableName(
              exists.name,
              rel.propertyName,
              targetTableName,
            );
            const { sourceColumn, targetColumn } = getJunctionColumnNames(
              exists.name,
              rel.propertyName,
              targetTableName,
            );
            relationData.junctionTableName = junctionTableName;
            relationData.junctionSourceColumn = sourceColumn;
            relationData.junctionTargetColumn = targetColumn;
          }
        } else {
          relationData.junctionTableName = null;
          relationData.junctionSourceColumn = null;
          relationData.junctionTargetColumn = null;
        }
        if (rel.id) {
          await queryRunner('relation_definition')
            .where({ id: rel.id })
            .update(relationData);
        } else {
          await queryRunner('relation_definition').insert(relationData);
        }
        if (rel.inversePropertyName && !rel.id) {
          if (rel.mappedBy) {
            throw new ValidationException(
              `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
              { relationName: rel.propertyName },
            );
          }
          const existingOnTarget = await queryRunner('relation_definition')
            .where({
              sourceTableId: targetTableId,
              propertyName: rel.inversePropertyName,
            })
            .first();
          if (existingOnTarget) {
            throw new ValidationException(
              `Cannot create inverse '${rel.inversePropertyName}' on target table: property name already exists`,
              { relationName: rel.inversePropertyName },
            );
          }
          const insertedRel = await queryRunner('relation_definition')
            .where({ sourceTableId: id, propertyName: rel.propertyName })
            .first();
          if (insertedRel) {
            const existingInverse = await queryRunner('relation_definition')
              .where({ mappedById: insertedRel.id })
              .first();
            if (existingInverse) {
              throw new ValidationException(
                `Relation '${rel.propertyName}' already has an inverse '${existingInverse.propertyName}'`,
                { relationName: rel.propertyName },
              );
            }
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') inverseType = 'one-to-many';
            else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
            const inverseData: any = {
              propertyName: rel.inversePropertyName,
              type: inverseType,
              sourceTableId: targetTableId,
              targetTableId: id,
              mappedById: insertedRel.id,
              isNullable: rel.isNullable ?? true,
              isSystem: false,
              isUpdatable: rel.isUpdatable ?? true,
              isPublished: rel.isPublished ?? true,
            };
            if (inverseType === 'many-to-many') {
              inverseData.junctionTableName = relationData.junctionTableName;
              inverseData.junctionSourceColumn =
                relationData.junctionTargetColumn;
              inverseData.junctionTargetColumn =
                relationData.junctionSourceColumn;
            }
            await queryRunner('relation_definition').insert(inverseData);
            const targetName = targetTablesMap.get(targetTableId);
            if (targetName) affectedTableNames.add(targetName);
            this.logger.log(
              `Auto-created inverse relation '${rel.inversePropertyName}'`,
            );
          }
        }
      }
    }
  }
}
