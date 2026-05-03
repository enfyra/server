import { Logger } from '../../../shared/logger';
import { TCreateTableBody } from '../types/table-handler.types';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { getForeignKeyColumnName } from '@enfyra/kernel';
import { DatabaseConfigService } from '../../../shared/services';
import { ValidationException } from '../../../domain/exceptions';
import {
  getRelationMappedByProperty,
  getRelationTargetTableId,
  relationTargetTableMapKey,
} from '../utils/relation-target-id.util';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';

export class SqlTableMetadataWriterService {
  private readonly logger = new Logger(SqlTableMetadataWriterService.name);

  async writeTableMetadataUpdates(
    queryRunner: any,
    id: string | number,
    body: TCreateTableBody,
    exists: any,
    affectedTableNames: Set<string>,
  ): Promise<void> {
    const allowedConstraintFields = this.getAllowedConstraintFields(body);
    const uniques =
      body.uniques && allowedConstraintFields
        ? this.filterConstraintGroups(body.uniques, allowedConstraintFields)
        : body.uniques;
    const indexes =
      body.indexes && allowedConstraintFields
        ? this.filterConstraintGroups(body.indexes, allowedConstraintFields)
        : body.indexes;

    await queryRunner('table_definition')
      .where({ id })
      .update({
        name: body.name,
        alias: body.alias,
        description: body.description,
        uniques: uniques ? JSON.stringify(uniques) : exists.uniques,
        indexes: indexes ? JSON.stringify(indexes) : exists.indexes,
        ...(body.isSingleRecord !== undefined && {
          isSingleRecord: body.isSingleRecord,
        }),
        ...(body.validateBody !== undefined && {
          validateBody: body.validateBody,
        }),
      });

    if (body.columns) {
      const ignoredFkColumns = await this.getInverseRelationFkColumnNames(
        queryRunner,
        id,
        body.relations || [],
      );
      const bodyColumns = body.columns.filter(
        (col: any) => !ignoredFkColumns.has(col.name),
      );
      const existingColumns = await queryRunner('column_definition')
        .where({ tableId: id })
        .select('id');
      const deletedColumnIds = getDeletedIds(existingColumns, bodyColumns);
      if (deletedColumnIds.length > 0) {
        await queryRunner('column_definition')
          .whereIn('id', deletedColumnIds)
          .delete();
      }
      for (const col of bodyColumns) {
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
        let columnId: number | string;
        if (col.id) {
          await queryRunner('column_definition')
            .where({ id: col.id })
            .update(columnData);
          columnId = col.id;
        } else {
          columnId = await this.insertAndGetId(
            queryRunner,
            'column_definition',
            columnData,
          );
        }
        await this.writeNestedRules(queryRunner, {
          rules: (col as any).rules,
          fkField: 'columnId',
          fkValue: columnId,
        });
        await this.writeNestedFieldPermissions(queryRunner, {
          permissions: (col as any).fieldPermissions,
          subjectFk: 'columnId',
          subjectFkValue: columnId,
        });
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
      const targetTableIds = body.relations
        .map((rel: any) => getRelationTargetTableId(rel))
        .filter((tid: any) => tid != null);
      const targetTablesMap = new Map<string, string>();
      if (targetTableIds.length > 0) {
        const targetTables = await queryRunner('table_definition')
          .select('id', 'name')
          .whereIn('id', targetTableIds);
        for (const table of targetTables) {
          targetTablesMap.set(relationTargetTableMapKey(table.id), table.name);
        }
      }
      for (const rel of body.relations) {
        const targetTableId = getRelationTargetTableId(rel);
        const mappedByProperty = getRelationMappedByProperty(rel);
        const targetTableName = targetTablesMap.get(
          relationTargetTableMapKey(targetTableId),
        );
        if (targetTableName) affectedTableNames.add(targetTableName);
        const existingRel = rel.id
          ? await queryRunner('relation_definition').where({ id: rel.id }).first()
          : null;
        if (rel.id) {
          if (existingRel && existingRel.type !== rel.type) {
            throw new Error(
              `Cannot change relation type from '${existingRel.type}' to '${rel.type}' for property '${rel.propertyName}'. ` +
                `Please delete the old relation and create a new one.`,
            );
          }
        }
        let updateMappedById: number | null = null;
        let mappedByRelation: any = null;
        if (mappedByProperty) {
          mappedByRelation = await queryRunner('relation_definition')
            .where({ sourceTableId: targetTableId, propertyName: mappedByProperty })
            .select(
              'id',
              'propertyName',
              'foreignKeyColumn',
              'referencedColumn',
              'constraintName',
            )
            .first();
          updateMappedById = mappedByRelation?.id || null;
        } else if (rel.id && existingRel) {
          updateMappedById = existingRel.mappedById || null;
          if (updateMappedById) {
            mappedByRelation = await queryRunner('relation_definition')
              .where({ id: updateMappedById })
              .select(
                'id',
                'propertyName',
                'foreignKeyColumn',
                'referencedColumn',
                'constraintName',
              )
              .first();
          }
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
          onDelete: rel.onDelete || 'SET NULL',
          description: rel.description,
          sourceTableId: id,
        };
        const relationOwnsForeignKey =
          (rel.type === 'many-to-one' || rel.type === 'one-to-one') &&
          !rel.mappedBy &&
          !updateMappedById;
        if (relationOwnsForeignKey) {
          relationData.foreignKeyColumn =
            existingRel?.foreignKeyColumn ||
            rel.foreignKeyColumn ||
            getForeignKeyColumnName(rel.propertyName);
          relationData.referencedColumn =
            existingRel?.referencedColumn || rel.referencedColumn || 'id';
          relationData.constraintName =
            existingRel?.constraintName || rel.constraintName || null;
        } else if (updateMappedById && mappedByRelation) {
          relationData.foreignKeyColumn =
            mappedByRelation.foreignKeyColumn ||
            getForeignKeyColumnName(mappedByRelation.propertyName);
          relationData.referencedColumn =
            mappedByRelation.referencedColumn || 'id';
          relationData.constraintName = mappedByRelation.constraintName || null;
        } else if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
          relationData.foreignKeyColumn = null;
          relationData.referencedColumn = null;
          relationData.constraintName = null;
        }
        if (rel.type === 'many-to-many') {
          if (!targetTableName) {
            throw new Error(`Target table with ID ${targetTableId} not found`);
          }
          if (rel.id) {
            if (existingRel && existingRel.junctionTableName) {
              relationData.junctionTableName = existingRel.junctionTableName;
              relationData.junctionSourceColumn =
                existingRel.junctionSourceColumn;
              relationData.junctionTargetColumn =
                existingRel.junctionTargetColumn;
            } else {
              const junction = getSqlJunctionPhysicalNames({
                sourceTable: exists.name,
                propertyName: rel.propertyName,
                targetTable: targetTableName,
              });
              relationData.junctionTableName = junction.junctionTableName;
              relationData.junctionSourceColumn = junction.junctionSourceColumn;
              relationData.junctionTargetColumn = junction.junctionTargetColumn;
            }
          } else if (updateMappedById) {
            const owningRel = await queryRunner('relation_definition')
              .where({ id: updateMappedById })
              .first();
            if (owningRel?.junctionTableName) {
              relationData.junctionTableName = owningRel.junctionTableName;
              relationData.junctionSourceColumn =
                owningRel.junctionTargetColumn;
              relationData.junctionTargetColumn =
                owningRel.junctionSourceColumn;
            }
          } else {
            const junction = getSqlJunctionPhysicalNames({
              sourceTable: exists.name,
              propertyName: rel.propertyName,
              targetTable: targetTablesMap.get(relationTargetTableMapKey(targetTableId))!,
            });
            relationData.junctionTableName = junction.junctionTableName;
            relationData.junctionSourceColumn = junction.junctionSourceColumn;
            relationData.junctionTargetColumn = junction.junctionTargetColumn;
          }
        }
        let relationId: number | string;
        if (rel.id) {
          await queryRunner('relation_definition')
            .where({ id: rel.id })
            .update(relationData);
          relationId = rel.id;
        } else {
          relationId = await this.insertAndGetId(
            queryRunner,
            'relation_definition',
            relationData,
          );
        }
        await this.writeNestedFieldPermissions(queryRunner, {
          permissions: (rel as any).fieldPermissions,
          subjectFk: 'relationId',
          subjectFkValue: relationId,
        });

        if (!rel.id && rel.inversePropertyName) {
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
              `Cannot create inverse '${rel.inversePropertyName}' on '${targetTablesMap.get(relationTargetTableMapKey(targetTableId))}': property name already exists`,
              {
                relationName: rel.inversePropertyName,
                targetTable: targetTablesMap.get(
                  relationTargetTableMapKey(targetTableId),
                ),
              },
            );
          }
          const existingInverse = await queryRunner('relation_definition')
            .where({ mappedById: relationId })
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
            mappedById: relationId,
            isNullable: rel.isNullable ?? true,
            isSystem: rel.isSystem || false,
            isUpdatable: rel.isUpdatable ?? true,
            isPublished: rel.isPublished ?? true,
            onDelete: rel.onDelete || 'SET NULL',
          };
          if (rel.type === 'many-to-many') {
            const owningRel = await queryRunner('relation_definition')
              .where({ id: relationId })
              .first();
            if (owningRel?.junctionTableName) {
              inverseData.junctionTableName = owningRel.junctionTableName;
              inverseData.junctionSourceColumn = owningRel.junctionTargetColumn;
              inverseData.junctionTargetColumn = owningRel.junctionSourceColumn;
            }
          }
          await queryRunner('relation_definition').insert(inverseData);
          const targetName = targetTablesMap.get(
            relationTargetTableMapKey(targetTableId),
          );
          if (targetName) affectedTableNames.add(targetName);
          this.logger.log(
            `Auto-created inverse relation '${rel.inversePropertyName}' on '${targetName}'`,
          );
        }
      }
    }
  }

  private async insertAndGetId(
    queryRunner: any,
    tableName: string,
    data: any,
  ): Promise<number | string> {
    const dbType = DatabaseConfigService.getInstanceDbType();
    if (dbType === 'postgres') {
      const [result] = await queryRunner(tableName)
        .insert(data)
        .returning('id');
      return result.id;
    }
    const [insertedId] = await queryRunner(tableName).insert(data);
    return insertedId;
  }

  private async getInverseRelationFkColumnNames(
    queryRunner: any,
    tableId: string | number,
    relations: any[],
  ): Promise<Set<string>> {
    const existingRelations = await queryRunner('relation_definition')
      .where({ sourceTableId: tableId })
      .select('id', 'propertyName', 'foreignKeyColumn', 'mappedById');
    const existingById = new Map(
      existingRelations.map((rel: any) => [String(rel.id), rel]),
    );
    const columns = new Set<string>();
    for (const rel of relations || []) {
      const existingRel: any = rel.id ? existingById.get(String(rel.id)) : null;
      const isInverse = Boolean(
        rel.mappedBy || rel.mappedById || existingRel?.mappedById,
      );
      if (!isInverse) continue;
      if (rel.foreignKeyColumn) columns.add(rel.foreignKeyColumn);
      if (rel.propertyName) {
        columns.add(getForeignKeyColumnName(rel.propertyName));
      }
      if (existingRel?.foreignKeyColumn) {
        columns.add(existingRel.foreignKeyColumn);
      }
      if (existingRel?.propertyName) {
        columns.add(getForeignKeyColumnName(existingRel.propertyName));
      }
    }
    return columns;
  }

  private getAllowedConstraintFields(body: TCreateTableBody): Set<string> | null {
    if (!body.columns && !body.relations) return null;
    const fields = new Set<string>(['id', 'createdAt', 'updatedAt']);
    for (const col of body.columns || []) {
      if (col?.name) fields.add(col.name);
    }
    for (const rel of body.relations || []) {
      if (rel?.propertyName) fields.add(rel.propertyName);
    }
    return fields;
  }

  private filterConstraintGroups(
    groups: any[],
    allowedFields: Set<string>,
  ): any[] {
    return (groups || []).filter((group) =>
      (Array.isArray(group) ? group : group?.value || []).every((field: string) =>
        allowedFields.has(field),
      ),
    );
  }

  private async writeNestedRules(
    queryRunner: any,
    opts: {
      rules: any[] | undefined;
      fkField: 'columnId' | 'relationId';
      fkValue: number | string;
    },
  ): Promise<void> {
    if (!Array.isArray(opts.rules)) return;
    const existing = await queryRunner('column_rule_definition')
      .where({ [opts.fkField]: opts.fkValue })
      .select('id');
    const deletedIds = getDeletedIds(existing, opts.rules);
    if (deletedIds.length > 0) {
      await queryRunner('column_rule_definition')
        .whereIn('id', deletedIds)
        .delete();
    }
    for (const rule of opts.rules) {
      const ruleData: any = {
        ruleType: rule.ruleType,
        value: rule.value != null ? JSON.stringify(rule.value) : null,
        message: rule.message ?? null,
        isEnabled: rule.isEnabled !== false,
        [opts.fkField]: opts.fkValue,
      };
      if (rule.id) {
        await queryRunner('column_rule_definition')
          .where({ id: rule.id })
          .update(ruleData);
      } else {
        await queryRunner('column_rule_definition').insert(ruleData);
      }
    }
  }

  private async writeNestedFieldPermissions(
    queryRunner: any,
    opts: {
      permissions: any[] | undefined;
      subjectFk: 'columnId' | 'relationId';
      subjectFkValue: number | string;
    },
  ): Promise<void> {
    if (!Array.isArray(opts.permissions)) return;
    const existing = await queryRunner('field_permission_definition')
      .where({ [opts.subjectFk]: opts.subjectFkValue })
      .select('id');
    const deletedIds = getDeletedIds(existing, opts.permissions);
    if (deletedIds.length > 0) {
      const junctionRows = await queryRunner(
        'field_permission_definition_allowedUsers_user_definition',
      )
        .whereIn('field_permission_definitionId', deletedIds)
        .select('*')
        .catch(() => [] as any[]);
      if (Array.isArray(junctionRows) && junctionRows.length > 0) {
        await queryRunner(
          'field_permission_definition_allowedUsers_user_definition',
        )
          .whereIn('field_permission_definitionId', deletedIds)
          .delete()
          .catch((): undefined => undefined);
      }
      await queryRunner('field_permission_definition')
        .whereIn('id', deletedIds)
        .delete();
    }
    for (const perm of opts.permissions) {
      const roleId =
        perm.role && typeof perm.role === 'object'
          ? (perm.role.id ?? perm.role._id)
          : perm.role;
      const permData: any = {
        action: perm.action,
        effect: perm.effect ?? 'allow',
        condition:
          perm.condition != null ? JSON.stringify(perm.condition) : null,
        isEnabled: perm.isEnabled !== false,
        description: perm.description ?? null,
        roleId: roleId ?? null,
        columnId: opts.subjectFk === 'columnId' ? opts.subjectFkValue : null,
        relationId:
          opts.subjectFk === 'relationId' ? opts.subjectFkValue : null,
      };
      let permId: number | string;
      if (perm.id) {
        await queryRunner('field_permission_definition')
          .where({ id: perm.id })
          .update(permData);
        permId = perm.id;
      } else {
        permId = await this.insertAndGetId(
          queryRunner,
          'field_permission_definition',
          permData,
        );
      }
      if (Array.isArray(perm.allowedUsers)) {
        await this.syncAllowedUsers(queryRunner, permId, perm.allowedUsers);
      }
    }
  }

  private async syncAllowedUsers(
    queryRunner: any,
    permId: number | string,
    users: any[],
  ): Promise<void> {
    const userIds = users
      .map((u: any) => (typeof u === 'object' ? (u.id ?? u._id) : u))
      .filter((v: any) => v != null);
    const junctionTable =
      'field_permission_definition_allowedUsers_user_definition';
    try {
      await queryRunner(junctionTable)
        .where({ field_permission_definitionId: permId })
        .delete();
      if (userIds.length > 0) {
        await queryRunner(junctionTable).insert(
          userIds.map((uid: any) => ({
            field_permission_definitionId: permId,
            user_definitionId: uid,
          })),
        );
      }
    } catch {
      // Junction table name may vary by schema convention; best-effort sync.
    }
  }
}
