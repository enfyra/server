import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../../../shared/utils/naming-helpers';

const logger = new Logger('RelationChanges');

export async function analyzeRelationChanges(
  knex: Knex,
  oldRelations: any[],
  newRelations: any[],
  diff: any,
  tableName: string,
): Promise<void> {
  logger.log('🔍 Relation Analysis (FK Column Generation):');
  logger.log(`🔍 DEBUG: oldRelations count: ${oldRelations.length}, newRelations count: ${newRelations.length}`);

  const targetTableIds = [...oldRelations, ...newRelations]
    .map(rel => typeof rel.targetTable === 'object' ? rel.targetTable.id : null)
    .filter(id => id != null);

  const targetTablesMap = new Map<number, string>();
  if (targetTableIds.length > 0) {
    const targetTables = await knex('table_definition')
      .select('id', 'name')
      .whereIn('id', targetTableIds);

    for (const table of targetTables) {
      targetTablesMap.set(table.id, table.name);
    }
  }

  oldRelations = oldRelations.map(rel => {
    const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : null;
    return {
      ...rel,
      sourceTableName: rel.sourceTableName || tableName,
      targetTableName: rel.targetTableName || (targetTableId ? targetTablesMap.get(targetTableId) : rel.targetTable)
    };
  });

  newRelations = newRelations.map(rel => {
    const targetTableId = typeof rel.targetTable === 'object' ? rel.targetTable.id : null;
    return {
      ...rel,
      sourceTableName: rel.sourceTableName || tableName,
      targetTableName: rel.targetTableName || (targetTableId ? targetTablesMap.get(targetTableId) : rel.targetTable)
    };
  });

  const oldRelMap = new Map(oldRelations.map(r => [r.id, r]));
  const newRelMap = new Map(newRelations.map(r => [r.id, r]));

  logger.log('🔍 Relation Analysis (FK Column Generation):');
  logger.log('  Old relations:', oldRelations.map(r => `${r.id}:${r.propertyName}`));
  logger.log('  New relations:', newRelations.map(r => `${r.id}:${r.propertyName}`));

  const deletedRelIds = oldRelations
    .filter(r => !newRelMap.has(r.id))
    .map(r => r.id);

  const createdRelIds = newRelations
    .filter(r => !oldRelMap.has(r.id))
    .map(r => r.id);

  logger.log(`📊 Deleted relation IDs: [${deletedRelIds.join(', ')}]`);
  logger.log(`📊 Created relation IDs: [${createdRelIds.join(', ')}]`);

  await handleDeletedRelations(knex, oldRelations, deletedRelIds, diff, tableName);
  await handleCreatedRelations(knex, newRelations, createdRelIds, diff, tableName);
  await handleUpdatedRelations(knex, oldRelMap, newRelMap, diff, tableName);
}

async function handleDeletedRelations(
  knex: Knex,
  oldRelations: any[],
  deletedRelIds: number[],
  diff: any,
  tableName: string,
): Promise<void> {
  for (const relId of deletedRelIds) {
    const rel = oldRelations.find(r => r.id === relId);
    if (!rel) continue;

    logger.log(`🗑️  Deleted relation: ${rel.propertyName} (${rel.type})`);

    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      logger.log(`  Will drop FK column: ${fkColumn}`);
      diff.columns.delete.push({
        name: fkColumn,
        isForeignKey: true,
      });
    } else if (rel.type === 'one-to-many') {
      const targetTableName = rel.targetTableName;
      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(tableName);
      logger.log(`  O2M: Will drop FK column ${fkColumn} from target table ${targetTableName}`);

      if (!diff.crossTableOperations) {
        diff.crossTableOperations = [];
      }

      diff.crossTableOperations.push({
        operation: 'dropColumn',
        targetTable: targetTableName,
        columnName: fkColumn,
        isForeignKey: true,
      });
    } else if (rel.type === 'many-to-many') {
      const junctionTableName = rel.junctionTableName;
      logger.log(`  M2M: Will drop junction table ${junctionTableName}`);

      if (!diff.junctionTables) {
        diff.junctionTables = { create: [], drop: [], update: [] };
      }

      diff.junctionTables.drop.push({
        tableName: junctionTableName,
        reason: 'Relation deleted',
      });
    }
  }
}

async function handleCreatedRelations(
  knex: Knex,
  newRelations: any[],
  createdRelIds: number[],
  diff: any,
  tableName: string,
): Promise<void> {
  for (const relId of createdRelIds) {
    const rel = newRelations.find(r => r.id === relId);
    if (!rel) continue;

    logger.log(`✨ Created relation: ${rel.propertyName} (${rel.type}) → ${rel.targetTableName}`);

    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      logger.log(`  Will create FK column: ${fkColumn} → ${rel.targetTableName}.id`);

      diff.columns.create.push({
        name: fkColumn,
        type: 'int',
        isNullable: rel.isNullable ?? true,
        isForeignKey: true,
        foreignKeyTarget: rel.targetTableName,
        foreignKeyColumn: 'id',
      });
    } else if (rel.type === 'one-to-many') {
      const targetTableName = rel.targetTableName;
      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(tableName);
      logger.log(`  O2M: Will create FK column ${fkColumn} in target table ${targetTableName}`);

      if (!diff.crossTableOperations) {
        diff.crossTableOperations = [];
      }

      diff.crossTableOperations.push({
        operation: 'createColumn',
        targetTable: targetTableName,
        column: {
          name: fkColumn,
          type: 'int',
          isNullable: true,
          isForeignKey: true,
          foreignKeyTarget: tableName,
          foreignKeyColumn: 'id',
        },
      });
    } else if (rel.type === 'many-to-many') {
      const junctionTableName = getJunctionTableName(tableName, rel.propertyName, rel.targetTableName);
      const { sourceColumn, targetColumn } = getJunctionColumnNames(tableName, rel.propertyName, rel.targetTableName);

      logger.log(`  M2M: Will create junction table ${junctionTableName}`);
      logger.log(`      Columns: ${sourceColumn}, ${targetColumn}`);

      if (!diff.junctionTables) {
        diff.junctionTables = { create: [], drop: [], update: [] };
      }

      diff.junctionTables.create.push({
        tableName: junctionTableName,
        sourceTable: tableName,
        targetTable: rel.targetTableName,
        sourceColumn: sourceColumn,
        targetColumn: targetColumn,
      });
    }
  }
}

async function handleUpdatedRelations(
  knex: Knex,
  oldRelMap: Map<number, any>,
  newRelMap: Map<number, any>,
  diff: any,
  tableName: string,
): Promise<void> {
  for (const [relId, newRel] of newRelMap) {
    const oldRel = oldRelMap.get(relId);
    if (!oldRel) continue;

    const changes: string[] = [];
    if (oldRel.propertyName !== newRel.propertyName) changes.push(`propertyName: ${oldRel.propertyName} → ${newRel.propertyName}`);
    if (oldRel.type !== newRel.type) changes.push(`type: ${oldRel.type} → ${newRel.type}`);
    if (oldRel.targetTableName !== newRel.targetTableName) changes.push(`target: ${oldRel.targetTableName} → ${newRel.targetTableName}`);
    if (oldRel.isNullable !== newRel.isNullable) changes.push(`nullable: ${oldRel.isNullable} → ${newRel.isNullable}`);

    if (changes.length > 0) {
      logger.log(`🔄 Updated relation ${relId}: ${changes.join(', ')}`);
    }
  }
}
