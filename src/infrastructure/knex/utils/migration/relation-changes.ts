import { Knex } from 'knex';
import { Logger } from '@nestjs/common';
import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../naming-helpers';

const logger = new Logger('RelationChanges');

async function getPrimaryKeyTypeForTable(
  knex: Knex,
  tableName: string,
  metadataCacheService?: any,
): Promise<'uuid' | 'int'> {
  try {
    if (metadataCacheService) {
      const targetMetadata = await metadataCacheService.lookupTableByName(tableName);
      if (targetMetadata) {
        const pkColumn = targetMetadata.columns.find((c: any) => c.isPrimary);
        if (pkColumn) {
          const type = pkColumn.type?.toLowerCase() || '';
          return type === 'uuid' || type === 'uuidv4' || type.includes('uuid') ? 'uuid' : 'int';
        }
      }
    }
    
    const pkInfo = await knex('column_definition')
      .join('table_definition', 'column_definition.table', '=', 'table_definition.id')
      .where('table_definition.name', tableName)
      .where('column_definition.isPrimary', true)
      .select('column_definition.type')
      .first();
    
    if (pkInfo) {
      const type = pkInfo.type?.toLowerCase() || '';
      return type === 'uuid' || type === 'uuidv4' || type.includes('uuid') ? 'uuid' : 'int';
    }
    
    logger.warn(`Could not find primary key for table ${tableName}, defaulting to int`);
    return 'int';
  } catch (error) {
    logger.warn(`Error getting primary key type for ${tableName}: ${error.message}, defaulting to int`);
    return 'int';
  }
}

async function validateFkColumnNotConflict(
  fkColumnName: string,
  existingColumns: any[],
  pendingCreateColumns: any[],
  pendingDeleteColumns: any[],
  context: string,
  tableName: string,
  knex?: Knex,
): Promise<void> {
  const deletedColNames = new Set(pendingDeleteColumns.map(c => c.name));
  const existingColNames = existingColumns
    .filter(c => !deletedColNames.has(c.name))
    .map(c => c.name);
  const pendingColNames = pendingCreateColumns.map(c => c.name);

  const allColNames = new Set([...existingColNames, ...pendingColNames]);

  if (allColNames.has(fkColumnName)) {
    throw new Error(
      `Cannot create FK column '${fkColumnName}' (${context}): ` +
      `A column with this name already exists or will be created. ` +
      `Please rename the relation property to avoid column name conflict.`
    );
  }

  if (knex && tableName) {
    try {
      const dbType = (knex as any).client.config.client;
      let columnExists = false;

      if (dbType === 'postgres') {
        const result = await knex.raw(`
          SELECT column_name
          FROM information_schema.columns
          WHERE table_schema = 'public'
            AND table_name = ?
            AND column_name = ?
        `, [tableName, fkColumnName]);
        columnExists = result.rows && result.rows.length > 0;
      } else if (dbType === 'mysql') {
        const result = await knex.raw(`
          SELECT COLUMN_NAME
          FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA = DATABASE()
            AND TABLE_NAME = ?
            AND COLUMN_NAME = ?
        `, [tableName, fkColumnName]);
        columnExists = result[0] && result[0].length > 0;
      }

      if (columnExists) {
        throw new Error(
          `Cannot create FK column '${fkColumnName}' (${context}): ` +
          `A column with this name already exists in physical database. ` +
          `Please rename the relation property to avoid column name conflict.`
        );
      }
    } catch (error: any) {
      if (error.message.includes('Cannot create FK column')) {
        throw error;
      }
      logger.warn(`Failed to check physical DB for column ${fkColumnName} in ${tableName}: ${error.message}`);
    }
  }
}

export async function analyzeRelationChanges(
  knex: Knex,
  oldRelations: any[],
  newRelations: any[],
  diff: any,
  tableName: string,
  oldColumns: any[],
  newColumns: any[],
  metadataCacheService?: any,
): Promise<void> {
  logger.log('Relation Analysis (FK Column Generation):');
  logger.log(`DEBUG: oldRelations count: ${oldRelations.length}, newRelations count: ${newRelations.length}`);

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


  const deletedRelIds = oldRelations
    .filter(r => !newRelMap.has(r.id))
    .map(r => r.id);

  const createdRelIds = newRelations
    .filter(r => r.id !== undefined && r.id !== null && !oldRelMap.has(r.id))
    .map(r => r.id)
    .filter((id, index, self) => self.indexOf(id) === index);


  await handleDeletedRelations(knex, oldRelations, deletedRelIds, diff, tableName);
  await handleCreatedRelations(knex, newRelations, createdRelIds, diff, tableName, newColumns, metadataCacheService);
  await handleUpdatedRelations(knex, oldRelMap, newRelMap, diff, tableName, oldColumns, newColumns, metadataCacheService);
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

    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
      const fkColumn = rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      diff.columns.delete.push({
        name: fkColumn,
        isForeignKey: true,
      });
    } else if (rel.type === 'one-to-many') {
      continue;
    } else if (rel.type === 'many-to-many') {
      const junctionTableName = rel.junctionTableName;

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
  newColumns: any[],
  metadataCacheService?: any,
): Promise<void> {
  for (const relId of createdRelIds) {
    const rel = newRelations.find(r => r.id === relId);
    if (!rel) continue;

    if (rel.type === 'many-to-one' || rel.type === 'one-to-one') {
      const fkColumn = getForeignKeyColumnName(rel.propertyName);

      await validateFkColumnNotConflict(
        fkColumn,
        newColumns,
        diff.columns.create,
        diff.columns.delete,
        `relation '${rel.propertyName}' (${rel.type})`,
        tableName,
        knex,
      );

      const targetPkType = await getPrimaryKeyTypeForTable(knex, rel.targetTableName, metadataCacheService);

      diff.columns.create.push({
        name: fkColumn,
        type: targetPkType,
        isNullable: rel.isNullable ?? true,
        isForeignKey: true,
        foreignKeyTarget: rel.targetTableName,
        foreignKeyColumn: 'id',
        isUnique: rel.type === 'one-to-one',
        onDelete: rel.onDelete || 'SET NULL',
      });
    } else if (rel.type === 'one-to-many') {
      const targetTableName = rel.targetTableName;
      if (!rel.inversePropertyName) {
        logger.warn(`  O2M relation '${rel.propertyName}' missing inversePropertyName, cannot determine FK column name`);
        continue;
      }
      const fkColumn = getForeignKeyColumnName(rel.inversePropertyName);

      const sourcePkType = await getPrimaryKeyTypeForTable(knex, tableName, metadataCacheService);

      if (!diff.crossTableOperations) {
        diff.crossTableOperations = [];
      }

      const existingCrossOp = diff.crossTableOperations.find(
        (op: any) =>
          op.operation === 'createColumn' &&
          op.targetTable === targetTableName &&
          op.column.name === fkColumn,
      );

      if (existingCrossOp) {
        logger.warn(
          `  Skipping duplicate FK column creation: ${fkColumn} in ${targetTableName} (already in crossTableOperations)`,
        );
        continue;
      }

      diff.crossTableOperations.push({
        operation: 'createColumn',
        targetTable: targetTableName,
        column: {
          name: fkColumn,
          type: sourcePkType,
          isNullable: true,
          isForeignKey: true,
          foreignKeyTarget: tableName,
          foreignKeyColumn: 'id',
          onDelete: rel.onDelete || 'SET NULL',
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
  oldColumns: any[],
  newColumns: any[],
  metadataCacheService?: any,
): Promise<void> {
  for (const [relId, newRel] of newRelMap) {
    const oldRel = oldRelMap.get(relId);
    if (!oldRel) continue;

    const changes: string[] = [];
    const propertyNameChanged = oldRel.propertyName !== newRel.propertyName;
    const inversePropertyNameChanged = oldRel.inversePropertyName !== newRel.inversePropertyName;
    const typeChanged = oldRel.type !== newRel.type;
    const targetTableChanged = oldRel.targetTableName !== newRel.targetTableName;
    const isNullableChanged = oldRel.isNullable !== newRel.isNullable;
    const onDeleteChanged = oldRel.onDelete !== newRel.onDelete;

    if (propertyNameChanged) changes.push(`propertyName: ${oldRel.propertyName} → ${newRel.propertyName}`);
    if (typeChanged) changes.push(`type: ${oldRel.type} → ${newRel.type}`);
    if (targetTableChanged) changes.push(`target: ${oldRel.targetTableName} → ${newRel.targetTableName}`);
    if (inversePropertyNameChanged) changes.push(`inversePropertyName: ${oldRel.inversePropertyName} → ${newRel.inversePropertyName}`);
    if (isNullableChanged) changes.push(`nullable: ${oldRel.isNullable} → ${newRel.isNullable}`);
    if (onDeleteChanged) changes.push(`onDelete: ${oldRel.onDelete} → ${newRel.onDelete}`);

    if (changes.length > 0) {
      logger.log(`Updated relation ${relId}: ${changes.join(', ')}`);

      if (typeChanged) {
        await handleRelationTypeChange(knex, oldRel, newRel, diff, tableName, newColumns, metadataCacheService);
      }
      else if (propertyNameChanged || inversePropertyNameChanged) {
        await handleRelationPropertyNameChange(knex, oldRel, newRel, diff, tableName, propertyNameChanged, inversePropertyNameChanged, metadataCacheService);
      }
      else if (onDeleteChanged) {
        await handleOnDeleteChange(knex, oldRel, newRel, diff, tableName);
      }
    }
  }
}

async function handleRelationPropertyNameChange(
  knex: Knex,
  oldRel: any,
  newRel: any,
  diff: any,
  tableName: string,
  propertyNameChanged: boolean,
  inversePropertyNameChanged: boolean,
  metadataCacheService?: any,
): Promise<void> {
  logger.log(`Handling relation property name change for ${oldRel.propertyName} (${oldRel.type})`);

  const relationType = oldRel.type;

  if ((relationType === 'many-to-one' || relationType === 'one-to-one') && propertyNameChanged) {
    const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.propertyName);
    const newFkColumn = getForeignKeyColumnName(newRel.propertyName);

    logger.log(`  M2O/O2O propertyName change: RENAME FK column ${oldFkColumn} → ${newFkColumn}`);

    if (oldFkColumn !== newFkColumn) {
      if (!diff.columns.rename) {
        diff.columns.rename = [];
      }

      const targetPkType = await getPrimaryKeyTypeForTable(knex, newRel.targetTableName, metadataCacheService);

      diff.columns.rename.push({
        oldName: oldFkColumn,
        newName: newFkColumn,
        column: {
          name: newFkColumn,
          type: targetPkType,
          isNullable: newRel.isNullable ?? true,
          isForeignKey: true,
        },
      });
    } else {
      logger.log(`  FK column name unchanged (${oldFkColumn}), skipping rename`);
    }
  }

  else if (relationType === 'one-to-many' && inversePropertyNameChanged) {
    if (!oldRel.inversePropertyName || !newRel.inversePropertyName) {
      logger.warn(`  O2M relation missing inversePropertyName, cannot rename FK column`);
      return;
    }

    const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.inversePropertyName);
    const newFkColumn = getForeignKeyColumnName(newRel.inversePropertyName);

    logger.log(`  O2M inversePropertyName change: RENAME FK column ${oldFkColumn} → ${newFkColumn} in target table ${oldRel.targetTableName}`);

    if (oldFkColumn !== newFkColumn) {
      if (!diff.crossTableOperations) {
        diff.crossTableOperations = [];
      }

      const sourcePkType = await getPrimaryKeyTypeForTable(knex, tableName, metadataCacheService);

      diff.crossTableOperations.push({
        operation: 'renameColumn',
        targetTable: oldRel.targetTableName,
        oldColumnName: oldFkColumn,
        newColumnName: newFkColumn,
        columnDef: {
          type: sourcePkType,
          isNullable: true,
          isForeignKey: true,
        },
      });
    } else {
      logger.log(`  FK column name unchanged (${oldFkColumn}), skipping rename`);
    }
  }

  else if (relationType === 'one-to-many' && propertyNameChanged && !inversePropertyNameChanged) {
    logger.log(`  O2M propertyName change (inversePropertyName unchanged): FK column name stays the same`);
  }

  else if (relationType === 'many-to-many' && propertyNameChanged) {
    const oldJunctionTableName = oldRel.junctionTableName;
    const newJunctionTableName = getJunctionTableName(tableName, newRel.propertyName, newRel.targetTableName);

    logger.log(`  M2M propertyName change: RENAME junction table ${oldJunctionTableName} → ${newJunctionTableName}`);

    if (oldJunctionTableName !== newJunctionTableName) {
      if (!diff.junctionTables) {
        diff.junctionTables = { create: [], drop: [], update: [], rename: [] };
      }

      diff.junctionTables.rename.push({
        oldTableName: oldJunctionTableName,
        newTableName: newJunctionTableName,
      });
    } else {
      logger.log(`  Junction table name unchanged (${oldJunctionTableName}), skipping rename`);
    }
  }

  else {
    logger.warn(`  Unhandled property name change scenario for ${relationType}`);
  }
}

async function handleRelationTypeChange(
  knex: Knex,
  oldRel: any,
  newRel: any,
  diff: any,
  tableName: string,
  newColumns: any[],
  metadataCacheService?: any,
): Promise<void> {
  logger.log(`Handling relation type change: ${oldRel.type} → ${newRel.type} for ${newRel.propertyName}`);

  if (!diff.crossTableOperations) {
    diff.crossTableOperations = [];
  }
  if (!diff.junctionTables) {
    diff.junctionTables = { create: [], drop: [], update: [] };
  }

  const oldType = oldRel.type;
  const newType = newRel.type;

  if ((oldType === 'many-to-one' || oldType === 'one-to-one') && newType === 'many-to-many') {
    logger.log(`  M2O/O2O → M2M: Drop FK column, Create junction table`);

    const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.propertyName);
    logger.log(`    ➖ Drop FK column: ${oldFkColumn}`);
    diff.columns.delete.push({
      name: oldFkColumn,
      isForeignKey: true,
    });

    const junctionTableName = getJunctionTableName(tableName, newRel.propertyName, newRel.targetTableName);
    const { sourceColumn, targetColumn } = getJunctionColumnNames(tableName, newRel.propertyName, newRel.targetTableName);
    logger.log(`    ➕ Create junction table: ${junctionTableName}`);
    diff.junctionTables.create.push({
      tableName: junctionTableName,
      sourceTable: tableName,
      targetTable: newRel.targetTableName,
      sourceColumn: sourceColumn,
      targetColumn: targetColumn,
    });
  }

  else if (oldType === 'many-to-many' && (newType === 'many-to-one' || newType === 'one-to-one')) {
    logger.log(`  M2M → M2O/O2O: Drop junction table, Create FK column`);

    const oldJunctionTableName = oldRel.junctionTableName;
    logger.log(`    ➖ Drop junction table: ${oldJunctionTableName}`);
    diff.junctionTables.drop.push({
      tableName: oldJunctionTableName,
      reason: 'Relation type changed from M2M to M2O/O2O',
    });

    const newFkColumn = getForeignKeyColumnName(newRel.propertyName);
    logger.log(`    ➕ Create FK column: ${newFkColumn} → ${newRel.targetTableName}.id`);
    logger.log(`    Note: FK column will be created as NULLABLE to avoid data constraint errors`);

    await validateFkColumnNotConflict(
      newFkColumn,
      newColumns,
      diff.columns.create,
      diff.columns.delete,
      `relation type change ${oldRel.type} → ${newRel.type} for '${newRel.propertyName}'`,
      tableName,
      knex,
    );

    const targetPkType = await getPrimaryKeyTypeForTable(knex, newRel.targetTableName, metadataCacheService);

    diff.columns.create.push({
      name: newFkColumn,
      type: targetPkType,
      isNullable: true,
      isForeignKey: true,
      foreignKeyTarget: newRel.targetTableName,
      foreignKeyColumn: 'id',
      isUnique: newType === 'one-to-one',
    });
  }

  else if (oldType === 'one-to-many' && newType === 'many-to-many') {
    logger.log(`  O2M → M2M: Drop FK column in target table, Create junction table`);

    if (!oldRel.inversePropertyName) {
      throw new Error(`O2M relation '${oldRel.propertyName}' must have inversePropertyName to determine FK column name`);
    }
    const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.inversePropertyName);
    logger.log(`    ➖ Drop FK column ${oldFkColumn} from target table ${oldRel.targetTableName}`);
    diff.crossTableOperations.push({
      operation: 'dropColumn',
      targetTable: oldRel.targetTableName,
      columnName: oldFkColumn,
      isForeignKey: true,
    });

    const junctionTableName = getJunctionTableName(tableName, newRel.propertyName, newRel.targetTableName);
    const { sourceColumn, targetColumn } = getJunctionColumnNames(tableName, newRel.propertyName, newRel.targetTableName);
    logger.log(`    ➕ Create junction table: ${junctionTableName}`);
    diff.junctionTables.create.push({
      tableName: junctionTableName,
      sourceTable: tableName,
      targetTable: newRel.targetTableName,
      sourceColumn: sourceColumn,
      targetColumn: targetColumn,
    });
  }

  else if (oldType === 'many-to-many' && newType === 'one-to-many') {
    logger.log(`  M2M → O2M: Drop junction table, Create FK column in target table`);

    const oldJunctionTableName = oldRel.junctionTableName;
    logger.log(`    ➖ Drop junction table: ${oldJunctionTableName}`);
    diff.junctionTables.drop.push({
      tableName: oldJunctionTableName,
      reason: 'Relation type changed from M2M to O2M',
    });

    if (!newRel.inversePropertyName) {
      throw new Error(`O2M relation '${newRel.propertyName}' must have inversePropertyName to determine FK column name`);
    }
    const newFkColumn = getForeignKeyColumnName(newRel.inversePropertyName);
    logger.log(`    ➕ Create FK column ${newFkColumn} in target table ${newRel.targetTableName}`);
    
    const sourcePkType = await getPrimaryKeyTypeForTable(knex, tableName, metadataCacheService);

    diff.crossTableOperations.push({
      operation: 'createColumn',
      targetTable: newRel.targetTableName,
      column: {
        name: newFkColumn,
        type: sourcePkType,
        isNullable: true,
        isForeignKey: true,
        foreignKeyTarget: tableName,
        foreignKeyColumn: 'id',
      },
    });
  }

  else if ((oldType === 'many-to-one' || oldType === 'one-to-one') && newType === 'one-to-many') {
    logger.log(`  M2O/O2O → O2M: Drop FK column, Create FK column in target table`);

    const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.propertyName);
    logger.log(`    ➖ Drop FK column: ${oldFkColumn}`);
    diff.columns.delete.push({
      name: oldFkColumn,
      isForeignKey: true,
    });

    if (!newRel.inversePropertyName) {
      throw new Error(`O2M relation '${newRel.propertyName}' must have inversePropertyName to determine FK column name`);
    }
    const newFkColumn = getForeignKeyColumnName(newRel.inversePropertyName);
    logger.log(`    ➕ Create FK column ${newFkColumn} in target table ${newRel.targetTableName}`);
    
    const sourcePkType = await getPrimaryKeyTypeForTable(knex, tableName, metadataCacheService);

    diff.crossTableOperations.push({
      operation: 'createColumn',
      targetTable: newRel.targetTableName,
      column: {
        name: newFkColumn,
        type: sourcePkType,
        isNullable: true,
        isForeignKey: true,
        foreignKeyTarget: tableName,
        foreignKeyColumn: 'id',
      },
    });
  }

  else if (oldType === 'one-to-many' && (newType === 'many-to-one' || newType === 'one-to-one')) {
    logger.log(`  O2M → M2O/O2O: Drop FK column in target table, Create FK column`);

    if (!oldRel.inversePropertyName) {
      throw new Error(`O2M relation '${oldRel.propertyName}' must have inversePropertyName to determine FK column name`);
    }
    const oldFkColumn = oldRel.foreignKeyColumn || getForeignKeyColumnName(oldRel.inversePropertyName);
    logger.log(`    ➖ Drop FK column ${oldFkColumn} from target table ${oldRel.targetTableName}`);
    diff.crossTableOperations.push({
      operation: 'dropColumn',
      targetTable: oldRel.targetTableName,
      columnName: oldFkColumn,
      isForeignKey: true,
    });

    const newFkColumn = getForeignKeyColumnName(newRel.propertyName);
    logger.log(`    ➕ Create FK column: ${newFkColumn} → ${newRel.targetTableName}.id`);

    await validateFkColumnNotConflict(
      newFkColumn,
      newColumns,
      diff.columns.create,
      diff.columns.delete,
      `relation type change ${oldRel.type} → ${newRel.type} for '${newRel.propertyName}'`,
      tableName,
      knex,
    );

    const targetPkType = await getPrimaryKeyTypeForTable(knex, newRel.targetTableName, metadataCacheService);

    diff.columns.create.push({
      name: newFkColumn,
      type: targetPkType,
      isNullable: newRel.isNullable ?? true,
      isForeignKey: true,
      foreignKeyTarget: newRel.targetTableName,
      foreignKeyColumn: 'id',
      isUnique: newType === 'one-to-one',
    });
  }

  else if ((oldType === 'many-to-one' && newType === 'one-to-one') || (oldType === 'one-to-one' && newType === 'many-to-one')) {
    logger.log(`  M2O ↔ O2O: FK column stays, constraint changes`);

    const fkColumn = newRel.foreignKeyColumn || getForeignKeyColumnName(newRel.propertyName);

    if (oldType === 'many-to-one' && newType === 'one-to-one') {
      logger.log(`    ➕ Add UNIQUE constraint on ${fkColumn}`);
      diff.constraints.uniques.create.push([fkColumn]);
    } else {
      logger.log(`    ➖ Drop UNIQUE constraint on ${fkColumn}`);
      diff.constraints.uniques.delete.push([fkColumn]);
    }
  }

  else {
    logger.warn(`  Unhandled relation type change: ${oldType} → ${newType}`);
  }
}

async function handleOnDeleteChange(
  knex: Knex,
  oldRel: any,
  newRel: any,
  diff: any,
  tableName: string,
): Promise<void> {
  logger.log(`Handling onDelete change for ${newRel.propertyName}: ${oldRel.onDelete} → ${newRel.onDelete}`);

  const relationType = newRel.type;

  if (relationType === 'many-to-one' || relationType === 'one-to-one') {
    const fkColumn = getForeignKeyColumnName(newRel.propertyName);
    logger.log(`  Will recreate FK constraint on ${tableName}.${fkColumn} with onDelete: ${newRel.onDelete}`);

    if (!diff.foreignKeys) {
      diff.foreignKeys = { recreate: [] };
    }

    diff.foreignKeys.recreate.push({
      tableName: tableName,
      columnName: fkColumn,
      targetTable: newRel.targetTableName,
      targetColumn: 'id',
      onDelete: newRel.onDelete || 'SET NULL',
    });
  } else if (relationType === 'one-to-many') {
    if (!newRel.inversePropertyName) {
      logger.warn(`  O2M relation '${newRel.propertyName}' missing inversePropertyName, cannot update FK constraint`);
      return;
    }

    const fkColumn = getForeignKeyColumnName(newRel.inversePropertyName);
    logger.log(`  Will recreate FK constraint on ${newRel.targetTableName}.${fkColumn} with onDelete: ${newRel.onDelete}`);

    if (!diff.foreignKeys) {
      diff.foreignKeys = { recreate: [] };
    }

    diff.foreignKeys.recreate.push({
      tableName: newRel.targetTableName,
      columnName: fkColumn,
      targetTable: tableName,
      targetColumn: 'id',
      onDelete: newRel.onDelete || 'SET NULL',
    });
  } else if (relationType === 'many-to-many') {
    logger.log(`  M2M relations do not have direct FK onDelete config (handled by junction table)`);
  }
}
