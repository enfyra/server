import { Logger } from '../../../shared/logger';
import { KnexService } from '../knex.service';
import { MetadataCacheService } from '../../cache';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
} from '../../../kernel/query';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { analyzeRelationChanges } from '../utils/migration/relation-changes';
import {
  generateSQLFromDiff,
  generateBatchSQL,
  executeBatchSQL,
  JournalContext,
} from '../utils/migration/sql-diff-generator';

export class SqlSchemaDiffService {
  private readonly logger = new Logger(SqlSchemaDiffService.name);
  private readonly knexService: KnexService;
  private readonly metadataCacheService: MetadataCacheService;
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    knexService: KnexService;
    metadataCacheService: MetadataCacheService;
    queryBuilderService: QueryBuilderService;
  }) {
    this.knexService = deps.knexService;
    this.metadataCacheService = deps.metadataCacheService;
    this.queryBuilderService = deps.queryBuilderService;
  }

  async generateSchemaDiff(oldMetadata: any, newMetadata: any): Promise<any> {
    const diff = {
      table: {
        create: null,
        update: null,
        delete: false,
      },
      columns: {
        create: [],
        update: [],
        delete: [],
        rename: [],
      },
      relations: {
        create: [],
        update: [],
        delete: [],
        rename: [],
      },
      constraints: {
        uniques: {
          create: [],
          update: [],
          delete: [],
        },
        indexes: {
          create: [],
          update: [],
          delete: [],
        },
      },
    };
    if (oldMetadata.name !== newMetadata.name) {
      diff.table.update = {
        oldName: oldMetadata.name,
        newName: newMetadata.name,
      };
    }
    this.analyzeColumnChanges(
      oldMetadata.columns || [],
      newMetadata.columns || [],
      diff,
    );
    const knex = this.knexService.getKnex();
    await analyzeRelationChanges(
      knex,
      oldMetadata.relations || [],
      newMetadata.relations || [],
      diff,
      newMetadata.name,
      oldMetadata.columns || [],
      newMetadata.columns || [],
      this.metadataCacheService,
    );
    this.analyzeConstraintChanges(oldMetadata, newMetadata, diff);
    return diff;
  }

  analyzeColumnChanges(oldColumns: any[], newColumns: any[], diff: any): void {
    const oldColMap = new Map(
      oldColumns.filter((c) => c.id != null).map((c) => [c.id, c]),
    );
    const newColMap = new Map(
      newColumns.filter((c) => c.id != null).map((c) => [c.id, c]),
    );
    const oldColNameMap = new Map(oldColumns.map((c) => [c.name, c]));
    for (const newCol of newColumns) {
      if (newCol.id == null) {
        const existsByName = oldColNameMap.has(newCol.name);
        if (existsByName) {
          continue;
        }
        diff.columns.create.push(newCol);
        continue;
      }
      const hasInOld = oldColMap.has(newCol.id);
      const existsByName = oldColNameMap.has(newCol.name);
      if (!hasInOld) {
        if (existsByName && this.isSystemColumn(newCol.name)) {
          continue;
        }
        if (existsByName) {
          continue;
        }
        diff.columns.create.push(newCol);
      }
    }
    for (const oldCol of oldColumns) {
      if (oldCol.id == null) {
        continue;
      }
      const newCol = newColMap.get(oldCol.id);
      if (!newCol) {
        if (this.isSystemColumn(oldCol.name)) {
        } else {
          diff.columns.delete.push(oldCol);
        }
      } else {
        if (
          oldCol.id &&
          newCol.id &&
          oldCol.id === newCol.id &&
          oldCol.name !== newCol.name
        ) {
          diff.columns.rename.push({
            oldName: oldCol.name,
            newName: newCol.name,
            column: newCol,
          });
        } else if (this.hasColumnChanged(oldCol, newCol)) {
          diff.columns.update.push({
            oldColumn: oldCol,
            newColumn: newCol,
          });
        }
      }
    }
  }

  analyzeConstraintChanges(
    oldMetadata: any,
    newMetadata: any,
    diff: any,
  ): void {
    const oldUniques = oldMetadata.uniques || [];
    const newUniques = newMetadata.uniques || [];

    const oldRelationFkMap = new Map<string, string>();
    const newRelationFkMap = new Map<string, string>();
    for (const rel of oldMetadata.relations || []) {
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
        oldRelationFkMap.set(rel.propertyName, fkCol);
      }
    }
    for (const rel of newMetadata.relations || []) {
      if (['many-to-one', 'one-to-one'].includes(rel.type)) {
        const fkCol =
          rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
        newRelationFkMap.set(rel.propertyName, fkCol);
      }
    }

    const columnRenames: Map<string, string> = new Map();
    for (const rename of diff.columns.rename || []) {
      columnRenames.set(rename.oldName, rename.newName);
    }

    const propertyNameRenames: Map<string, string> = new Map();
    const fkColumnRenames: Map<string, string> = new Map();
    const oldRelMap = new Map<number, any>(
      (oldMetadata.relations || []).map((r: any) => [r.id, r]),
    );
    const newRelMap = new Map<number, any>(
      (newMetadata.relations || []).map((r: any) => [r.id, r]),
    );

    for (const [relId, newRel] of newRelMap) {
      const oldRel = oldRelMap.get(relId as number);
      if (oldRel && oldRel.propertyName !== newRel.propertyName) {
        propertyNameRenames.set(oldRel.propertyName, newRel.propertyName);
        const oldFk =
          oldRelationFkMap.get(oldRel.propertyName) ||
          getForeignKeyColumnName(oldRel.propertyName);
        const newFk =
          newRelationFkMap.get(newRel.propertyName) ||
          getForeignKeyColumnName(newRel.propertyName);
        if (oldFk !== newFk) {
          fkColumnRenames.set(oldFk, newFk);
        }
      }
    }

    for (const [oldFk, newFk] of fkColumnRenames) {
      columnRenames.set(oldFk, newFk);
    }

    if (columnRenames.size > 0 || propertyNameRenames.size > 0) {
      const updatedUniquesByColumn = this.updateConstraintColumns(
        oldUniques,
        columnRenames,
      );
      const updatedIndexesByColumn = this.updateConstraintColumns(
        oldMetadata.indexes || [],
        columnRenames,
      );

      const updatedUniquesByProperty = this.updateConstraintColumns(
        updatedUniquesByColumn,
        propertyNameRenames,
      );
      const updatedIndexesByProperty = this.updateConstraintColumns(
        updatedIndexesByColumn,
        propertyNameRenames,
      );

      const finalUniques = updatedUniquesByProperty;
      const finalIndexes = updatedIndexesByProperty;

      if (JSON.stringify(finalUniques) !== JSON.stringify(oldUniques)) {
        diff.constraints.uniques.update = finalUniques;
        diff.metadataUpdate = diff.metadataUpdate || {};
        diff.metadataUpdate.uniques = finalUniques;
      }

      if (
        JSON.stringify(finalIndexes) !==
        JSON.stringify(oldMetadata.indexes || [])
      ) {
        diff.constraints.indexes.update = finalIndexes;
        diff.metadataUpdate = diff.metadataUpdate || {};
        diff.metadataUpdate.indexes = finalIndexes;
      }
    } else if (!this.arraysEqual(oldUniques, newUniques)) {
      diff.constraints.uniques.update = newUniques;
    }

    const oldIndexes = (oldMetadata.indexes || []).map((idx: any) =>
      this.normalizeIndexColumns(idx),
    );
    const newIndexes = (newMetadata.indexes || []).map((idx: any) =>
      this.normalizeIndexColumns(idx),
    );
    const oldIndexKeys = new Set(
      oldIndexes.map((cols: string[]) => this.indexKey(cols)),
    );
    const newIndexKeys = new Set(
      newIndexes.map((cols: string[]) => this.indexKey(cols)),
    );
    const toDelete = oldIndexes.filter(
      (cols: string[]) =>
        cols.length > 0 && !newIndexKeys.has(this.indexKey(cols)),
    );
    const toCreate = newIndexes.filter(
      (cols: string[]) =>
        cols.length > 0 && !oldIndexKeys.has(this.indexKey(cols)),
    );
    if (toDelete.length > 0) {
      diff.constraints.indexes.delete = toDelete;
    }
    if (toCreate.length > 0) {
      diff.constraints.indexes.create = toCreate;
    }
  }

  hasColumnChanged(oldCol: any, newCol: any): boolean {
    return (
      oldCol.type !== newCol.type ||
      oldCol.isNullable !== newCol.isNullable ||
      oldCol.isGenerated !== newCol.isGenerated ||
      JSON.stringify(oldCol.defaultValue) !==
        JSON.stringify(newCol.defaultValue) ||
      JSON.stringify(oldCol.options) !== JSON.stringify(newCol.options)
    );
  }

  normalizeIndexColumns(idx: any): string[] {
    if (Array.isArray(idx)) return idx;
    if (idx && Array.isArray(idx.value)) return idx.value;
    return [];
  }

  indexKey(cols: string[]): string {
    return cols.slice().sort().join(',');
  }

  arraysEqual(arr1: any[], arr2: any[]): boolean {
    if (arr1.length !== arr2.length) {
      return false;
    }
    const sorted1 = [...arr1].sort();
    const sorted2 = [...arr2].sort();
    for (let i = 0; i < sorted1.length; i++) {
      if (Array.isArray(sorted1[i]) && Array.isArray(sorted2[i])) {
        if (!this.arraysEqual(sorted1[i], sorted2[i])) {
          return false;
        }
      } else {
        if (sorted1[i] !== sorted2[i]) {
          return false;
        }
      }
    }
    return true;
  }

  updateConstraintColumns(
    constraints: any[],
    columnRenames: Map<string, string>,
  ): any[] {
    if (!constraints || constraints.length === 0) return constraints;

    return constraints.map((constraint) => {
      if (Array.isArray(constraint)) {
        return constraint.map((col) => columnRenames.get(col) || col);
      }
      return constraint;
    });
  }

  async updateMetadataIndexes(
    tableName: string,
    userDefinedIndexes: string[][],
    autoGeneratedIndexes: string[][],
  ): Promise<void> {
    if (autoGeneratedIndexes.length === 0) return;

    const knex = this.knexService.getKnex();

    const existingIndexKeys = new Set(
      userDefinedIndexes.map((idx) => this.indexKey(idx)),
    );

    const newIndexes: string[][] = [];
    for (const autoIdx of autoGeneratedIndexes) {
      if (!existingIndexKeys.has(this.indexKey(autoIdx))) {
        newIndexes.push(autoIdx);
      }
    }

    if (newIndexes.length === 0) return;

    const mergedIndexes = [...userDefinedIndexes, ...newIndexes];

    try {
      await knex('table_definition')
        .where('name', tableName)
        .update({ indexes: JSON.stringify(mergedIndexes) });
    } catch (error) {
      this.logger.error(
        `  Failed to update indexes metadata for ${tableName}: ${getErrorMessage(error)}`,
      );
    }
  }

  isSystemColumn(columnName: string): boolean {
    const systemColumns = ['id', 'createdAt', 'updatedAt'];
    return systemColumns.includes(columnName);
  }

  async executeSchemaDiff(
    tableName: string,
    diff: any,
    trx?: any,
    journal?: JournalContext,
  ): Promise<string> {
    const knex = this.knexService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType() as
      | 'mysql'
      | 'postgres'
      | 'sqlite';
    const sqlStatements = await generateSQLFromDiff(
      knex,
      tableName,
      diff,
      dbType,
      this.metadataCacheService,
    );
    const batchSQL = generateBatchSQL(sqlStatements);
    await executeBatchSQL(knex, batchSQL, dbType, trx, journal);
    return batchSQL;
  }
}
