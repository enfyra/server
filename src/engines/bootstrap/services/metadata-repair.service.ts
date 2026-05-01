import { Logger } from '../../../shared/logger';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
  getShortFkConstraintName,
} from '@enfyra/kernel';
import type { Knex } from 'knex';
import { MetadataCacheService } from '../../cache';
import { DatabaseConfigService } from '../../../shared/services';
import {
  MONGO_PRIMARY_KEY_NAME,
  MONGO_PRIMARY_KEY_TYPE,
} from '../../../modules/table-management/utils/mongo-primary-key.util';

export class MetadataRepairService {
  private readonly logger = new Logger(MetadataRepairService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataCacheService: MetadataCacheService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataCacheService: MetadataCacheService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataCacheService = deps.metadataCacheService;
  }

  async runIfNeeded(): Promise<void> {
    const setting = await this.loadSetting();
    if (!setting) return;

    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();

    const relationPhysicalMappingRepairCount =
      await this.repairRelationPhysicalMappings(isMongoDB);

    if (relationPhysicalMappingRepairCount > 0) {
      this.logger.log(
        `Repaired relation physical metadata on ${relationPhysicalMappingRepairCount} relation(s)`,
      );
    }

    const mongoSystemShapeRepairCount = isMongoDB
      ? await this.repairMongoSystemRecordShapes()
      : 0;

    if (mongoSystemShapeRepairCount > 0) {
      this.logger.log(
        `Repaired Mongo system record shapes on ${mongoSystemShapeRepairCount} collection(s)`,
      );
    }

    const mongoPrimaryKeyRepairCount = isMongoDB
      ? await this.repairMongoPrimaryKeyColumns()
      : 0;

    if (mongoPrimaryKeyRepairCount > 0) {
      this.logger.log(
        `Repaired Mongo primary key metadata on ${mongoPrimaryKeyRepairCount} table(s)`,
      );
    }

    if (setting.uniquesIndexesRepaired !== true) {
      const repairedCount = await this.repairUserTables();
      await this.markRepaired(setting);

      if (repairedCount > 0) {
        this.logger.log(
          `Repaired uniques/indexes metadata on ${repairedCount} user table(s)`,
        );
      }
    }
  }

  private async repairRelationPhysicalMappings(
    isMongoDB: boolean,
  ): Promise<number> {
    if (isMongoDB) {
      return this.repairMongoRelationPhysicalMappings();
    }
    return this.repairSqlRelationPhysicalMappings();
  }

  private async repairSqlRelationPhysicalMappings(): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    const rows = await knex('relation_definition as r')
      .leftJoin(
        'table_definition as sourceTable',
        'r.sourceTableId',
        'sourceTable.id',
      )
      .select('r.*', 'sourceTable.name as sourceTableName');
    let repaired = 0;

    for (const rel of rows) {
      if (!this.isSqlOwningRelation(rel)) continue;

      const foreignKeyColumn =
        rel.foreignKeyColumn || getForeignKeyColumnName(rel.propertyName);
      const referencedColumn = rel.referencedColumn || 'id';
      const constraintName =
        rel.constraintName ||
        (await this.findSqlForeignKeyConstraintName(
          knex,
          rel.sourceTableName,
          foreignKeyColumn,
        )) ||
        getShortFkConstraintName(rel.sourceTableName, foreignKeyColumn, 'src');
      const updateData: any = {};

      if (!rel.foreignKeyColumn) updateData.foreignKeyColumn = foreignKeyColumn;
      if (!rel.referencedColumn) updateData.referencedColumn = referencedColumn;
      if (!rel.constraintName) updateData.constraintName = constraintName;
      if (Object.keys(updateData).length === 0) continue;

      await knex('relation_definition').where({ id: rel.id }).update(updateData);
      repaired++;
    }

    return repaired;
  }

  private async repairMongoRelationPhysicalMappings(): Promise<number> {
    const collection = this.queryBuilderService
      .getMongoDb()
      .collection('relation_definition');
    const relations = await collection.find({}).toArray();
    const relationsById = new Map(
      relations.map((rel: any) => [String(rel._id), rel]),
    );
    let repaired = 0;

    for (const rel of relations) {
      const owningRel = rel.mappedBy
        ? relationsById.get(String(rel.mappedBy))
        : null;
      const updateData: any = {};

      if (!this.hasOwn(rel, 'foreignKeyColumn')) {
        updateData.foreignKeyColumn = this.getMongoRelationForeignKeyColumn(
          rel,
          owningRel,
        );
      }
      if (!this.hasOwn(rel, 'referencedColumn')) {
        const hasForeignKeyColumn =
          rel.foreignKeyColumn ||
          updateData.foreignKeyColumn ||
          this.isMongoOwningRelation(rel);
        updateData.referencedColumn = hasForeignKeyColumn
          ? MONGO_PRIMARY_KEY_NAME
          : null;
      }
      if (!this.hasOwn(rel, 'constraintName')) {
        updateData.constraintName = null;
      }
      if (!this.hasOwn(rel, 'junctionTableName')) {
        updateData.junctionTableName = null;
      }
      if (!this.hasOwn(rel, 'junctionSourceColumn')) {
        updateData.junctionSourceColumn = null;
      }
      if (!this.hasOwn(rel, 'junctionTargetColumn')) {
        updateData.junctionTargetColumn = null;
      }
      if (Object.keys(updateData).length === 0) continue;

      await collection.updateOne({ _id: rel._id }, { $set: updateData });
      repaired++;
    }

    return repaired;
  }

  private async repairMongoSystemRecordShapes(): Promise<number> {
    const db = this.queryBuilderService.getMongoDb();
    const tables = await db
      .collection('table_definition')
      .find({ isSystem: true })
      .toArray();
    let repairedCollections = 0;

    for (const table of tables) {
      const tableId = table._id;
      const columns = await db
        .collection('column_definition')
        .find({ table: tableId })
        .toArray();
      const missingFieldSet: Record<string, any> = {};

      for (const column of columns) {
        if (!column.name || column.name === MONGO_PRIMARY_KEY_NAME) continue;
        missingFieldSet[column.name] = this.getMongoColumnDefaultValue(column);
      }
      if (Object.keys(missingFieldSet).length === 0) continue;

      let modified = 0;
      for (const [field, value] of Object.entries(missingFieldSet)) {
        const result = await db
          .collection(table.name)
          .updateMany({ [field]: { $exists: false } }, { $set: { [field]: value } });
        modified += result.modifiedCount;
      }

      if (modified > 0) {
        repairedCollections++;
      }
    }

    return repairedCollections;
  }

  private isSqlOwningRelation(rel: any): boolean {
    return (
      rel.type === 'many-to-one' ||
      (rel.type === 'one-to-one' && !rel.mappedById)
    );
  }

  private isMongoOwningRelation(rel: any): boolean {
    return (
      rel.type === 'many-to-one' ||
      (rel.type === 'one-to-one' && !rel.mappedBy)
    );
  }

  private getMongoRelationForeignKeyColumn(rel: any, owningRel: any): string | null {
    if (this.isMongoOwningRelation(rel)) {
      return rel.propertyName || null;
    }
    if (
      (rel.type === 'one-to-many' ||
        (rel.type === 'one-to-one' && rel.mappedBy)) &&
      owningRel
    ) {
      return owningRel.foreignKeyColumn || owningRel.propertyName || null;
    }
    return null;
  }

  private getMongoColumnDefaultValue(column: any): any {
    if (this.hasOwn(column, 'defaultValue') && column.defaultValue !== undefined) {
      return column.defaultValue;
    }
    return null;
  }

  private hasOwn(value: any, key: string): boolean {
    return Object.prototype.hasOwnProperty.call(value, key);
  }

  private async findSqlForeignKeyConstraintName(
    knex: Knex,
    tableName: string,
    columnName: string,
  ): Promise<string | null> {
    const client = String((knex.client.config as any).client || '');
    if (client === 'pg') {
      const result = await knex.raw(
        `
        SELECT tc.constraint_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = current_schema()
          AND tc.table_name = ?
          AND kcu.column_name = ?
        LIMIT 1
      `,
        [tableName, columnName],
      );
      return result.rows?.[0]?.constraint_name || null;
    }
    if (client === 'mysql2') {
      const result = await knex.raw(
        `
        SELECT CONSTRAINT_NAME AS constraint_name
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = ?
          AND COLUMN_NAME = ?
          AND REFERENCED_TABLE_NAME IS NOT NULL
        LIMIT 1
      `,
        [tableName, columnName],
      );
      return result[0]?.[0]?.constraint_name || null;
    }
    return null;
  }

  private async repairMongoPrimaryKeyColumns(): Promise<number> {
    const result = await this.queryBuilderService.find({
      table: 'column_definition',
      limit: 10000,
    });
    const columns = result?.data || [];
    let repaired = 0;
    const idField = DatabaseConfigService.getPkField();

    for (const primaryColumn of columns) {
      if (primaryColumn.isPrimary !== true) continue;
      if (
        primaryColumn.name !== MONGO_PRIMARY_KEY_NAME &&
        primaryColumn.name !== 'id'
      ) {
        continue;
      }
      if (
        primaryColumn.name === MONGO_PRIMARY_KEY_NAME &&
        primaryColumn.type === MONGO_PRIMARY_KEY_TYPE
      ) {
        continue;
      }

      const columnId = primaryColumn._id ?? primaryColumn.id;
      if (!columnId) continue;

      await this.queryBuilderService.update(
        'column_definition',
        { where: [{ field: idField, operator: '=', value: columnId }] },
        { name: MONGO_PRIMARY_KEY_NAME, type: MONGO_PRIMARY_KEY_TYPE },
      );
      repaired++;
      this.logger.log(
        `Repaired Mongo primary key column '${primaryColumn.name}' from type '${primaryColumn.type}' to '${MONGO_PRIMARY_KEY_NAME}' '${MONGO_PRIMARY_KEY_TYPE}'`,
      );
    }

    return repaired;
  }

  private async repairUserTables(): Promise<number> {
    const tables = await this.metadataCacheService.getAllTablesMetadata();
    let repaired = 0;

    for (const table of tables) {
      if (table.isSystem === true) continue;

      const fkToProperty = this.buildFkToPropertyMap(table);
      if (fkToProperty.size === 0) continue;

      const originalUniques = this.parseArray(table.uniques);
      const originalIndexes = this.parseArray(table.indexes);

      const newUniques = this.normalizeGroups(originalUniques, fkToProperty);
      const newIndexes = this.normalizeGroups(originalIndexes, fkToProperty);

      const uniquesChanged =
        JSON.stringify(originalUniques) !== JSON.stringify(newUniques);
      const indexesChanged =
        JSON.stringify(originalIndexes) !== JSON.stringify(newIndexes);

      if (!uniquesChanged && !indexesChanged) continue;

      const idField = DatabaseConfigService.getPkField();
      await this.queryBuilderService.update(
        'table_definition',
        { where: [{ field: idField, operator: '=', value: table.id }] },
        { uniques: newUniques, indexes: newIndexes },
      );
      repaired++;
      this.logger.log(
        `Repaired '${table.name}': uniques ${JSON.stringify(originalUniques)} → ${JSON.stringify(newUniques)}, indexes ${JSON.stringify(originalIndexes)} → ${JSON.stringify(newIndexes)}`,
      );
    }

    return repaired;
  }

  private buildFkToPropertyMap(table: any): Map<string, string> {
    const map = new Map<string, string>();
    for (const rel of table.relations || []) {
      if (!rel.foreignKeyColumn || !rel.propertyName) continue;
      if (rel.foreignKeyColumn === rel.propertyName) continue;
      map.set(rel.foreignKeyColumn, rel.propertyName);
    }
    return map;
  }

  private normalizeGroups(
    groups: string[][],
    fkToProperty: Map<string, string>,
  ): string[][] {
    return groups.map((group) =>
      group.map((entry) => fkToProperty.get(entry) ?? entry),
    );
  }

  private parseArray(value: any): string[][] {
    if (Array.isArray(value)) return value;
    if (typeof value === 'string') {
      try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed : [];
      } catch {
        return [];
      }
    }
    return [];
  }

  private async loadSetting(): Promise<any | null> {
    const sortField = DatabaseConfigService.getPkField();
    try {
      const result = await this.queryBuilderService.find({
        table: 'setting_definition',
        sort: [sortField],
        limit: 1,
      });
      return result?.data?.[0] ?? null;
    } catch {
      return null;
    }
  }

  private async markRepaired(setting: any): Promise<void> {
    const idField = DatabaseConfigService.getPkField();
    const settingId = setting._id || setting.id;
    await this.queryBuilderService.update(
      'setting_definition',
      { where: [{ field: idField, operator: '=', value: settingId }] },
      { uniquesIndexesRepaired: true },
    );
  }
}
