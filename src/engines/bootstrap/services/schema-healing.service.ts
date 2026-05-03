import { Logger } from '../../../shared/logger';
import {
  QueryBuilderService,
  getForeignKeyColumnName,
  getJunctionColumnNames,
  getJunctionTableName,
  getShortFkConstraintName,
} from '@enfyra/kernel';
import type { Knex } from 'knex';
import { MetadataCacheService } from '../../cache';
import { DatabaseConfigService } from '../../../shared/services';
import {
  MONGO_PRIMARY_KEY_NAME,
  MONGO_PRIMARY_KEY_TYPE,
} from '../../../modules/table-management/utils/mongo-primary-key.util';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';
import { buildSqlJunctionTableContract } from '../../knex/utils/sql-physical-schema-contract';

export class SchemaHealingService {
  private readonly logger = new Logger(SchemaHealingService.name);
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
    const junctionContractRepairCount = isMongoDB
      ? await this.healMongoJunctionContracts()
      : await this.healSqlJunctionContracts();

    if (relationPhysicalMappingRepairCount > 0) {
      this.logger.log(
        `Repaired relation physical metadata on ${relationPhysicalMappingRepairCount} relation(s)`,
      );
    }
    if (junctionContractRepairCount > 0) {
      this.logger.log(
        `Healed many-to-many junction contract on ${junctionContractRepairCount} relation(s)`,
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

  private async healSqlJunctionContracts(): Promise<number> {
    const knex = this.queryBuilderService.getKnex();
    const rows = await knex('relation_definition as r')
      .leftJoin(
        'table_definition as sourceTable',
        'r.sourceTableId',
        'sourceTable.id',
      )
      .leftJoin(
        'table_definition as targetTable',
        'r.targetTableId',
        'targetTable.id',
      )
      .select(
        'r.*',
        'sourceTable.name as sourceTableName',
        'targetTable.name as targetTableName',
      );
    const byMappedById = new Map<string, any[]>();
    for (const rel of rows) {
      if (!rel.mappedById) continue;
      const key = String(rel.mappedById);
      const list = byMappedById.get(key) || [];
      list.push(rel);
      byMappedById.set(key, list);
    }

    let repaired = 0;
    for (const rel of rows) {
      if (rel.type !== 'many-to-many' || rel.mappedById) continue;
      if (!rel.sourceTableName || !rel.targetTableName || !rel.propertyName) {
        continue;
      }

      const standard = getSqlJunctionPhysicalNames({
        sourceTable: rel.sourceTableName,
        propertyName: rel.propertyName,
        targetTable: rel.targetTableName,
      });
      const oldJunctionTableName = rel.junctionTableName || null;
      await this.ensureSqlJunctionPhysicalTable(knex, {
        oldJunctionTableName,
        oldJunctionSourceColumn: rel.junctionSourceColumn || null,
        oldJunctionTargetColumn: rel.junctionTargetColumn || null,
        sourceTable: rel.sourceTableName,
        targetTable: rel.targetTableName,
        sourcePropertyName: rel.propertyName,
        junctionTableName: standard.junctionTableName,
        junctionSourceColumn: standard.junctionSourceColumn,
        junctionTargetColumn: standard.junctionTargetColumn,
      });

      const owningUpdate = this.diffJunctionMetadata(rel, standard);
      if (Object.keys(owningUpdate).length > 0) {
        await knex('relation_definition').where({ id: rel.id }).update(owningUpdate);
        repaired++;
      }

      for (const inverseRel of byMappedById.get(String(rel.id)) || []) {
        const inverseStandard = {
          junctionTableName: standard.junctionTableName,
          junctionSourceColumn: standard.junctionTargetColumn,
          junctionTargetColumn: standard.junctionSourceColumn,
        };
        const inverseUpdate = this.diffJunctionMetadata(
          inverseRel,
          inverseStandard,
        );
        if (Object.keys(inverseUpdate).length === 0) continue;
        await knex('relation_definition')
          .where({ id: inverseRel.id })
          .update(inverseUpdate);
        repaired++;
      }
    }

    return repaired;
  }

  private async ensureSqlJunctionPhysicalTable(
    knex: Knex,
    input: {
      oldJunctionTableName: string | null;
      oldJunctionSourceColumn: string | null;
      oldJunctionTargetColumn: string | null;
      sourceTable: string;
      targetTable: string;
      sourcePropertyName: string;
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
    },
  ): Promise<void> {
    const standardExists = await knex.schema.hasTable(input.junctionTableName);
    if (standardExists) {
      await this.ensureSqlJunctionColumns(knex, input);
      return;
    }

    if (
      input.oldJunctionTableName &&
      input.oldJunctionTableName !== input.junctionTableName &&
      (await knex.schema.hasTable(input.oldJunctionTableName))
    ) {
      await knex.schema.renameTable(
        input.oldJunctionTableName,
        input.junctionTableName,
      );
      this.logger.log(
        `Renamed junction table '${input.oldJunctionTableName}' to '${input.junctionTableName}'`,
      );
      await this.ensureSqlJunctionColumns(knex, input);
      return;
    }

    const sourceExists = await knex.schema.hasTable(input.sourceTable);
    const targetExists = await knex.schema.hasTable(input.targetTable);
    if (!sourceExists || !targetExists) return;

    const junction = buildSqlJunctionTableContract({
      tableName: input.junctionTableName,
      sourceTable: input.sourceTable,
      targetTable: input.targetTable,
      sourceColumn: input.junctionSourceColumn,
      targetColumn: input.junctionTargetColumn,
      sourcePropertyName: input.sourcePropertyName,
    });
    const sourcePkType = await this.getSqlPrimaryKeyType(input.sourceTable);
    const targetPkType = await this.getSqlPrimaryKeyType(input.targetTable);
    const dbType = this.queryBuilderService.getDatabaseType?.() || 'postgres';

    await knex.schema.createTable(junction.tableName, (table) => {
      this.addSqlJunctionColumn(table, junction.sourceColumn, sourcePkType, dbType)
        .notNullable();
      this.addSqlJunctionColumn(table, junction.targetColumn, targetPkType, dbType)
        .notNullable();
      table.primary([junction.sourceColumn, junction.targetColumn], junction.primaryKeyName);
      table
        .foreign(junction.sourceColumn)
        .references('id')
        .inTable(junction.sourceTable)
        .onDelete(junction.onDelete)
        .onUpdate(junction.onUpdate)
        .withKeyName(junction.sourceForeignKeyName);
      table
        .foreign(junction.targetColumn)
        .references('id')
        .inTable(junction.targetTable)
        .onDelete(junction.onDelete)
        .onUpdate(junction.onUpdate)
        .withKeyName(junction.targetForeignKeyName);
      table.index([junction.sourceColumn], junction.sourceIndexName);
      table.index([junction.targetColumn], junction.targetIndexName);
      table.index(
        [junction.targetColumn, junction.sourceColumn],
        junction.reverseIndexName,
      );
    });
    this.logger.log(`Created missing junction table '${junction.tableName}'`);
  }

  private async ensureSqlJunctionColumns(
    knex: Knex,
    input: {
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
      oldJunctionSourceColumn: string | null;
      oldJunctionTargetColumn: string | null;
    },
  ): Promise<void> {
    await this.renameSqlJunctionColumnIfNeeded(
      knex,
      input.junctionTableName,
      input.oldJunctionSourceColumn,
      input.junctionSourceColumn,
    );
    await this.renameSqlJunctionColumnIfNeeded(
      knex,
      input.junctionTableName,
      input.oldJunctionTargetColumn,
      input.junctionTargetColumn,
    );
  }

  private async renameSqlJunctionColumnIfNeeded(
    knex: Knex,
    tableName: string,
    oldColumn: string | null,
    newColumn: string,
  ): Promise<void> {
    if (!oldColumn || oldColumn === newColumn) return;
    const oldExists = await knex.schema.hasColumn(tableName, oldColumn);
    const newExists = await knex.schema.hasColumn(tableName, newColumn);
    if (!oldExists || newExists) return;
    await knex.schema.alterTable(tableName, (table) => {
      table.renameColumn(oldColumn, newColumn);
    });
    this.logger.log(
      `Renamed junction column '${tableName}.${oldColumn}' to '${newColumn}'`,
    );
  }

  private addSqlJunctionColumn(
    table: Knex.CreateTableBuilder,
    columnName: string,
    pkType: 'uuid' | 'varchar' | 'integer',
    dbType: string,
  ): Knex.ColumnBuilder {
    if (pkType === 'uuid') {
      return dbType === 'postgres'
        ? table.uuid(columnName)
        : table.string(columnName, 36);
    }
    if (pkType === 'varchar') {
      return table.string(columnName, 255);
    }
    return dbType === 'mysql'
      ? table.integer(columnName).unsigned()
      : table.integer(columnName);
  }

  private async getSqlPrimaryKeyType(
    tableName: string,
  ): Promise<'uuid' | 'varchar' | 'integer'> {
    const table = await this.metadataCacheService.lookupTableByName?.(tableName);
    const primaryColumn = table?.columns?.find((column: any) => column.isPrimary);
    const type = String(primaryColumn?.type || '').toLowerCase();
    if (type === 'uuid' || type === 'uuidv4' || type.includes('uuid')) {
      return 'uuid';
    }
    if (type === 'varchar' || type === 'string' || type === 'char') {
      return 'varchar';
    }
    return 'integer';
  }

  private async healMongoJunctionContracts(): Promise<number> {
    const db = this.queryBuilderService.getMongoDb();
    const relations = await db.collection('relation_definition').find({}).toArray();
    const tables = await db.collection('table_definition').find({}).toArray();
    const tableById = new Map<string, any>(
      tables.map((table: any) => [String(table._id), table]),
    );
    const byMappedBy = new Map<string, any[]>();
    for (const rel of relations) {
      if (!rel.mappedBy) continue;
      const key = String(rel.mappedBy);
      const list = byMappedBy.get(key) || [];
      list.push(rel);
      byMappedBy.set(key, list);
    }

    let repaired = 0;
    for (const rel of relations) {
      if (rel.type !== 'many-to-many' || rel.mappedBy) continue;
      const sourceTable = tableById.get(String(rel.sourceTable));
      const targetTable = tableById.get(String(rel.targetTable));
      if (!sourceTable?.name || !targetTable?.name || !rel.propertyName) {
        continue;
      }

      const junctionTableName = getJunctionTableName(
        sourceTable.name,
        rel.propertyName,
        targetTable.name,
      );
      const columns = getJunctionColumnNames(
        sourceTable.name,
        rel.propertyName,
        targetTable.name,
      );
      await this.ensureMongoJunctionCollection(db, {
        oldJunctionTableName: rel.junctionTableName || null,
        oldJunctionSourceColumn: rel.junctionSourceColumn || null,
        oldJunctionTargetColumn: rel.junctionTargetColumn || null,
        junctionTableName,
        junctionSourceColumn: columns.sourceColumn,
        junctionTargetColumn: columns.targetColumn,
      });

      const owningUpdate = this.diffJunctionMetadata(rel, {
        junctionTableName,
        junctionSourceColumn: columns.sourceColumn,
        junctionTargetColumn: columns.targetColumn,
      });
      if (Object.keys(owningUpdate).length > 0) {
        await db
          .collection('relation_definition')
          .updateOne({ _id: rel._id }, { $set: owningUpdate });
        repaired++;
      }

      for (const inverseRel of byMappedBy.get(String(rel._id)) || []) {
        const inverseUpdate = this.diffJunctionMetadata(inverseRel, {
          junctionTableName,
          junctionSourceColumn: columns.targetColumn,
          junctionTargetColumn: columns.sourceColumn,
        });
        if (Object.keys(inverseUpdate).length === 0) continue;
        await db
          .collection('relation_definition')
          .updateOne({ _id: inverseRel._id }, { $set: inverseUpdate });
        repaired++;
      }
    }

    return repaired;
  }

  private async ensureMongoJunctionCollection(
    db: any,
    input: {
      oldJunctionTableName: string | null;
      oldJunctionSourceColumn: string | null;
      oldJunctionTargetColumn: string | null;
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
    },
  ): Promise<void> {
    const standardExists = await this.mongoCollectionExists(
      db,
      input.junctionTableName,
    );
    if (!standardExists) {
      const oldExists =
        input.oldJunctionTableName &&
        input.oldJunctionTableName !== input.junctionTableName &&
        (await this.mongoCollectionExists(db, input.oldJunctionTableName));
      if (oldExists) {
        await db
          .collection(input.oldJunctionTableName)
          .rename(input.junctionTableName);
        this.logger.log(
          `Renamed junction collection '${input.oldJunctionTableName}' to '${input.junctionTableName}'`,
        );
      } else {
        await db.createCollection(input.junctionTableName);
        this.logger.log(
          `Created missing junction collection '${input.junctionTableName}'`,
        );
      }
    }

    const collection = db.collection(input.junctionTableName);
    await this.renameMongoJunctionFieldIfNeeded(
      collection,
      input.oldJunctionSourceColumn,
      input.junctionSourceColumn,
    );
    await this.renameMongoJunctionFieldIfNeeded(
      collection,
      input.oldJunctionTargetColumn,
      input.junctionTargetColumn,
    );
    try {
      await collection.createIndex(
        {
          [input.junctionSourceColumn]: 1,
          [input.junctionTargetColumn]: 1,
        },
        { unique: true, name: `${input.junctionTableName}_src_tgt_uq` },
      );
    } catch (error: any) {
      if (error.code !== 85 && error.code !== 86) throw error;
    }
    try {
      await collection.createIndex(
        { [input.junctionTargetColumn]: 1 },
        { name: `${input.junctionTableName}_tgt_idx` },
      );
    } catch (error: any) {
      if (error.code !== 85 && error.code !== 86) throw error;
    }
  }

  private async mongoCollectionExists(db: any, name: string): Promise<boolean> {
    const existing = await db.listCollections({ name }).toArray();
    return existing.length > 0;
  }

  private async renameMongoJunctionFieldIfNeeded(
    collection: any,
    oldField: string | null,
    newField: string,
  ): Promise<void> {
    if (!oldField || oldField === newField) return;
    await collection.updateMany(
      { [oldField]: { $exists: true }, [newField]: { $exists: false } },
      { $rename: { [oldField]: newField } },
    );
  }

  private diffJunctionMetadata(
    rel: any,
    expected: {
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
    },
  ): any {
    const updateData: any = {};
    if (rel.junctionTableName !== expected.junctionTableName) {
      updateData.junctionTableName = expected.junctionTableName;
    }
    if (rel.junctionSourceColumn !== expected.junctionSourceColumn) {
      updateData.junctionSourceColumn = expected.junctionSourceColumn;
    }
    if (rel.junctionTargetColumn !== expected.junctionTargetColumn) {
      updateData.junctionTargetColumn = expected.junctionTargetColumn;
    }
    return updateData;
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
