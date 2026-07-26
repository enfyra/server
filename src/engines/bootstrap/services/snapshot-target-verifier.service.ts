import type { QueryBuilderService } from '@enfyra/kernel';
import type { Knex } from 'knex';
import type { Db } from 'mongodb';
import type { SchemaMigrationDef } from '../../../shared/types/schema-migration.types';
import { buildMongoFullIndexSpecs } from '../../mongo';
import {
  buildSqlForeignKeyContracts,
  buildSqlJunctionTableContractFromRelation,
} from '../../knex/utils/sql-physical-schema-contract';
import {
  compareSchemas,
  getCurrentDatabaseSchema,
} from '../../knex/utils/provision/schema-comparison';
import { parseSnapshotToSchema } from '../../knex/utils/provision/schema-parser';
import { getSqlJunctionPhysicalNames } from '../../../modules/table-management/utils/sql-junction-naming.util';
import type { DataMigrationService } from './data-migration.service';
import type { MetadataMigrationService } from './metadata-migration.service';
import { BootstrapDefinitionService } from './bootstrap-definition.service';

type SqlForeignKeyState = {
  columnName: string;
  constraintName: string;
  targetTable: string;
  targetColumn: string;
  onDelete: string;
};

export class SnapshotTargetVerifierService {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly metadataMigrationService: MetadataMigrationService;
  private readonly dataMigrationService: DataMigrationService;
  private readonly bootstrapDefinitionService: BootstrapDefinitionService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    metadataMigrationService: MetadataMigrationService;
    dataMigrationService: DataMigrationService;
    bootstrapDefinitionService?: BootstrapDefinitionService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.metadataMigrationService = deps.metadataMigrationService;
    this.dataMigrationService = deps.dataMigrationService;
    this.bootstrapDefinitionService =
      deps.bootstrapDefinitionService ?? new BootstrapDefinitionService();
  }

  async assertSchemaTargetState(): Promise<void> {
    await this.metadataMigrationService.assertSnapshotTargetStateAfterHealing();

    const snapshot = this.loadSnapshot();
    const migration = this.loadMigration();
    const errors = this.queryBuilderService.isMongoDb()
      ? await this.collectMongoErrors(snapshot, migration)
      : await this.collectSqlErrors(snapshot, migration);

    if (errors.length > 0) {
      throw new Error(
        `Snapshot physical target attestation failed:\n- ${errors.join('\n- ')}`,
      );
    }
  }

  async assertDataTargetState(): Promise<void> {
    await this.dataMigrationService.assertTargetState();
  }

  private loadSnapshot(): Record<string, any> {
    return this.bootstrapDefinitionService.getSnapshot();
  }

  private loadMigration(): SchemaMigrationDef | null {
    return this.bootstrapDefinitionService.getMigration();
  }

  private async collectSqlErrors(
    snapshot: Record<string, any>,
    migration: SchemaMigrationDef | null,
  ): Promise<string[]> {
    const knex = this.queryBuilderService.getKnex();
    const errors: string[] = [];

    for (const schema of parseSnapshotToSchema(snapshot)) {
      if (!(await knex.schema.hasTable(schema.tableName))) {
        errors.push(`physical table ${schema.tableName} is missing`);
        continue;
      }

      const current = await getCurrentDatabaseSchema(knex, schema.tableName);
      const diff = compareSchemas(schema, current);
      for (const column of diff.columnsToAdd) {
        errors.push(
          `physical column ${schema.tableName}.${column.name} is missing`,
        );
      }
      for (const { column, changes } of diff.columnsToModify) {
        const currentColumn = current.columns.find(
          (candidate) => candidate.name === column.name,
        );
        const targetChanges = changes.filter(
          (change) =>
            change !== 'type' ||
            column.type !== 'simple-json' ||
            !currentColumn ||
            !/json|text/i.test(currentColumn.type),
        );
        if (targetChanges.length === 0) continue;
        errors.push(
          `physical column ${schema.tableName}.${column.name} differs on ${targetChanges.join(', ')}`,
        );
      }
      for (const columns of diff.uniquesToAdd) {
        errors.push(
          `physical unique ${schema.tableName}(${columns.join(', ')}) is missing`,
        );
      }
      for (const columns of diff.indexesToAdd) {
        errors.push(
          `physical index ${schema.tableName}(${columns.join(', ')}) is missing`,
        );
      }
      await this.collectSqlForeignKeyErrors(
        knex,
        schema.tableName,
        schema.definition,
        current.columns,
        errors,
      );
    }

    await this.collectSqlJunctionErrors(knex, snapshot, errors);
    await this.collectSqlLegacyErrors(knex, migration, errors);
    return errors;
  }

  private async collectSqlForeignKeyErrors(
    knex: Knex,
    tableName: string,
    table: Record<string, any>,
    columns: Array<{ name: string; isNullable: boolean }>,
    errors: string[],
  ): Promise<void> {
    const expected = buildSqlForeignKeyContracts(
      tableName,
      table.relations ?? [],
    );
    const actual = await this.readSqlForeignKeys(knex, tableName);

    for (const target of expected) {
      const current = actual.find(
        (foreignKey) => foreignKey.columnName === target.columnName,
      );
      if (!current) {
        errors.push(
          `physical foreign key ${tableName}.${target.columnName} is missing`,
        );
        continue;
      }
      const differences: string[] = [];
      if (current.targetTable !== target.targetTable) {
        differences.push('targetTable');
      }
      if (current.targetColumn !== target.targetColumn) {
        differences.push('targetColumn');
      }
      if (current.onDelete.toUpperCase() !== target.onDelete.toUpperCase()) {
        differences.push('onDelete');
      }
      const relation = (table.relations ?? []).find(
        (candidate: any) => candidate.propertyName === target.propertyName,
      );
      if (
        relation?.constraintName &&
        current.constraintName !== relation.constraintName
      ) {
        differences.push('constraintName');
      }
      const column = columns.find(
        (candidate) => candidate.name === target.columnName,
      );
      if (column && column.isNullable !== target.nullable) {
        differences.push('nullable');
      }
      if (differences.length > 0) {
        errors.push(
          `physical foreign key ${tableName}.${target.columnName} differs on ${differences.join(', ')}`,
        );
      }
    }
  }

  private async collectSqlJunctionErrors(
    knex: Knex,
    snapshot: Record<string, any>,
    errors: string[],
  ): Promise<void> {
    const visited = new Set<string>();
    const generatedInverseKeys = new Set<string>();
    for (const [sourceTable, table] of Object.entries(snapshot)) {
      for (const relation of (table as any).relations ?? []) {
        const relationKey = `${sourceTable}.${relation.propertyName}`;
        if (generatedInverseKeys.has(relationKey)) continue;
        if (relation.inversePropertyName) {
          generatedInverseKeys.add(
            `${relation.targetTable}.${relation.inversePropertyName}`,
          );
        }
        if (relation.type !== 'many-to-many' || relation.mappedBy) continue;
        const fallback = getSqlJunctionPhysicalNames({
          sourceTable,
          propertyName: relation.propertyName,
          targetTable: relation.targetTable,
        });
        const contract = buildSqlJunctionTableContractFromRelation(
          sourceTable,
          {
            ...relation,
            junctionTableName:
              relation.junctionTableName ?? fallback.junctionTableName,
            junctionSourceColumn:
              relation.junctionSourceColumn ?? fallback.junctionSourceColumn,
            junctionTargetColumn:
              relation.junctionTargetColumn ?? fallback.junctionTargetColumn,
          } as any,
        );
        if (!contract || visited.has(contract.tableName)) continue;
        visited.add(contract.tableName);

        if (!(await knex.schema.hasTable(contract.tableName))) {
          errors.push(
            `physical junction table ${contract.tableName} is missing`,
          );
          continue;
        }
        for (const column of [contract.sourceColumn, contract.targetColumn]) {
          if (!(await knex.schema.hasColumn(contract.tableName, column))) {
            errors.push(
              `physical junction column ${contract.tableName}.${column} is missing`,
            );
          }
        }
        const primaryColumns = await this.readSqlPrimaryKeyColumns(
          knex,
          contract.tableName,
        );
        if (
          this.groupKey(primaryColumns) !==
          this.groupKey([contract.sourceColumn, contract.targetColumn])
        ) {
          errors.push(
            `physical junction primary key ${contract.tableName} differs from ${contract.sourceColumn}, ${contract.targetColumn}`,
          );
        }
        const foreignKeys = await this.readSqlForeignKeys(
          knex,
          contract.tableName,
        );
        for (const target of [
          {
            columnName: contract.sourceColumn,
            targetTable: contract.sourceTable,
          },
          {
            columnName: contract.targetColumn,
            targetTable: contract.targetTable,
          },
        ]) {
          const current = foreignKeys.find(
            (foreignKey) => foreignKey.columnName === target.columnName,
          );
          if (
            !current ||
            current.targetTable !== target.targetTable ||
            current.targetColumn !== 'id' ||
            current.onDelete.toUpperCase() !== 'CASCADE'
          ) {
            errors.push(
              `physical junction foreign key ${contract.tableName}.${target.columnName} differs from target`,
            );
          }
        }
      }
    }
  }

  private async collectSqlLegacyErrors(
    knex: Knex,
    migration: SchemaMigrationDef | null,
    errors: string[],
  ): Promise<void> {
    if (!migration) return;
    const legacyTables = new Set<string>([
      ...(migration.tablesToDrop ?? []),
      ...(migration.physicalTablesToDrop ?? []),
      ...(migration.coreTablesToRename ?? []).map((entry) => entry.from),
      ...(migration.tablesToRename ?? []).map((entry) => entry.from),
      ...(migration.physicalTablesToRename ?? []).map((entry) => entry.from),
    ]);
    for (const tableName of legacyTables) {
      if (tableName && (await knex.schema.hasTable(tableName))) {
        errors.push(`legacy physical table ${tableName} still exists`);
      }
    }

    for (const tableMigration of migration.tables ?? []) {
      const tableName = tableMigration._unique.name._eq;
      for (const columnName of tableMigration.columnsToRemove ?? []) {
        if (await knex.schema.hasColumn(tableName, columnName)) {
          errors.push(
            `legacy physical column ${tableName}.${columnName} still exists`,
          );
        }
      }
      for (const modification of tableMigration.columnsToModify ?? []) {
        if (
          modification.from.name !== modification.to.name &&
          (await knex.schema.hasColumn(tableName, modification.from.name))
        ) {
          errors.push(
            `legacy physical column ${tableName}.${modification.from.name} still exists`,
          );
        }
      }
      for (const propertyName of tableMigration.relationsToRemove ?? []) {
        const field = `${propertyName}Id`;
        if (await knex.schema.hasColumn(tableName, field)) {
          errors.push(
            `legacy relation column ${tableName}.${field} still exists`,
          );
        }
      }
      for (const modification of tableMigration.relationsToModify ?? []) {
        const oldField =
          modification.from.foreignKeyColumn ??
          `${modification.from.propertyName}Id`;
        const newField =
          modification.to.foreignKeyColumn ??
          `${modification.to.propertyName}Id`;
        if (
          oldField !== newField &&
          (await knex.schema.hasColumn(tableName, oldField))
        ) {
          errors.push(
            `legacy relation column ${tableName}.${oldField} still exists`,
          );
        }
        const oldJunction = modification.from.junctionTableName;
        const newJunction = modification.to.junctionTableName;
        if (
          oldJunction &&
          oldJunction !== newJunction &&
          (await knex.schema.hasTable(oldJunction))
        ) {
          errors.push(
            `legacy physical junction table ${oldJunction} still exists`,
          );
        }
      }
    }
  }

  private async collectMongoErrors(
    snapshot: Record<string, any>,
    migration: SchemaMigrationDef | null,
  ): Promise<string[]> {
    const db = this.queryBuilderService.getMongoDb();
    const errors: string[] = [];

    for (const [collectionName, table] of Object.entries(snapshot)) {
      if (!(await this.mongoCollectionExists(db, collectionName))) {
        errors.push(`physical collection ${collectionName} is missing`);
        continue;
      }
      const expected = buildMongoFullIndexSpecs({
        collectionName,
        columns: (table as any).columns ?? [],
        uniques: (table as any).uniques ?? [],
        indexes: (table as any).indexes ?? [],
        relations: (table as any).relations ?? [],
      });
      const current = await db
        .collection(collectionName)
        .listIndexes()
        .toArray();
      this.collectMongoIndexErrors(collectionName, expected, current, errors);
    }

    await this.collectMongoJunctionErrors(db, snapshot, errors);
    await this.collectMongoLegacyErrors(db, snapshot, migration, errors);
    return errors;
  }

  private collectMongoIndexErrors(
    collectionName: string,
    expected: Array<{ name: string; keys: Record<string, any>; options?: any }>,
    current: any[],
    errors: string[],
  ): void {
    for (const target of expected) {
      const actual = current.find((index) => index.name === target.name);
      if (!actual) {
        errors.push(
          `physical index ${collectionName}.${target.name} is missing`,
        );
        continue;
      }
      const targetOptions = target.options ?? {};
      const differences: string[] = [];
      if (this.canonical(actual.key ?? {}) !== this.canonical(target.keys)) {
        differences.push('keys');
      }
      if (Boolean(actual.unique) !== Boolean(targetOptions.unique)) {
        differences.push('unique');
      }
      if (Boolean(actual.sparse) !== Boolean(targetOptions.sparse)) {
        differences.push('sparse');
      }
      if (
        (actual.expireAfterSeconds ?? null) !==
        (targetOptions.expireAfterSeconds ?? null)
      ) {
        differences.push('expireAfterSeconds');
      }
      if (
        this.canonical(actual.partialFilterExpression ?? null) !==
        this.canonical(targetOptions.partialFilterExpression ?? null)
      ) {
        differences.push('partialFilterExpression');
      }
      if (differences.length > 0) {
        errors.push(
          `physical index ${collectionName}.${target.name} differs on ${differences.join(', ')}`,
        );
      }
    }
  }

  private async collectMongoJunctionErrors(
    db: Db,
    snapshot: Record<string, any>,
    errors: string[],
  ): Promise<void> {
    const visited = new Set<string>();
    const generatedInverseKeys = new Set<string>();
    for (const [sourceTable, table] of Object.entries(snapshot)) {
      for (const relation of (table as any).relations ?? []) {
        const relationKey = `${sourceTable}.${relation.propertyName}`;
        if (generatedInverseKeys.has(relationKey)) continue;
        if (relation.inversePropertyName) {
          generatedInverseKeys.add(
            `${relation.targetTable}.${relation.inversePropertyName}`,
          );
        }
        if (relation.type !== 'many-to-many' || relation.mappedBy) continue;
        const fallback = getSqlJunctionPhysicalNames({
          sourceTable,
          propertyName: relation.propertyName,
          targetTable: relation.targetTable,
        });
        const tableName =
          relation.junctionTableName ?? fallback.junctionTableName;
        const sourceColumn =
          relation.junctionSourceColumn ?? fallback.junctionSourceColumn;
        const targetColumn =
          relation.junctionTargetColumn ?? fallback.junctionTargetColumn;
        if (visited.has(tableName)) continue;
        visited.add(tableName);

        if (!(await this.mongoCollectionExists(db, tableName))) {
          errors.push(`physical junction collection ${tableName} is missing`);
          continue;
        }
        const current = await db.collection(tableName).listIndexes().toArray();
        this.collectMongoIndexErrors(
          tableName,
          [
            {
              name: `${tableName}_src_tgt_uq`,
              keys: { [sourceColumn]: 1, [targetColumn]: 1 },
              options: { unique: true },
            },
            {
              name: `${tableName}_tgt_idx`,
              keys: { [targetColumn]: 1 },
              options: {},
            },
          ],
          current,
          errors,
        );
      }
    }
  }

  private async collectMongoLegacyErrors(
    db: Db,
    snapshot: Record<string, any>,
    migration: SchemaMigrationDef | null,
    errors: string[],
  ): Promise<void> {
    if (!migration) return;
    const legacyCollections = new Set<string>([
      ...(migration.tablesToDrop ?? []),
      ...(migration.physicalTablesToDrop ?? []),
      ...(migration.coreTablesToRename ?? []).map((entry) => entry.from),
      ...(migration.tablesToRename ?? []).map((entry) => entry.from),
      ...(migration.physicalTablesToRename ?? []).map((entry) => entry.from),
    ]);
    for (const collectionName of legacyCollections) {
      if (
        collectionName &&
        (await this.mongoCollectionExists(db, collectionName))
      ) {
        errors.push(
          `legacy physical collection ${collectionName} still exists`,
        );
      }
    }

    for (const tableMigration of migration.tables ?? []) {
      const collectionName = tableMigration._unique.name._eq;
      if (!(await this.mongoCollectionExists(db, collectionName))) continue;
      const currentFields = new Set<string>([
        ...((snapshot[collectionName]?.columns ?? []).map(
          (column: any) => column.name,
        ) as string[]),
        ...((snapshot[collectionName]?.relations ?? []).map(
          (relation: any) => relation.propertyName,
        ) as string[]),
      ]);
      const fields = new Set<string>(
        (tableMigration.columnsToRemove ?? []).filter(
          (field) => !currentFields.has(field),
        ),
      );
      for (const modification of tableMigration.columnsToModify ?? []) {
        if (
          modification.from.name !== modification.to.name &&
          !currentFields.has(modification.from.name)
        ) {
          fields.add(modification.from.name);
        }
      }
      for (const propertyName of tableMigration.relationsToRemove ?? []) {
        if (!currentFields.has(propertyName)) fields.add(propertyName);
      }
      for (const modification of tableMigration.relationsToModify ?? []) {
        if (
          modification.from.propertyName !== modification.to.propertyName &&
          !currentFields.has(modification.from.propertyName)
        ) {
          fields.add(modification.from.propertyName);
        }
      }
      for (const field of fields) {
        const count = await db
          .collection(collectionName)
          .countDocuments({ [field]: { $exists: true } });
        if (count > 0) {
          errors.push(
            `legacy field ${collectionName}.${field} still exists in ${count} document(s)`,
          );
        }
      }
    }
  }

  private async readSqlForeignKeys(
    knex: Knex,
    tableName: string,
  ): Promise<SqlForeignKeyState[]> {
    const dbType = String(knex.client.config.client).toLowerCase();
    if (dbType.includes('pg') || dbType.includes('postgres')) {
      const result = await knex.raw(
        `
          SELECT tc.constraint_name AS "constraintName",
                 kcu.column_name AS "columnName",
                 ccu.table_name AS "targetTable",
                 ccu.column_name AS "targetColumn",
                 rc.delete_rule AS "onDelete"
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
           AND tc.constraint_schema = kcu.constraint_schema
          JOIN information_schema.referential_constraints rc
            ON tc.constraint_name = rc.constraint_name
           AND tc.constraint_schema = rc.constraint_schema
          JOIN information_schema.constraint_column_usage ccu
            ON rc.unique_constraint_name = ccu.constraint_name
           AND rc.unique_constraint_schema = ccu.constraint_schema
          WHERE tc.table_schema = current_schema()
            AND tc.table_name = ?
            AND tc.constraint_type = 'FOREIGN KEY'
        `,
        [tableName],
      );
      return result.rows ?? [];
    }
    const result = await knex.raw(
      `
        SELECT kcu.CONSTRAINT_NAME AS constraintName,
               kcu.COLUMN_NAME AS columnName,
               kcu.REFERENCED_TABLE_NAME AS targetTable,
               kcu.REFERENCED_COLUMN_NAME AS targetColumn,
               rc.DELETE_RULE AS onDelete
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
        JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
          ON kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
         AND kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
        WHERE kcu.TABLE_SCHEMA = DATABASE()
          AND kcu.TABLE_NAME = ?
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
      `,
      [tableName],
    );
    return result[0] ?? [];
  }

  private async readSqlPrimaryKeyColumns(
    knex: Knex,
    tableName: string,
  ): Promise<string[]> {
    const dbType = String(knex.client.config.client).toLowerCase();
    if (dbType.includes('pg') || dbType.includes('postgres')) {
      const result = await knex.raw(
        `
          SELECT kcu.column_name AS "columnName"
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
           AND tc.constraint_schema = kcu.constraint_schema
          WHERE tc.table_schema = current_schema()
            AND tc.table_name = ?
            AND tc.constraint_type = 'PRIMARY KEY'
          ORDER BY kcu.ordinal_position
        `,
        [tableName],
      );
      return (result.rows ?? []).map((row: any) => row.columnName);
    }
    const result = await knex.raw(
      `
        SELECT COLUMN_NAME AS columnName
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = ?
          AND CONSTRAINT_NAME = 'PRIMARY'
        ORDER BY ORDINAL_POSITION
      `,
      [tableName],
    );
    return (result[0] ?? []).map((row: any) => row.columnName);
  }

  private async mongoCollectionExists(
    db: Db,
    collectionName: string,
  ): Promise<boolean> {
    const collections = await db
      .listCollections({ name: collectionName })
      .toArray();
    return collections.length > 0;
  }

  private groupKey(columns: string[]): string {
    return columns.map((column) => column.toLowerCase()).join('|');
  }

  private canonical(value: any): string {
    if (Array.isArray(value)) {
      return `[${value.map((entry) => this.canonical(entry)).join(',')}]`;
    }
    if (value && typeof value === 'object') {
      return `{${Object.keys(value)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${this.canonical(value[key])}`)
        .join(',')}}`;
    }
    return JSON.stringify(value);
  }
}
