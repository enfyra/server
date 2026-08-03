import type { QueryBuilderService } from '@enfyra/kernel';
import type { Knex } from 'knex';
import type { Db } from 'mongodb';
import { buildMongoFullIndexSpecs } from '../../../engines/mongo';
import { getRemovedMongoStoredFields } from '../../../engines/mongo/utils/mongo-physical-schema-contract';
import {
  buildMongoValidationSchema,
  MONGO_VALIDATION_LEVEL,
  MONGO_VALIDATION_ACTION,
} from '../../../engines/mongo/utils/mongo-validation-schema.util';
import {
  buildSqlForeignKeyContracts,
  isSqlForeignKeyRelation,
} from '../../../engines/knex/utils/sql-physical-schema-contract';
import {
  compareSchemas,
  getCurrentDatabaseSchema,
} from '../../../engines/knex/utils/provision/schema-comparison';
import { parseSnapshotToSchema } from '../../../engines/knex/utils/provision/schema-parser';
import type { DatabaseConfigService } from '../../../shared/services';
import type {
  RuntimeSchemaMutationContract,
  RuntimeSchemaRelationContract,
  RuntimeTableSchemaContract,
} from '../types/runtime-schema-mutation.types';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';

type PhysicalState = RuntimeTableSchemaContract | null;

function inverseRelationType(type: string): string {
  if (type === 'many-to-one') return 'one-to-many';
  if (type === 'one-to-many') return 'many-to-one';
  return type;
}

export class RuntimeSchemaTargetAttestorService {
  constructor(
    private readonly deps: {
      queryBuilderService: QueryBuilderService;
      databaseConfigService: DatabaseConfigService;
    },
  ) {}

  async assertSource(contract: RuntimeSchemaMutationContract): Promise<void> {
    const source = contract.context.source;
    const target = contract.context.target;
    if (contract.context.operation === 'create') {
      await this.assertAbsent(target?.name ?? contract.context.tableName, 'source');
      return;
    }
    await this.assertPresent(source, 'source');
  }

  async assertTarget(contract: RuntimeSchemaMutationContract): Promise<void> {
    const source = contract.context.source;
    const target = contract.context.target;
    if (!target) {
      await this.assertAbsent(source?.name ?? contract.context.tableName, 'target');
      await this.assertRemovedJunctions(source, null);
      return;
    }
    await this.assertPresent(target, 'target');
    await this.assertRequestedInverseMetadata(contract);
    if (source?.name && source.name !== target.name) {
      await this.assertAbsent(source.name, 'renamed source');
    }
    await this.assertRemovedJunctions(source, target);
    if (this.deps.databaseConfigService.isMongoDb()) {
      await this.assertRemovedMongoFields(source, target);
    }
  }

  private async assertRequestedInverseMetadata(
    contract: RuntimeSchemaMutationContract,
  ): Promise<void> {
    const target = contract.context.executionTarget;
    if (!target) return;
    const relations = target.relations.filter(
      (relation) => relation.inversePropertyName && !relation.mappedBy,
    );
    if (relations.length === 0) return;
    if (this.deps.databaseConfigService.isMongoDb()) {
      await this.assertMongoInverseMetadata(target.name, relations);
      return;
    }
    await this.assertSqlInverseMetadata(target.name, relations);
  }

  private async assertSqlInverseMetadata(
    sourceTableName: string,
    relations: readonly RuntimeSchemaRelationContract[],
  ): Promise<void> {
    const knex = this.deps.queryBuilderService.getKnex();
    const sourceTable = await knex('enfyra_table')
      .where({ name: sourceTableName })
      .first();
    if (!sourceTable) {
      throw new Error(
        `Runtime schema inverse metadata source table '${sourceTableName}' is missing`,
      );
    }
    for (const relation of relations) {
      const targetTable = await knex('enfyra_table')
        .where({ name: relation.targetTableName })
        .first();
      const owning = await knex('enfyra_relation')
        .where({
          sourceTableId: sourceTable.id,
          targetTableId: targetTable?.id,
          propertyName: relation.propertyName,
        })
        .first();
      const inverse = owning && targetTable
        ? await knex('enfyra_relation')
            .where({
              sourceTableId: targetTable.id,
              targetTableId: sourceTable.id,
              mappedById: owning.id,
              propertyName: relation.inversePropertyName,
              type: inverseRelationType(relation.type),
            })
            .first()
        : null;
      if (!inverse) {
        throw new Error(
          `Runtime schema inverse metadata '${relation.targetTableName}.${relation.inversePropertyName}' was not materialized for '${sourceTableName}.${relation.propertyName}'`,
        );
      }
    }
  }

  private async assertMongoInverseMetadata(
    sourceTableName: string,
    relations: readonly RuntimeSchemaRelationContract[],
  ): Promise<void> {
    const db = this.deps.queryBuilderService.getMongoDb();
    const sourceTable = await db
      .collection('enfyra_table')
      .findOne({ name: sourceTableName });
    if (!sourceTable) {
      throw new Error(
        `Runtime schema inverse metadata source table '${sourceTableName}' is missing`,
      );
    }
    for (const relation of relations) {
      const targetTable = await db
        .collection('enfyra_table')
        .findOne({ name: relation.targetTableName });
      const owning = targetTable
        ? await db.collection('enfyra_relation').findOne({
            sourceTable: sourceTable._id,
            targetTable: targetTable._id,
            propertyName: relation.propertyName,
          })
        : null;
      const inverse = owning && targetTable
        ? await db.collection('enfyra_relation').findOne({
            sourceTable: targetTable._id,
            targetTable: sourceTable._id,
            mappedBy: owning._id,
            propertyName: relation.inversePropertyName,
            type: inverseRelationType(relation.type),
          })
        : null;
      if (!inverse) {
        throw new Error(
          `Runtime schema inverse metadata '${relation.targetTableName}.${relation.inversePropertyName}' was not materialized for '${sourceTableName}.${relation.propertyName}'`,
        );
      }
    }
  }

  private async assertPresent(
    state: PhysicalState,
    phase: 'source' | 'target',
  ): Promise<void> {
    if (!state) {
      throw new Error(`Runtime schema ${phase} physical proof is missing`);
    }
    const errors = this.deps.databaseConfigService.isMongoDb()
      ? await this.collectMongoErrors(state)
      : await this.collectSqlErrors(state);
    if (errors.length > 0) {
      throw new Error(
        `Runtime schema ${phase} physical attestation failed:\n- ${errors.join('\n- ')}`,
      );
    }
  }

  private async assertAbsent(name: string, phase: string): Promise<void> {
    if (!name) {
      throw new Error(`Runtime schema ${phase} physical identity is missing`);
    }
    const exists = this.deps.databaseConfigService.isMongoDb()
      ? await this.mongoCollectionExists(
          this.deps.queryBuilderService.getMongoDb(),
          name,
        )
      : await this.deps.queryBuilderService.getKnex().schema.hasTable(name);
    if (exists) {
      throw new Error(
        `Runtime schema ${phase} physical attestation failed: '${name}' still exists`,
      );
    }
  }

  private async collectSqlErrors(
    state: RuntimeTableSchemaContract,
  ): Promise<string[]> {
    const knex = this.deps.queryBuilderService.getKnex();
    if (!(await knex.schema.hasTable(state.name))) {
      return [`physical table ${state.name} is missing`];
    }
    const definition = this.toSqlPhysicalDefinition(state);
    const schema = parseSnapshotToSchema({ [state.name]: definition })[0];
    const current = await getCurrentDatabaseSchema(knex, state.name);
    const diff = compareSchemas(schema, current);
    const errors: string[] = [];
    for (const column of diff.columnsToAdd) {
      errors.push(`physical column ${state.name}.${column.name} is missing`);
    }
    for (const column of diff.columnsToRemove) {
      errors.push(`unexpected physical column ${state.name}.${column}`);
    }
    for (const { column, changes } of diff.columnsToModify) {
      errors.push(
        `physical column ${state.name}.${column.name} differs on ${changes.join(', ')}`,
      );
    }
    for (const columns of diff.uniquesToAdd) {
      errors.push(`physical unique ${state.name}(${columns.join(', ')}) is missing`);
    }
    for (const unique of diff.uniquesToRemove) {
      errors.push(
        `unexpected physical unique ${state.name}(${unique.columns.join(', ')})`,
      );
    }
    for (const columns of diff.indexesToAdd) {
      errors.push(`physical index ${state.name}(${columns.join(', ')}) is missing`);
    }
    for (const index of diff.indexesToRemove) {
      errors.push(
        `unexpected physical index ${state.name}(${index.columns.join(', ')})`,
      );
    }
    for (const relation of diff.relationsToAdd) {
      if (relation.type !== 'many-to-many') {
        errors.push(
          `physical relation ${state.name}.${relation.propertyName} is missing`,
        );
      }
    }
    for (const relation of diff.relationsToRemove) {
      errors.push(`unexpected physical relation column ${state.name}.${relation}`);
    }
    await this.collectSqlForeignKeyErrors(knex, state, errors);
    await this.collectSqlJunctionErrors(knex, state, errors);
    return errors;
  }

  private async collectSqlForeignKeyErrors(
    knex: Knex,
    state: RuntimeTableSchemaContract,
    errors: string[],
  ): Promise<void> {
    const expected = buildSqlForeignKeyContracts(
      state.name,
      this.toSqlPhysicalDefinition(state).relations,
    );
    const actual = await this.readSqlForeignKeys(knex, state.name);
    for (const target of expected) {
      const current = actual.find(
        (foreignKey) => foreignKey.columnName === target.columnName,
      );
      if (!current) {
        errors.push(
          `physical foreign key ${state.name}.${target.columnName} is missing`,
        );
        continue;
      }
      const differences: string[] = [];
      if (current.targetTable !== target.targetTable) differences.push('targetTable');
      if (current.targetColumn !== target.targetColumn) differences.push('targetColumn');
      if (current.onDelete.toUpperCase() !== target.onDelete.toUpperCase()) {
        differences.push('onDelete');
      }
      if (differences.length > 0) {
        errors.push(
          `physical foreign key ${state.name}.${target.columnName} differs on ${differences.join(', ')}`,
        );
      }
    }
  }

  private async collectSqlJunctionErrors(
    knex: Knex,
    state: RuntimeTableSchemaContract,
    errors: string[],
  ): Promise<void> {
    for (const junction of this.getJunctions(state)) {
      if (!(await knex.schema.hasTable(junction.name))) {
        errors.push(`physical junction table ${junction.name} is missing`);
        continue;
      }
      for (const column of [junction.sourceColumn, junction.targetColumn]) {
        if (!(await knex.schema.hasColumn(junction.name, column))) {
          errors.push(`physical junction column ${junction.name}.${column} is missing`);
        }
      }
    }
  }

  private async collectMongoErrors(
    state: RuntimeTableSchemaContract,
  ): Promise<string[]> {
    const db = this.deps.queryBuilderService.getMongoDb();
    if (!(await this.mongoCollectionExists(db, state.name))) {
      return [`physical collection ${state.name} is missing`];
    }
    const expected = buildMongoFullIndexSpecs({
      collectionName: state.name,
      columns: state.columns as any[],
      uniques: this.asConstraintGroups(state.uniques),
      indexes: this.asConstraintGroups(state.indexes),
      relations: this.toPhysicalDefinition(state).relations,
    });
    const current = await db.collection(state.name).listIndexes().toArray();
    const errors: string[] = [];
    for (const target of expected) {
      const actual = current.find((index: any) => index.name === target.name);
      if (!actual) {
        errors.push(`physical index ${state.name}.${target.name} is missing`);
        continue;
      }
      const options = target.options ?? {};
      if (this.canonical(actual.key ?? {}) !== this.canonical(target.keys)) {
        errors.push(`physical index ${state.name}.${target.name} differs on keys`);
      }
      if (Boolean(actual.unique) !== Boolean(options.unique)) {
        errors.push(`physical index ${state.name}.${target.name} differs on unique`);
      }
      if (Boolean(actual.sparse) !== Boolean(options.sparse)) {
        errors.push(`physical index ${state.name}.${target.name} differs on sparse`);
      }
      if (
        this.canonical(actual.partialFilterExpression ?? null) !==
        this.canonical(options.partialFilterExpression ?? null)
      ) {
        errors.push(
          `physical index ${state.name}.${target.name} differs on partialFilterExpression`,
        );
      }
    }
    await this.collectMongoJunctionErrors(db, state, errors);
    await this.collectMongoValidatorErrors(db, state, errors);
    return errors;
  }

  private async collectMongoValidatorErrors(
    db: Db,
    state: RuntimeTableSchemaContract,
    errors: string[],
  ): Promise<void> {
    const collections = await db
      .listCollections({ name: state.name })
      .toArray();
    const options = (collections[0] as any)?.options ?? {};
    const expectedSchema = buildMongoValidationSchema(state.columns as any[]);
    const actualValidator = options.validator;
    const expectedValidator = { $jsonSchema: expectedSchema };
    if (!actualValidator) {
      errors.push(
        `physical collection ${state.name} is missing a $jsonSchema validator`,
      );
      return;
    }
    if (
      this.canonicalValidator(actualValidator) !==
      this.canonicalValidator(expectedValidator)
    ) {
      errors.push(
        `physical collection ${state.name} validator does not match the target contract`,
      );
    }
    if (options.validationLevel !== MONGO_VALIDATION_LEVEL) {
      errors.push(
        `physical collection ${state.name} validationLevel is '${options.validationLevel}', expected '${MONGO_VALIDATION_LEVEL}'`,
      );
    }
    if (options.validationAction !== MONGO_VALIDATION_ACTION) {
      errors.push(
        `physical collection ${state.name} validationAction is '${options.validationAction}', expected '${MONGO_VALIDATION_ACTION}'`,
      );
    }
  }

  private canonicalValidator(validator: unknown): string {
    return this.canonical(this.normalizeValidatorNode(validator));
  }

  private normalizeValidatorNode(node: unknown): unknown {
    if (node == null || typeof node !== 'object') return node;
    if (Array.isArray(node)) {
      const items = node.map((item) => this.normalizeValidatorNode(item));
      if (items.every((item) => typeof item === 'string')) {
        return [...items].sort();
      }
      return items;
    }
    const entries = Object.entries(node as Record<string, unknown>);
    const normalized: Record<string, unknown> = {};
    for (const [key, value] of entries) {
      if (key === 'required' && Array.isArray(value)) {
        normalized[key] = [...value].sort();
      } else if (key === 'bsonType' && Array.isArray(value)) {
        normalized[key] = [...value].sort();
      } else {
        normalized[key] = this.normalizeValidatorNode(value);
      }
    }
    return normalized;
  }

  private async collectMongoJunctionErrors(
    db: Db,
    state: RuntimeTableSchemaContract,
    errors: string[],
  ): Promise<void> {
    for (const junction of this.getJunctions(state)) {
      if (!(await this.mongoCollectionExists(db, junction.name))) {
        errors.push(`physical junction collection ${junction.name} is missing`);
        continue;
      }
      const indexes = await db.collection(junction.name).listIndexes().toArray();
      const unique = indexes.find((index) => index.name === `${junction.name}_src_tgt_uq`);
      const target = indexes.find((index) => index.name === `${junction.name}_tgt_idx`);
      if (!unique || unique.unique !== true) {
        errors.push(`physical junction unique ${junction.name} is missing`);
      }
      if (!target) {
        errors.push(`physical junction target index ${junction.name} is missing`);
      }
    }
  }

  private async assertRemovedJunctions(
    source: PhysicalState,
    target: PhysicalState,
  ): Promise<void> {
    if (!source) return;
    const targetNames = new Set(this.getJunctions(target).map((item) => item.name));
    for (const junction of this.getJunctions(source)) {
      if (targetNames.has(junction.name)) continue;
      await this.assertAbsent(junction.name, 'removed junction');
    }
  }

  private async assertRemovedMongoFields(
    source: PhysicalState,
    target: RuntimeTableSchemaContract,
  ): Promise<void> {
    if (!source) return;
    const removed = getRemovedMongoStoredFields(source, target);
    if (removed.length === 0) return;
    const db = this.deps.queryBuilderService.getMongoDb();
    for (const field of removed) {
      const count = await db
        .collection(target.name)
        .countDocuments({ [field]: { $exists: true } });
      if (count > 0) {
        throw new Error(
          `Runtime schema target physical attestation failed: legacy field ${target.name}.${field} remains in ${count} document(s)`,
        );
      }
    }
  }

  private getJunctions(state: PhysicalState): Array<{
    name: string;
    sourceColumn: string;
    targetColumn: string;
  }> {
    if (!state) return [];
    return state.relations
      .filter((relation) => relation.type === 'many-to-many' && !relation.mappedBy)
      .map((relation) => {
        const fallback = getSqlJunctionPhysicalNames({
          sourceTable: state.name,
          propertyName: relation.propertyName,
          targetTable: relation.targetTableName,
        });
        return {
          name: relation.junctionTableName || fallback.junctionTableName,
          sourceColumn: fallback.junctionSourceColumn,
          targetColumn: fallback.junctionTargetColumn,
        };
      });
  }

  private toPhysicalDefinition(state: RuntimeTableSchemaContract): any {
    return {
      ...state,
      uniques: this.asConstraintGroups(state.uniques),
      indexes: this.asConstraintGroups(state.indexes),
      columns: state.columns.map((column) => ({ ...column })),
      relations: state.relations.map((relation) => ({
        ...relation,
        targetTable: relation.targetTableName,
      })),
    };
  }

  private toSqlPhysicalDefinition(state: RuntimeTableSchemaContract): any {
    const definition = this.toPhysicalDefinition(state);
    return {
      ...definition,
      relations: definition.relations.filter(isSqlForeignKeyRelation),
    };
  }

  private asConstraintGroups(value: unknown): string[][] {
    if (Array.isArray(value)) return value as string[][];
    if (typeof value !== 'string') return [];
    try {
      const parsed = JSON.parse(value);
      return Array.isArray(parsed) ? parsed : [];
    } catch {
      return [];
    }
  }

  private async mongoCollectionExists(db: Db, name: string): Promise<boolean> {
    return (await db.listCollections({ name }).toArray()).length > 0;
  }

  private canonical(value: unknown): string {
    if (value == null || typeof value !== 'object') return JSON.stringify(value);
    if (Array.isArray(value)) return `[${value.map((item) => this.canonical(item)).join(',')}]`;
    return `{${Object.entries(value as Record<string, unknown>)
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([key, nested]) => `${JSON.stringify(key)}:${this.canonical(nested)}`)
      .join(',')}}`;
  }

  private async readSqlForeignKeys(
    knex: Knex,
    tableName: string,
  ): Promise<Array<{
    columnName: string;
    targetTable: string;
    targetColumn: string;
    onDelete: string;
  }>> {
    const client = String(knex.client.config.client).toLowerCase();
    if (client.includes('pg') || client.includes('postgres')) {
      const result = await knex.raw(
        `SELECT kcu.column_name AS "columnName",
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
            AND tc.constraint_type = 'FOREIGN KEY'`,
        [tableName],
      );
      return result.rows ?? [];
    }
    const result = await knex.raw(
      `SELECT kcu.COLUMN_NAME AS columnName,
              kcu.REFERENCED_TABLE_NAME AS targetTable,
              kcu.REFERENCED_COLUMN_NAME AS targetColumn,
              rc.DELETE_RULE AS onDelete
         FROM information_schema.KEY_COLUMN_USAGE kcu
         JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
           ON rc.CONSTRAINT_SCHEMA = kcu.CONSTRAINT_SCHEMA
          AND rc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE kcu.TABLE_SCHEMA = DATABASE()
          AND kcu.TABLE_NAME = ?
          AND kcu.REFERENCED_TABLE_NAME IS NOT NULL`,
      [tableName],
    );
    return result[0] ?? [];
  }
}
