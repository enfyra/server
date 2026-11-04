import { QueryBuilderService } from '@enfyra/kernel';
import {
  buildMongoValidationSchema,
  MONGO_VALIDATION_ACTION,
  MONGO_VALIDATION_LEVEL,
} from '../../../mongo/utils/mongo-validation-schema.util';
import {
  MONGO_PRIMARY_KEY_NAME,
  MONGO_PRIMARY_KEY_TYPE,
} from '../../../../modules/table-management/utils/mongo-primary-key.util';
import type { SchemaHealingSnapshot } from '../../types/schema-healing.types';
import { diffJunctionMetadata } from '../../utils/schema-healing-junction.util';
import { getSqlJunctionPhysicalNames } from '../../../../modules/table-management/utils/sql-junction-naming.util';
import { getErrorMessage } from '../../../../shared/utils/error.util';
import { coerceTemporalWriteValue } from '../../../../shared/utils/temporal-write.util';
import { Logger } from '../../../../shared/logger';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';
import { MongoJunctionHealingService } from './mongo-junction-healing.service';

export class MongoSchemaHealingService {
  private readonly logger = new Logger(MongoSchemaHealingService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly systemCoreTableResolver: SystemCoreTableResolver;
  private readonly log: (message: string) => void;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    systemCoreTableResolver: SystemCoreTableResolver;
    log: (message: string) => void;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.systemCoreTableResolver = deps.systemCoreTableResolver;
    this.log = deps.log;
  }

  async syncMongoCollectionValidators(
    snapshot: SchemaHealingSnapshot,
  ): Promise<number> {
    const db = this.queryBuilderService.getMongoDb();
    let synchronized = 0;
    for (const [collectionName, definition] of Object.entries(snapshot)) {
      try {
        const current = await db
          .listCollections({ name: collectionName })
          .next();
        if (!current) continue;
        const validator = {
          $jsonSchema: buildMongoValidationSchema(definition.columns ?? []),
        };
        const options = (current as any).options ?? {};
        // MongoDB defaults an absent option to 'strict'/'error', so an unset value
        // is drift from our contract and must be rewritten, not skipped.
        if (
          this.canonical(options.validator ?? {}) ===
            this.canonical(validator) &&
          options.validationLevel === MONGO_VALIDATION_LEVEL &&
          options.validationAction === MONGO_VALIDATION_ACTION
        ) {
          continue;
        }
        await db.command({
          collMod: collectionName,
          validator,
          validationLevel: MONGO_VALIDATION_LEVEL,
          validationAction: MONGO_VALIDATION_ACTION,
        });
        synchronized++;
      } catch (error) {
        this.logger.warn(
          `Mongo validator sync skipped for ${collectionName}: ${getErrorMessage(error)}`,
        );
      }
    }
    return synchronized;
  }

  async healMongoJunctionContracts(): Promise<number> {
    const db = this.queryBuilderService.getMongoDb();
    const coreNames = await this.systemCoreTableResolver.getNames();
    const relations = await db
      .collection(coreNames.relation)
      .find({})
      .toArray();
    const tables = await db.collection(coreNames.table).find({}).toArray();
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

      const target = getSqlJunctionPhysicalNames({
        sourceTable: sourceTable.name,
        propertyName: rel.propertyName,
        targetTable: targetTable.name,
      });
      await new MongoJunctionHealingService().converge(db, {
        ...target,
        sourceTable: sourceTable.name,
        targetTable: targetTable.name,
        propertyName: rel.propertyName,
        previousTableName: rel.junctionTableName,
        previousSourceColumn: rel.junctionSourceColumn,
        previousTargetColumn: rel.junctionTargetColumn,
      });

      const owningUpdate = diffJunctionMetadata(rel, target);
      if (Object.keys(owningUpdate).length > 0) {
        await db
          .collection(coreNames.relation)
          .updateOne({ _id: rel._id }, { $set: owningUpdate });
        repaired++;
      }

      for (const inverseRel of byMappedBy.get(String(rel._id)) || []) {
        const inverseUpdate = diffJunctionMetadata(inverseRel, {
          junctionTableName: target.junctionTableName,
          junctionSourceColumn: target.junctionTargetColumn,
          junctionTargetColumn: target.junctionSourceColumn,
        });
        if (Object.keys(inverseUpdate).length === 0) continue;
        await db
          .collection(coreNames.relation)
          .updateOne({ _id: inverseRel._id }, { $set: inverseUpdate });
        repaired++;
      }
    }

    return repaired;
  }

  private async mongoCollectionExists(db: any, name: string): Promise<boolean> {
    const existing = await db.listCollections({ name }).toArray();
    return existing.length > 0;
  }

  async repairMongoRelationPhysicalMappings(): Promise<number> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    const collection = this.queryBuilderService
      .getMongoDb()
      .collection(coreNames.relation);
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

  /**
   * Rewrites temporal fields that are not stored as a BSON date.
   *
   * Mongo has a single temporal type, so there is no naive/aware split to repair;
   * the equivalent defect is a field holding a string or a number instead of a date.
   * That value is then an instant only by convention, and the collection's
   * `$jsonSchema` validator rejects it on the next write, which is what makes a
   * temporal field impossible to round-trip. Records written before the write path
   * coerced these values are carried onto the declared type here.
   *
   * The field list comes from column metadata, which is what both the built-in and
   * the runtime-created collections register, so the repair covers every collection
   * rather than only the ones a declaration file happens to list.
   */
  async repairMongoTemporalFields(): Promise<number> {
    const db = this.queryBuilderService.getMongoDb();
    const coreNames = await this.systemCoreTableResolver.getNames();
    const tables = await db.collection(coreNames.table).find({}).toArray();
    let repaired = 0;

    for (const table of tables) {
      const collectionName = String(table?.name ?? '');
      if (!collectionName) continue;
      if (!(await this.mongoCollectionExists(db, collectionName))) continue;

      const columns = await db
        .collection(coreNames.column)
        .find({ table: table._id })
        .toArray();

      for (const column of columns) {
        const fieldName = String(column?.name ?? '');
        if (!fieldName || column?.type !== 'datetime') continue;

        const coerced = await this.coerceMongoTemporalField(
          db,
          collectionName,
          fieldName,
        );
        if (coerced === 0) continue;
        this.log(
          `Coerced ${coerced} ${collectionName}.${fieldName} value(s) to a BSON date`,
        );
        repaired += coerced;
      }
    }

    return repaired;
  }

  private async coerceMongoTemporalField(
    db: any,
    collectionName: string,
    fieldName: string,
  ): Promise<number> {
    const collection = db.collection(collectionName);
    // A string or numeric value is what an earlier write left behind; a date is
    // already the contract and a null/absent field carries no value to convert.
    const cursor = collection.find({
      [fieldName]: { $exists: true, $not: { $type: 'date' }, $ne: null },
    });
    const updates: Array<{ _id: any; value: Date }> = [];
    for await (const document of cursor) {
      const value = coerceTemporalWriteValue(document[fieldName]);
      if (!(value instanceof Date) || Number.isNaN(value.getTime())) continue;
      updates.push({ _id: document._id, value });
    }
    if (updates.length === 0) return 0;

    const operations = updates.map((update) => ({
      updateOne: {
        filter: { _id: update._id },
        update: { $set: { [fieldName]: update.value } },
      },
    }));
    const result = await collection.bulkWrite(operations, { ordered: false });
    return result.modifiedCount ?? 0;
  }

  async repairMongoSystemRecordShapes(): Promise<number> {
    const db = this.queryBuilderService.getMongoDb();
    const coreNames = await this.systemCoreTableResolver.getNames();
    const tables = await db
      .collection(coreNames.table)
      .find({ isSystem: true })
      .toArray();
    let repairedCollections = 0;

    for (const table of tables) {
      const tableId = table._id;
      const columns = await db
        .collection(coreNames.column)
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
          .updateMany(
            { [field]: { $exists: false } },
            { $set: { [field]: value } },
          );
        modified += result.modifiedCount;
      }

      if (modified > 0) {
        repairedCollections++;
      }
    }

    return repairedCollections;
  }

  private isMongoOwningRelation(rel: any): boolean {
    return (
      rel.type === 'many-to-one' || (rel.type === 'one-to-one' && !rel.mappedBy)
    );
  }

  private getMongoRelationForeignKeyColumn(
    rel: any,
    owningRel: any,
  ): string | null {
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
    if (
      this.hasOwn(column, 'defaultValue') &&
      column.defaultValue !== undefined
    ) {
      return column.defaultValue;
    }
    return null;
  }

  private hasOwn(value: any, key: string): boolean {
    return Object.prototype.hasOwnProperty.call(value, key);
  }

  private canonical(value: unknown): string {
    if (Array.isArray(value)) {
      return `[${value.map((entry) => this.canonical(entry)).join(',')}]`;
    }
    if (value && typeof value === 'object') {
      return `{${Object.entries(value as Record<string, unknown>)
        .sort(([left], [right]) => left.localeCompare(right))
        .map(
          ([key, entry]) => `${JSON.stringify(key)}:${this.canonical(entry)}`,
        )
        .join(',')}}`;
    }
    return JSON.stringify(value);
  }

  async repairMongoPrimaryKeyColumns(): Promise<number> {
    const coreNames = await this.systemCoreTableResolver.getNames();
    const db = this.queryBuilderService.getMongoDb();
    const columnCollection = db.collection(coreNames.column);
    const columns = await columnCollection
      .find({ isPrimary: true, name: { $in: ['id', MONGO_PRIMARY_KEY_NAME] } })
      .toArray();
    let repaired = 0;

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

      if (primaryColumn.name === 'id') {
        const duplicate = await columnCollection.findOne({
          table: primaryColumn.table,
          name: MONGO_PRIMARY_KEY_NAME,
        });
        if (duplicate?._id) {
          await columnCollection.deleteOne({ _id: columnId });
          repaired++;
          this.log(
            `Removed duplicate Mongo primary key column metadata '${primaryColumn.name}' for table '${primaryColumn.table}'`,
          );
          continue;
        }
      }

      await columnCollection.updateOne(
        { _id: columnId },
        {
          $set: { name: MONGO_PRIMARY_KEY_NAME, type: MONGO_PRIMARY_KEY_TYPE },
        },
      );
      repaired++;
      this.log(
        `Repaired Mongo primary key column '${primaryColumn.name}' from type '${primaryColumn.type}' to '${MONGO_PRIMARY_KEY_NAME}' '${MONGO_PRIMARY_KEY_TYPE}'`,
      );
    }

    return repaired;
  }
}
