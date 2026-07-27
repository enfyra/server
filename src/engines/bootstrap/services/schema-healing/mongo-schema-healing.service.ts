import { QueryBuilderService } from '@enfyra/kernel';
import {
  MONGO_PRIMARY_KEY_NAME,
  MONGO_PRIMARY_KEY_TYPE,
} from '../../../../modules/table-management/utils/mongo-primary-key.util';
import type { SchemaHealingSnapshot } from '../../types/schema-healing.types';
import {
  diffJunctionMetadata,
  getTargetJunctionContract,
} from '../../utils/schema-healing-junction.util';
import { SystemCoreTableResolver } from '../system-core-table-resolver.service';

export class MongoSchemaHealingService {
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

  async healMongoJunctionContracts(
    snapshot: SchemaHealingSnapshot,
  ): Promise<number> {
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

      const target = getTargetJunctionContract(snapshot, {
        sourceTable: sourceTable.name,
        propertyName: rel.propertyName,
        targetTable: targetTable.name,
      });
      const legacyColumns = this.getLegacyJunctionColumnNames(
        sourceTable.name,
        targetTable.name,
      );
      await this.ensureMongoJunctionCollection(db, {
        oldJunctionTableName: rel.junctionTableName || null,
        oldJunctionSourceColumn: rel.junctionSourceColumn || null,
        oldJunctionTargetColumn: rel.junctionTargetColumn || null,
        junctionTableName: target.junctionTableName,
        junctionSourceColumn: target.junctionSourceColumn,
        junctionTargetColumn: target.junctionTargetColumn,
        legacyJunctions: [
          {
            tableName: this.getLegacyJunctionTableName(
              sourceTable.name,
              rel.propertyName,
              targetTable.name,
            ),
            sourceColumn: legacyColumns.sourceColumn,
            targetColumn: legacyColumns.targetColumn,
          },
        ],
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

    repaired += await this.cleanupMongoLegacyJunctionCollections(snapshot);

    return repaired;
  }

  private async cleanupMongoLegacyJunctionCollections(
    snapshot: SchemaHealingSnapshot,
  ): Promise<number> {
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
    let repaired = 0;

    for (const rel of relations) {
      if (rel.type !== 'many-to-many' || rel.mappedBy) continue;
      const sourceTable = tableById.get(String(rel.sourceTable));
      const targetTable = tableById.get(String(rel.targetTable));
      if (!sourceTable?.name || !targetTable?.name || !rel.propertyName) {
        continue;
      }

      const target = getTargetJunctionContract(snapshot, {
        sourceTable: sourceTable.name,
        propertyName: rel.propertyName,
        targetTable: targetTable.name,
      });
      const legacyColumns = this.getLegacyJunctionColumnNames(
        sourceTable.name,
        targetTable.name,
      );
      const legacyTable = this.getLegacyJunctionTableName(
        sourceTable.name,
        rel.propertyName,
        targetTable.name,
      );
      if (
        legacyTable === target.junctionTableName ||
        !(await this.mongoCollectionExists(db, legacyTable)) ||
        !(await this.mongoCollectionExists(db, target.junctionTableName))
      ) {
        continue;
      }

      await this.mergeMongoJunctionCollection(db, {
        oldJunctionTableName: legacyTable,
        oldJunctionSourceColumn: legacyColumns.sourceColumn,
        oldJunctionTargetColumn: legacyColumns.targetColumn,
        junctionTableName: target.junctionTableName,
        junctionSourceColumn: target.junctionSourceColumn,
        junctionTargetColumn: target.junctionTargetColumn,
      });
      await db.collection(legacyTable).drop();
      repaired++;
      this.log(
        `Removed orphan legacy junction collection '${legacyTable}' after merging into '${target.junctionTableName}'`,
      );
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
      legacyJunctions?: Array<{
        tableName: string;
        sourceColumn: string;
        targetColumn: string;
      }>;
    },
  ): Promise<void> {
    const standardExists = await this.mongoCollectionExists(
      db,
      input.junctionTableName,
    );
    const legacyCandidates = this.getMongoJunctionLegacyCandidates(input);
    let renamedFrom: {
      tableName: string;
      sourceColumn: string | null;
      targetColumn: string | null;
    } | null = null;

    if (!standardExists) {
      const existingLegacy = await this.findExistingMongoJunctionCandidate(
        db,
        legacyCandidates,
      );
      if (existingLegacy) {
        await db
          .collection(existingLegacy.tableName)
          .rename(input.junctionTableName);
        renamedFrom = existingLegacy;
        this.log(
          `Renamed junction collection '${existingLegacy.tableName}' to '${input.junctionTableName}'`,
        );
      } else {
        await db.createCollection(input.junctionTableName);
        this.log(
          `Created missing junction collection '${input.junctionTableName}'`,
        );
      }
    } else {
      for (const candidate of legacyCandidates) {
        if (!(await this.mongoCollectionExists(db, candidate.tableName))) {
          continue;
        }
        await this.mergeMongoJunctionCollection(db, {
          oldJunctionTableName: candidate.tableName,
          oldJunctionSourceColumn: candidate.sourceColumn,
          oldJunctionTargetColumn: candidate.targetColumn,
          junctionTableName: input.junctionTableName,
          junctionSourceColumn: input.junctionSourceColumn,
          junctionTargetColumn: input.junctionTargetColumn,
        });
        await db.collection(candidate.tableName).drop();
        this.log(
          `Merged legacy junction collection '${candidate.tableName}' into '${input.junctionTableName}'`,
        );
      }
    }

    const collection = db.collection(input.junctionTableName);
    await this.renameMongoJunctionFieldIfNeeded(
      collection,
      renamedFrom?.sourceColumn || input.oldJunctionSourceColumn,
      input.junctionSourceColumn,
    );
    await this.renameMongoJunctionFieldIfNeeded(
      collection,
      renamedFrom?.targetColumn || input.oldJunctionTargetColumn,
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

  private async mergeMongoJunctionCollection(
    db: any,
    input: {
      oldJunctionTableName: string;
      oldJunctionSourceColumn: string | null;
      oldJunctionTargetColumn: string | null;
      junctionTableName: string;
      junctionSourceColumn: string;
      junctionTargetColumn: string;
    },
  ): Promise<void> {
    const sourceCollection = db.collection(input.oldJunctionTableName);
    const targetCollection = db.collection(input.junctionTableName);
    const cursor = sourceCollection.find({});

    while (await cursor.hasNext()) {
      const doc = await cursor.next();
      if (!doc) continue;
      const sourceValue =
        doc[input.oldJunctionSourceColumn || ''] ??
        doc[input.junctionSourceColumn];
      const targetValue =
        doc[input.oldJunctionTargetColumn || ''] ??
        doc[input.junctionTargetColumn];
      if (sourceValue == null || targetValue == null) continue;

      await targetCollection.updateOne(
        {
          [input.junctionSourceColumn]: sourceValue,
          [input.junctionTargetColumn]: targetValue,
        },
        {
          $setOnInsert: {
            [input.junctionSourceColumn]: sourceValue,
            [input.junctionTargetColumn]: targetValue,
          },
        },
        { upsert: true },
      );
    }
  }

  private getLegacyJunctionTableName(
    sourceTable: string,
    propertyName: string,
    targetTable: string,
  ): string {
    return `${sourceTable}_${propertyName}_${targetTable}`;
  }

  private getLegacyJunctionColumnNames(
    sourceTable: string,
    targetTable: string,
  ): { sourceColumn: string; targetColumn: string } {
    return {
      sourceColumn: `${this.toCamelCase(sourceTable)}Id`,
      targetColumn: `${this.toCamelCase(targetTable)}Id`,
    };
  }

  private toCamelCase(value: string): string {
    return value.replace(/_([a-z])/g, (_, letter: string) =>
      letter.toUpperCase(),
    );
  }

  private getMongoJunctionLegacyCandidates(input: {
    oldJunctionTableName: string | null;
    oldJunctionSourceColumn: string | null;
    oldJunctionTargetColumn: string | null;
    junctionTableName: string;
    legacyJunctions?: Array<{
      tableName: string;
      sourceColumn: string;
      targetColumn: string;
    }>;
  }): Array<{
    tableName: string;
    sourceColumn: string | null;
    targetColumn: string | null;
  }> {
    const candidates: Array<{
      tableName: string;
      sourceColumn: string | null;
      targetColumn: string | null;
    }> = [];
    if (
      input.oldJunctionTableName &&
      input.oldJunctionTableName !== input.junctionTableName
    ) {
      candidates.push({
        tableName: input.oldJunctionTableName,
        sourceColumn: input.oldJunctionSourceColumn,
        targetColumn: input.oldJunctionTargetColumn,
      });
    }
    for (const legacy of input.legacyJunctions || []) {
      if (legacy.tableName === input.junctionTableName) continue;
      if (
        candidates.some((candidate) => candidate.tableName === legacy.tableName)
      ) {
        continue;
      }
      candidates.push({
        tableName: legacy.tableName,
        sourceColumn: legacy.sourceColumn,
        targetColumn: legacy.targetColumn,
      });
    }
    return candidates;
  }

  private async findExistingMongoJunctionCandidate(
    db: any,
    candidates: Array<{
      tableName: string;
      sourceColumn: string | null;
      targetColumn: string | null;
    }>,
  ): Promise<{
    tableName: string;
    sourceColumn: string | null;
    targetColumn: string | null;
  } | null> {
    for (const candidate of candidates) {
      if (await this.mongoCollectionExists(db, candidate.tableName)) {
        return candidate;
      }
    }
    return null;
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
