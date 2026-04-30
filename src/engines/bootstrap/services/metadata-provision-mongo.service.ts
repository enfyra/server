import { Logger } from '../../../shared/logger';
import {
  QueryBuilderService,
  getJunctionTableName,
  getJunctionColumnNames,
} from '@enfyra/kernel';
import { ObjectId, type Db } from 'mongodb';
import {
  BaseTableProcessor,
  loadRelationRenameMap,
} from '../../../domain/bootstrap';
import { buildMongoFullIndexSpecs } from '../../mongo';
import { normalizeMongoPrimaryKeyColumn } from '../../../modules/table-management/utils/mongo-primary-key.util';
class TableDefinitionProcessor extends BaseTableProcessor {
  async transformRecords(records: any[]): Promise<any[]> {
    const now = new Date();
    return records.map((record) => ({
      ...record,
      createdAt: record.createdAt || now,
      updatedAt: record.updatedAt || now,
    }));
  }
  getUniqueIdentifier(record: any): object {
    return { name: record.name };
  }
  protected getCompareFields(): string[] {
    return ['isSystem', 'alias', 'description', 'uniques', 'indexes'];
  }
}
class ColumnDefinitionProcessor extends BaseTableProcessor {
  private tableFieldName: string;
  constructor(tableFieldName: string) {
    super();
    this.tableFieldName = tableFieldName;
  }
  async transformRecords(records: any[]): Promise<any[]> {
    const now = new Date();
    return records.map((record) => ({
      ...record,
      createdAt: record.createdAt || now,
      updatedAt: record.updatedAt || now,
    }));
  }
  getUniqueIdentifier(record: any): object {
    return {
      [this.tableFieldName]: record[this.tableFieldName],
      name: record.name,
    };
  }
  protected getCompareFields(): string[] {
    return [
      'type',
      'isPrimary',
      'isGenerated',
      'isNullable',
      'isSystem',
      'isUpdatable',
      'isPublished',
      'defaultValue',
      'options',
      'description',
      'placeholder',
    ];
  }
}
class RelationDefinitionProcessor extends BaseTableProcessor {
  private sourceTableFieldName: string;
  constructor(sourceTableFieldName: string) {
    super();
    this.sourceTableFieldName = sourceTableFieldName;
  }
  async transformRecords(records: any[]): Promise<any[]> {
    const now = new Date();
    return records.map((record) => ({
      ...record,
      createdAt: record.createdAt || now,
      updatedAt: record.updatedAt || now,
    }));
  }
  getUniqueIdentifier(record: any): object {
    return {
      [this.sourceTableFieldName]: record[this.sourceTableFieldName],
      propertyName: record.propertyName,
    };
  }
  protected getCompareFields(): string[] {
    return ['type', 'isNullable', 'isSystem', 'description'];
  }
}
export class MetadataProvisionMongoService {
  private readonly logger = new Logger(MetadataProvisionMongoService.name);
  private readonly queryBuilderService: QueryBuilderService;
  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }
  private async syncPhysicalIndexesFromSnapshot(
    snapshot: any,
    db: Db,
  ): Promise<void> {
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const specs = buildMongoFullIndexSpecs({
        collectionName: tableName,
        columns: def.columns || [],
        uniques: def.uniques || [],
        indexes: def.indexes || [],
        relations: def.relations || [],
      });
      const collection = db.collection(tableName);
      for (const spec of specs) {
        await collection.createIndex(spec.keys, spec.options);
      }
    }
  }
  private buildRecordFromColumns(data: any, columns: any[]): any {
    const record: any = {};
    for (const col of columns) {
      if (col.name === 'id' || col.name === '_id') {
        continue;
      }
      const columnName = col.name;
      if (data.hasOwnProperty(col.name)) {
        record[columnName] = data[col.name];
      } else if (col.defaultValue !== undefined && col.defaultValue !== null) {
        record[columnName] = col.defaultValue;
      } else if (col.isNullable === false) {
        if (col.type === 'boolean') {
          record[columnName] = false;
        } else if (col.type === 'int' || col.type === 'number') {
          record[columnName] = 0;
        } else if (col.type === 'varchar' || col.type === 'text') {
          record[columnName] = '';
        } else {
          record[columnName] = null;
        }
      } else {
        record[columnName] = null;
      }
    }
    return record;
  }
  async createInitMetadata(snapshot: any): Promise<void> {
    this.logger.log('MongoDB: Creating metadata from snapshot...');
    const db = this.queryBuilderService.getMongoDb();
    const tableNameToId: Record<string, ObjectId> = {};
    const columnDef = snapshot['column_definition'];
    const tableRelation = columnDef?.relations?.find(
      (r: any) => r.targetTable === 'table_definition',
    );
    const tableFieldName = tableRelation?.propertyName || 'table';
    const relationDef = snapshot['relation_definition'];
    const sourceTableRelation = relationDef?.relations?.find(
      (r: any) => r.propertyName === 'sourceTable',
    );
    const sourceTableFieldName =
      sourceTableRelation?.propertyName || 'sourceTable';
    const targetTableRelation = relationDef?.relations?.find(
      (r: any) => r.propertyName === 'targetTable',
    );
    const targetTableFieldName =
      targetTableRelation?.propertyName || 'targetTable';
    this.logger.log(
      `Field names: table=${tableFieldName}, sourceTable=${sourceTableFieldName}, targetTable=${targetTableFieldName}`,
    );
    const tableProcessor = new TableDefinitionProcessor();
    const columnProcessor = new ColumnDefinitionProcessor(tableFieldName);
    const relationProcessor = new RelationDefinitionProcessor(
      sourceTableFieldName,
    );
    this.logger.log('Step 1: Upserting tables...');
    const tableDef = snapshot['table_definition'];
    if (!tableDef || !tableDef.columns) {
      throw new Error('table_definition not found in snapshot');
    }
    const tableRecords = Object.entries(snapshot).map(
      ([_tableName, defRaw]) => {
        const def = defRaw as any;
        const { columns: _c, relations: _r, ...tableData } = def;
        const record = this.buildRecordFromColumns(tableData, tableDef.columns);
        record.isSingleRecord = tableData.isSingleRecord || false;
        return record;
      },
    );
    const tableResult = await tableProcessor.processMongo(
      tableRecords,
      db,
      'table_definition',
    );
    this.logger.log(
      `Tables: ${tableResult.created} created, ${tableResult.skipped} skipped`,
    );
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const table = await db
        .collection('table_definition')
        .findOne({ name: def.name });
      if (table) {
        tableNameToId[tableName] = table._id;
      }
    }
    this.logger.log('Step 2: Upserting columns...');
    if (!columnDef || !columnDef.columns) {
      throw new Error('column_definition not found in snapshot');
    }
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const tableId = tableNameToId[tableName];
      if (!tableId) continue;
      const columnRecords = (def.columns || []).map((col: any) => {
        const normalizedColumn = normalizeMongoPrimaryKeyColumn(col);
        const record = this.buildRecordFromColumns(
          normalizedColumn,
          columnDef.columns,
        );
        record[tableFieldName] = tableId;
        if (tableName === 'table_definition' && col.name === 'name') {
          this.logger.debug(
            `Sample column record: ${JSON.stringify(record, null, 2)}`,
          );
        }
        return record;
      });
      if (columnRecords.length > 0) {
        const columnResult = await columnProcessor.processMongo(
          columnRecords,
          db,
          'column_definition',
        );
        this.logger.debug(
          `${tableName} columns: ${columnResult.created} created, ${columnResult.skipped} skipped`,
        );
      }
    }
    this.logger.log('Step 3: Upserting owning relations...');
    const relationRenameMap = loadRelationRenameMap();
    const relationColl = db.collection('relation_definition');
    if (!relationDef || !relationDef.columns) {
      throw new Error('relation_definition not found in snapshot');
    }
    const owningIdMap = new Map<string, ObjectId>();
    const pendingInverses: Array<{
      tableName: string;
      tableId: ObjectId;
      rel: any;
      owningTableName: string;
      owningPropertyName: string;
    }> = [];
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const tableId = tableNameToId[tableName];
      if (!tableId) continue;
      for (const rel of def.relations || []) {
        if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
        const targetTableId = tableNameToId[rel.targetTable];
        if (!targetTableId) continue;
        if (rel.inversePropertyName && rel.type === 'one-to-many') {
          pendingInverses.push({
            tableName: rel.targetTable,
            tableId: targetTableId,
            rel,
            owningTableName: tableName,
            owningPropertyName: rel.propertyName,
          });
          continue;
        }
        const directRelationRecord = this.buildRecordFromColumns(
          rel,
          relationDef.columns,
        );
        directRelationRecord[sourceTableFieldName] = tableId;
        directRelationRecord[targetTableFieldName] = targetTableId;
        if (rel.type === 'many-to-many' && !rel.mappedBy) {
          const junctionTableName = getJunctionTableName(
            tableName,
            rel.propertyName,
            rel.targetTable,
          );
          const { sourceColumn, targetColumn } = getJunctionColumnNames(
            tableName,
            rel.propertyName,
            rel.targetTable,
          );
          directRelationRecord.junctionTableName = junctionTableName;
          directRelationRecord.junctionSourceColumn = sourceColumn;
          directRelationRecord.junctionTargetColumn = targetColumn;
        }
        const oldPropertyName =
          relationRenameMap[tableName]?.[rel.propertyName];
        if (oldPropertyName) {
          const existing = await relationColl.findOne({
            [sourceTableFieldName]: tableId,
            propertyName: oldPropertyName,
          });
          if (existing) {
            const updatePayload: any = {
              propertyName: rel.propertyName,
              type: rel.type,
            };
            if (rel.isNullable !== undefined)
              updatePayload.isNullable = rel.isNullable;
            if (rel.isSystem !== undefined)
              updatePayload.isSystem = rel.isSystem;
            if (rel.isUpdatable !== undefined)
              updatePayload.isUpdatable = rel.isUpdatable;
            if (rel.description !== undefined)
              updatePayload.description = rel.description;
            if (rel.type === 'many-to-many' && !rel.mappedBy) {
              const jt = getJunctionTableName(
                tableName,
                rel.propertyName,
                rel.targetTable,
              );
              const jc = getJunctionColumnNames(
                tableName,
                rel.propertyName,
                rel.targetTable,
              );
              updatePayload.junctionTableName = jt;
              updatePayload.junctionSourceColumn = jc.sourceColumn;
              updatePayload.junctionTargetColumn = jc.targetColumn;
            }
            updatePayload.updatedAt = new Date();
            await relationColl.updateOne(
              { _id: existing._id },
              { $set: updatePayload },
            );
            this.logger.debug(
              `Relation rename (Mongo): ${tableName}.${oldPropertyName} → ${rel.propertyName}`,
            );
            owningIdMap.set(`${tableName}.${rel.propertyName}`, existing._id);
            if (rel.inversePropertyName) {
              pendingInverses.push({
                tableName: rel.targetTable,
                tableId: targetTableId,
                rel,
                owningTableName: tableName,
                owningPropertyName: rel.propertyName,
              });
            }
            continue;
          }
        }
        await relationProcessor.processMongo(
          [directRelationRecord],
          db,
          'relation_definition',
        );
        const insertedDoc = await relationColl.findOne({
          [sourceTableFieldName]: tableId,
          propertyName: rel.propertyName,
        });
        if (insertedDoc) {
          owningIdMap.set(`${tableName}.${rel.propertyName}`, insertedDoc._id);
        }
        if (rel.inversePropertyName) {
          pendingInverses.push({
            tableName: rel.targetTable,
            tableId: targetTableId,
            rel,
            owningTableName: tableName,
            owningPropertyName: rel.propertyName,
          });
        }
      }
    }
    this.logger.log('Step 4: Upserting inverse relations...');
    const processedInverseRelations = new Set<string>();
    const upsertGeneratedReverseOneToMany = async (
      sourceTableName: string,
      sourceTableId: ObjectId,
      targetTableName: string,
      targetTableId: ObjectId,
      propertyName: string,
      mappedById: ObjectId,
      sourceRel: any,
    ): Promise<void> => {
      const existingReverse = await relationColl.findOne({
        [sourceTableFieldName]: sourceTableId,
        propertyName,
      });
      const reverseData = {
        propertyName,
        type: 'one-to-many',
        isNullable: sourceRel.isNullable !== false,
        isSystem: sourceRel.isSystem || false,
        isUpdatable: sourceRel.isUpdatable !== false,
        description: sourceRel.description,
      };
      if (existingReverse) {
        const needsUpdate =
          existingReverse.mappedBy?.toString() !== mappedById.toString() ||
          existingReverse.type !== 'one-to-many' ||
          existingReverse[targetTableFieldName]?.toString() !==
            targetTableId.toString();
        if (needsUpdate) {
          await relationColl.updateOne(
            { _id: existingReverse._id },
            {
              $set: {
                ...reverseData,
                [targetTableFieldName]: targetTableId,
                mappedBy: mappedById,
                updatedAt: new Date(),
              },
            },
          );
        }
        return;
      }

      const reverseRelationRecord = this.buildRecordFromColumns(
        reverseData,
        relationDef.columns,
      );
      reverseRelationRecord[sourceTableFieldName] = sourceTableId;
      reverseRelationRecord[targetTableFieldName] = targetTableId;
      reverseRelationRecord.mappedBy = mappedById;
      await relationProcessor.processMongo(
        [reverseRelationRecord],
        db,
        'relation_definition',
      );
      this.logger.debug(
        `Added generated reverse relation ${sourceTableName}.${propertyName} for ${targetTableName}`,
      );
    };
    for (const {
      tableName,
      tableId,
      rel,
      owningTableName,
      owningPropertyName,
    } of pendingInverses) {
      const inverseKey = `${tableName}.${rel.inversePropertyName}`;
      const reverseKey = `${owningTableName}.${owningPropertyName}`;
      if (processedInverseRelations.has(inverseKey)) continue;
      if (processedInverseRelations.has(reverseKey)) continue;
      processedInverseRelations.add(inverseKey);
      let inverseType = rel.type;
      if (rel.type === 'many-to-one') inverseType = 'one-to-many';
      else if (rel.type === 'one-to-many') inverseType = 'many-to-one';
      const snapshotRelId = owningIdMap.get(
        `${owningTableName}.${owningPropertyName}`,
      );
      const isGeneratedManyToOne = inverseType === 'many-to-one';
      const inverseData: any = {
        propertyName: rel.inversePropertyName,
        type: inverseType,
        isNullable: rel.isNullable !== false,
        isSystem: rel.isSystem || false,
        isUpdatable: rel.isUpdatable !== false,
      };
      const inverseRelationRecord = this.buildRecordFromColumns(
        inverseData,
        relationDef.columns,
      );
      inverseRelationRecord[sourceTableFieldName] = tableId;
      inverseRelationRecord[targetTableFieldName] =
        tableNameToId[owningTableName];
      inverseRelationRecord.mappedBy = isGeneratedManyToOne
        ? null
        : snapshotRelId || null;
      if (inverseType === 'many-to-many') {
        const owningDoc = snapshotRelId
          ? await relationColl.findOne({ _id: snapshotRelId })
          : null;
        inverseRelationRecord.junctionTableName =
          owningDoc?.junctionTableName ||
          getJunctionTableName(owningTableName, owningPropertyName, tableName);
        inverseRelationRecord.junctionSourceColumn =
          owningDoc?.junctionTargetColumn || null;
        inverseRelationRecord.junctionTargetColumn =
          owningDoc?.junctionSourceColumn || null;
      }
      const existing = await relationColl.findOne({
        [sourceTableFieldName]: tableId,
        propertyName: rel.inversePropertyName,
      });
      if (existing) {
        const mappedByValue = isGeneratedManyToOne
          ? null
          : snapshotRelId || null;
        const inverseJunctionUpdate: any = {};
        if (inverseType === 'many-to-many') {
          const owningDoc = snapshotRelId
            ? await relationColl.findOne({ _id: snapshotRelId })
            : null;
          inverseJunctionUpdate.junctionTableName =
            owningDoc?.junctionTableName ||
            getJunctionTableName(
              owningTableName,
              owningPropertyName,
              tableName,
            );
          inverseJunctionUpdate.junctionSourceColumn =
            owningDoc?.junctionTargetColumn || null;
          inverseJunctionUpdate.junctionTargetColumn =
            owningDoc?.junctionSourceColumn || null;
        }
        const needsUpdate =
          existing.mappedBy?.toString() !== mappedByValue?.toString() ||
          existing.type !== inverseType ||
          (inverseType === 'many-to-many' && !existing.junctionSourceColumn);
        if (needsUpdate) {
          await relationColl.updateOne(
            { _id: existing._id },
            {
              $set: {
                mappedBy: mappedByValue,
                type: inverseType,
                ...inverseJunctionUpdate,
                updatedAt: new Date(),
              },
            },
          );
          this.logger.debug(
            `Updated inverse relation ${rel.inversePropertyName} for ${tableName}`,
          );
        }
        if (isGeneratedManyToOne && snapshotRelId) {
          await relationColl.updateOne(
            { _id: snapshotRelId },
            { $set: { mappedBy: existing._id } },
          );
        } else if (isGeneratedManyToOne && !snapshotRelId) {
          const sourceTableId = tableNameToId[owningTableName];
          if (sourceTableId) {
            await upsertGeneratedReverseOneToMany(
              owningTableName,
              sourceTableId,
              tableName,
              tableId,
              owningPropertyName,
              existing._id,
              rel,
            );
          }
        }
      } else {
        await relationProcessor.processMongo(
          [inverseRelationRecord],
          db,
          'relation_definition',
        );
        if (isGeneratedManyToOne && snapshotRelId) {
          const insertedDoc = await relationColl.findOne({
            [sourceTableFieldName]: tableId,
            propertyName: rel.inversePropertyName,
          });
          if (insertedDoc) {
            await relationColl.updateOne(
              { _id: snapshotRelId },
              { $set: { mappedBy: insertedDoc._id } },
            );
          }
        } else if (isGeneratedManyToOne) {
          const insertedDoc = await relationColl.findOne({
            [sourceTableFieldName]: tableId,
            propertyName: rel.inversePropertyName,
          });
          const sourceTableId = tableNameToId[owningTableName];
          if (insertedDoc && sourceTableId) {
            await upsertGeneratedReverseOneToMany(
              owningTableName,
              sourceTableId,
              tableName,
              tableId,
              owningPropertyName,
              insertedDoc._id,
              rel,
            );
          }
        }
        this.logger.debug(
          `Added inverse relation ${rel.inversePropertyName} for ${tableName}`,
        );
      }
    }
    this.logger.log('Step 5: Syncing physical indexes...');
    await this.syncPhysicalIndexesFromSnapshot(snapshot, db);
    this.logger.log('MongoDB metadata creation completed');
  }
}
