import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import { BaseTableProcessor } from '../processors/base-table-processor';

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
    return { [this.tableFieldName]: record[this.tableFieldName], name: record.name };
  }

  protected getCompareFields(): string[] {
    return ['type', 'isPrimary', 'isGenerated', 'isNullable', 'isSystem', 'isUpdatable', 'isHidden', 'defaultValue', 'options', 'description', 'placeholder'];
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
    return { [this.sourceTableFieldName]: record[this.sourceTableFieldName], propertyName: record.propertyName };
  }

  protected getCompareFields(): string[] {
    return ['type', 'inversePropertyName', 'isNullable', 'isSystem', 'description'];
  }
}

@Injectable()
export class CoreInitMongoService {
  private readonly logger = new Logger(CoreInitMongoService.name);

  constructor(
    private readonly queryBuilder: QueryBuilderService,
  ) {}

  private buildRecordFromColumns(data: any, columns: any[]): any {
    const record: any = {};

    for (const col of columns) {
      // Skip id field - MongoDB will auto-generate _id
      if (col.name === 'id') {
        continue;
      }

      const columnName = col.name;

      if (data.hasOwnProperty(col.name)) {
        record[columnName] = data[col.name];
      } else if (col.defaultValue !== undefined && col.defaultValue !== null) {
        record[columnName] = col.defaultValue;
      } else if (col.isNullable === false) {
        // Required field missing, skip or set default based on type
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

    const db = this.queryBuilder.getMongoDb();
    const tableNameToId: Record<string, ObjectId> = {};

    // Find relation field names from snapshot
    const columnDef = snapshot['column_definition'];
    const tableRelation = columnDef?.relations?.find((r: any) => r.targetTable === 'table_definition');
    const tableFieldName = tableRelation?.propertyName || 'table';

    const relationDef = snapshot['relation_definition'];
    const sourceTableRelation = relationDef?.relations?.find((r: any) => r.propertyName === 'sourceTable');
    const sourceTableFieldName = sourceTableRelation?.propertyName || 'sourceTable';
    const targetTableRelation = relationDef?.relations?.find((r: any) => r.propertyName === 'targetTable');
    const targetTableFieldName = targetTableRelation?.propertyName || 'targetTable';

    this.logger.log(`Field names: table=${tableFieldName}, sourceTable=${sourceTableFieldName}, targetTable=${targetTableFieldName}`);

    // Create processors with correct field names
    const tableProcessor = new TableDefinitionProcessor();
    const columnProcessor = new ColumnDefinitionProcessor(tableFieldName);
    const relationProcessor = new RelationDefinitionProcessor(sourceTableFieldName);

    // Step 1: Upsert all tables
    this.logger.log('Step 1: Upserting tables...');

    // Get table_definition schema from snapshot
    const tableDef = snapshot['table_definition'];
    if (!tableDef || !tableDef.columns) {
      throw new Error('table_definition not found in snapshot');
    }

    const tableRecords = Object.entries(snapshot).map(([tableName, defRaw]) => {
      const def = defRaw as any;
      const { columns, relations, ...tableData } = def;

      const record = this.buildRecordFromColumns(tableData, tableDef.columns);

      if (tableName === 'setting_definition') {
        record.isSingleRecord = true;
      }

      return record;
    });

    const tableResult = await tableProcessor.processMongo(tableRecords, db, 'table_definition');
    this.logger.log(`Tables: ${tableResult.created} created, ${tableResult.skipped} skipped`);

    // Load tableNameToId mapping
    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const table = await db.collection('table_definition').findOne({ name: def.name });
      if (table) {
        tableNameToId[tableName] = table._id;
      }
    }

    // Step 2: Upsert all columns
    this.logger.log('Step 2: Upserting columns...');

    // Get column_definition schema
    if (!columnDef || !columnDef.columns) {
      throw new Error('column_definition not found in snapshot');
    }

    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const tableId = tableNameToId[tableName];
      if (!tableId) continue;

      const columnRecords = (def.columns || []).map((col: any) => {
        // Build record based on column_definition columns
        const record = this.buildRecordFromColumns(col, columnDef.columns);

        // Add table reference using correct field name
        record[tableFieldName] = tableId;

        // Debug: log first column record for table_definition
        if (tableName === 'table_definition' && col.name === 'name') {
          this.logger.log(`Sample column record: ${JSON.stringify(record, null, 2)}`);
        }

        return record;
      });

      if (columnRecords.length > 0) {
        const columnResult = await columnProcessor.processMongo(columnRecords, db, 'column_definition');
        this.logger.log(`${tableName} columns: ${columnResult.created} created, ${columnResult.skipped} skipped`);
      }
    }

    // Step 3: Upsert all relations (including inverse)
    this.logger.log('Step 3: Upserting relations...');
    const processedInverseRelations = new Set<string>();

    // Get relation_definition schema
    if (!relationDef || !relationDef.columns) {
      throw new Error('relation_definition not found in snapshot');
    }

    for (const [tableName, defRaw] of Object.entries(snapshot)) {
      const def = defRaw as any;
      const tableId = tableNameToId[tableName];
      if (!tableId) continue;

      for (const rel of def.relations || []) {
        if (!rel.propertyName || !rel.targetTable || !rel.type) continue;
        const targetTableId = tableNameToId[rel.targetTable];
        if (!targetTableId) continue;

        // Build direct relation record based on relation_definition columns
        const directRelationRecord = this.buildRecordFromColumns(rel, relationDef.columns);

        // Add source and target table references
        directRelationRecord[sourceTableFieldName] = tableId;
        directRelationRecord[targetTableFieldName] = targetTableId;

        const directResult = await relationProcessor.processMongo([directRelationRecord], db, 'relation_definition');

        // Upsert inverse relation if inversePropertyName exists
        if (rel.inversePropertyName) {
          const inverseKey = `${rel.targetTable}.${rel.inversePropertyName}`;

          if (!processedInverseRelations.has(inverseKey)) {
            processedInverseRelations.add(inverseKey);

            // Determine inverse type
            let inverseType = rel.type;
            if (rel.type === 'many-to-one') {
              inverseType = 'one-to-many';
            } else if (rel.type === 'one-to-many') {
              inverseType = 'many-to-one';
            }

            // Build inverse relation from columns
            const inverseData = {
              propertyName: rel.inversePropertyName,
              type: inverseType,
              inversePropertyName: rel.propertyName,
              isNullable: rel.isNullable !== false,
              isSystem: rel.isSystem || false,
            };

            const inverseRelationRecord = this.buildRecordFromColumns(inverseData, relationDef.columns);

            inverseRelationRecord[sourceTableFieldName] = targetTableId;
            inverseRelationRecord[targetTableFieldName] = tableId;

            const inverseResult = await relationProcessor.processMongo([inverseRelationRecord], db, 'relation_definition');
          }
        }
      }
    }

    // Step 4: Skip - columns and relations are inverse one-to-many (NOT stored)
    // They will be computed via $lookup based on metadata
    this.logger.log('Step 4: Skipped - inverse relations not stored');

    this.logger.log('MongoDB metadata creation completed');
  }
}
