import { Injectable } from '@nestjs/common';
import { Knex } from 'knex';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';

@Injectable()
export class MenuDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly queryBuilder: QueryBuilderService) {
    super();
  }

  /**
   * Process menu definitions for SQL databases.
   * Only creates new records, skips existing ones without updating.
   * This preserves user modifications to menus.
   */
  async processSql(
    records: any[],
    knex: Knex,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    const transformedRecords = await this.transformRecords(records, { ...context, knex });
    let totalCreated = 0;
    let totalSkipped = 0;

    // Process in order: Dropdown Menus first, then Menu items
    const dropdownMenus = transformedRecords.filter(r => r.type === 'Dropdown Menu');
    const menuItems = transformedRecords.filter(r => r.type === 'Menu');

    for (const record of [...dropdownMenus, ...menuItems]) {
      try {
        const result = await this.processSqlRecordOnlyCreate(record, knex, tableName, context);
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
        this.logger.error(`   Record: ${JSON.stringify(record).substring(0, 200)}`);
      }
    }

    return { created: totalCreated, skipped: totalSkipped };
  }

  private async processSqlRecordOnlyCreate(
    record: any,
    knex: Knex,
    tableName: string,
    context?: any,
  ): Promise<{ created: boolean; skipped: boolean }> {
    const uniqueWhere = this.getUniqueIdentifier(record);
    const whereConditions = Array.isArray(uniqueWhere) ? uniqueWhere : [uniqueWhere];

    let existingRecord = null;
    for (const whereCondition of whereConditions) {
      const cleanedCondition = { ...whereCondition };
      for (const key in cleanedCondition) {
        if (Array.isArray(cleanedCondition[key])) {
          delete cleanedCondition[key];
        }
      }
      existingRecord = await knex(tableName).where(cleanedCondition).first();
      if (existingRecord) break;
    }

    if (existingRecord) {
      // Skip existing record - do NOT update to preserve user modifications
      this.logger.log(`   Skipped (existing): ${this.getRecordIdentifier(record)}`);
      if (this.afterUpsert) {
        await this.afterUpsert({ ...record, id: existingRecord.id }, false, context);
      }
      return { created: false, skipped: true };
    }

    // Create new record
    const cleanedRecord = this.cleanRecordForKnex(record);
    const dbType = context?.dbType;
    let insertedId: any;
    if (dbType === 'postgres') {
      const result = await knex(tableName).insert(cleanedRecord, ['id']);
      insertedId = result[0]?.id || result[0];
    } else {
      const result = await knex(tableName).insert(cleanedRecord);
      insertedId = Array.isArray(result) ? result[0] : result;
    }
    this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
    if (this.afterUpsert) {
      await this.afterUpsert({ ...record, id: insertedId }, true, context);
    }
    return { created: true, skipped: false };
  }

  /**
   * Process menu definitions for MongoDB.
   * Only creates new records, skips existing ones without updating.
   * This preserves user modifications to menus.
   */
  async processMongo(
    records: any[],
    db: any,
    collectionName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    const transformedRecords = await this.transformRecords(records, context);
    let totalCreated = 0;
    let totalSkipped = 0;

    // Process in order: Dropdown Menus first, then Menu items without parent, then with parent
    const dropdownMenus = transformedRecords.filter(r => r.type === 'Dropdown Menu');
    const otherMenuItems = transformedRecords.filter(r => r.type === 'Menu' && !r.parent);
    const menuItemsWithParent = transformedRecords.filter(r => r.type === 'Menu' && r.parent);

    for (const record of [...dropdownMenus, ...otherMenuItems, ...menuItemsWithParent]) {
      try {
        const result = await this.processMongoRecordOnlyCreate(record, db, collectionName, context);
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
        this.logger.error(`   Record: ${JSON.stringify(record).substring(0, 200)}`);
      }
    }

    return { created: totalCreated, skipped: totalSkipped };
  }

  private async processMongoRecordOnlyCreate(
    record: any,
    db: any,
    collectionName: string,
    context?: any,
  ): Promise<{ created: boolean; skipped: boolean }> {
    const uniqueWhere = this.getUniqueIdentifier(record);
    const whereConditions = Array.isArray(uniqueWhere) ? uniqueWhere : [uniqueWhere];

    let existingRecord = null;
    for (const whereCondition of whereConditions) {
      const cleanedCondition = { ...whereCondition };
      for (const key in cleanedCondition) {
        if (Array.isArray(cleanedCondition[key])) {
          delete cleanedCondition[key];
        }
      }
      existingRecord = await db.collection(collectionName).findOne(cleanedCondition);
      if (existingRecord) break;
    }

    if (existingRecord) {
      // Skip existing record - do NOT update to preserve user modifications
      this.logger.log(`   Skipped (existing): ${this.getRecordIdentifier(record)}`);
      if (this.afterUpsert) {
        await this.afterUpsert({ ...record, _id: existingRecord._id }, false, context);
      }
      return { created: false, skipped: true };
    }

    // Create new record
    const cleanedRecord = this.cleanRecordForMongo(record);
    const result = await db.collection(collectionName).insertOne(cleanedRecord);
    this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
    if (this.afterUpsert) {
      await this.afterUpsert({ ...record, _id: result.insertedId }, true, context);
    }
    return { created: true, skipped: false };
  }
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    const knex = context?.knex;
    if (!isMongoDB && !knex) {
      this.logger.warn('Knex not provided in context for SQL, returning records as-is');
      return records;
    }
    const transformedRecords = [];
    for (const record of records) {
      const transformed = { ...record };
      if (transformed.icon === undefined) transformed.icon = 'lucide:menu';
      if (transformed.isEnabled === undefined) transformed.isEnabled = true;
      if (transformed.isSystem === undefined) transformed.isSystem = false;
      if (transformed.order === undefined) transformed.order = 0;
      if (isMongoDB) {
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }
      if (isMongoDB) {
        if (!('parent' in transformed)) transformed.parent = null;
      }
      if (transformed.parent && typeof transformed.parent === 'string') {
        const parentLabel = transformed.parent;
        if (isMongoDB) {
          const parent = await this.queryBuilder.findOneWhere('menu_definition', {
            type: 'Dropdown Menu',
            label: parentLabel,
          });
          if (parent) {
            this.logger.debug(`Found parent: ${parentLabel} with id ${parent._id}`);
            transformed.parent = typeof parent._id === 'string'
              ? new ObjectId(parent._id)
              : parent._id;
          } else {
            this.logger.warn(`Parent not found: ${parentLabel} for ${transformed.label}`);
            transformed.parent = null;
          }
        } else {
          const parent = await knex('menu_definition')
            .where({ type: 'Dropdown Menu', label: parentLabel })
            .first();
          if (parent) {
            this.logger.debug(`Found parent: ${parentLabel} with id ${parent.id}`);
            transformed.parentId = parent.id;
            delete transformed.parent;
          } else {
            this.logger.warn(`Parent not found: ${parentLabel} for ${transformed.label}`);
            delete transformed.parent;
          }
        }
      }
      transformedRecords.push(transformed);
    }
    return transformedRecords;
  }
  getUniqueIdentifier(record: any): object[] {
    const conditions = [];
    if (record.path) {
      conditions.push({ path: record.path });
    }
    conditions.push({ type: record.type, label: record.label });
    if (record.parent) {
      conditions.push({ type: record.type, label: record.label, parent: record.parent });
    }
    return conditions;
  }
  protected getCompareFields(): string[] {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';
    const parentField = isMongoDB ? 'parent' : 'parentId';
    return ['type', 'label', 'icon', 'path', 'isEnabled', 'description', 'order', 'permission', parentField];
  }
  protected hasValueChanged(newValue: any, existingValue: any): boolean {
    if (typeof newValue === 'object' && newValue?.id && typeof existingValue === 'object' && existingValue?.id) {
      return newValue.id !== existingValue.id;
    }
    if ((newValue === null || newValue === undefined) && (existingValue && typeof existingValue === 'object' && existingValue.id)) {
      return true;
    }
    if ((existingValue === null || existingValue === undefined) && (newValue && typeof newValue === 'object' && newValue.id)) {
      return true;
    }
    return super.hasValueChanged(newValue, existingValue);
  }
  protected getRecordIdentifier(record: any): string {
    const type = record.type;
    const label = record.label;
    const parent = record.parent;
    if (type === 'Dropdown Menu') {
      return `[Dropdown Menu] ${label}`;
    } else if (type === 'Menu' || type === 'menu') {
      return `[Menu] ${label}${parent ? ` (parent: ${parent})` : ''} -> ${record.path || 'no-path'}`;
    }
    return `[${type}] ${label}`;
  }
}