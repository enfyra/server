import { Knex } from 'knex';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { getErrorMessage } from '../../../shared/utils/error.util';

export class MenuDefinitionProcessor extends BaseTableProcessor {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    super();
    this.queryBuilderService = deps.queryBuilderService;
  }

  async processSql(
    records: any[],
    knex: Knex,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    let totalCreated = 0;
    let totalSkipped = 0;

    const dropdownMenus = records.filter((r) => r.type === 'Dropdown Menu');
    const menuItems = records.filter((r) => r.type === 'Menu');

    const dropdownsWithoutParent = dropdownMenus.filter((r) => !r.parent);
    const dropdownsWithParent = dropdownMenus.filter((r) => r.parent);

    const transformedDropdownsWithoutParent = await this.transformRecords(
      dropdownsWithoutParent,
      { ...context, knex },
    );
    for (const record of transformedDropdownsWithoutParent) {
      try {
        const result = await this.processSqlRecordOnlyCreate(
          record,
          knex,
          tableName,
          context,
        );
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }

    const transformedDropdownsWithParent = await this.transformRecords(
      dropdownsWithParent,
      { ...context, knex },
    );
    for (const record of transformedDropdownsWithParent) {
      try {
        const result = await this.processSqlRecordOnlyCreate(
          record,
          knex,
          tableName,
          context,
        );
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }

    const transformedMenuItems = await this.transformRecords(menuItems, {
      ...context,
      knex,
    });
    for (const record of transformedMenuItems) {
      try {
        const result = await this.processSqlRecordOnlyCreate(
          record,
          knex,
          tableName,
          context,
        );
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
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
    const whereConditions = Array.isArray(uniqueWhere)
      ? uniqueWhere
      : [uniqueWhere];

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
      this.logger.log(
        `   Skipped (existing): ${this.getRecordIdentifier(record)}`,
      );
      if (this.afterUpsert) {
        await this.afterUpsert(
          { ...record, id: existingRecord.id },
          false,
          context,
        );
      }
      return { created: false, skipped: true };
    }

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

  async processMongo(
    records: any[],
    db: any,
    collectionName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    let totalCreated = 0;
    let totalSkipped = 0;

    const dropdownMenus = records.filter((r) => r.type === 'Dropdown Menu');
    const menuItems = records.filter((r) => r.type === 'Menu');

    const dropdownsWithoutParent = dropdownMenus.filter((r) => !r.parent);
    const dropdownsWithParent = dropdownMenus.filter((r) => r.parent);

    const transformedDropdownsWithoutParent = await this.transformRecords(
      dropdownsWithoutParent,
      context,
    );
    for (const record of transformedDropdownsWithoutParent) {
      try {
        const result = await this.processMongoRecordOnlyCreate(
          record,
          db,
          collectionName,
          context,
        );
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }

    const transformedDropdownsWithParent = await this.transformRecords(
      dropdownsWithParent,
      context,
    );
    for (const record of transformedDropdownsWithParent) {
      try {
        const result = await this.processMongoRecordOnlyCreate(
          record,
          db,
          collectionName,
          context,
        );
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }

    const transformedMenuItems = await this.transformRecords(
      menuItems,
      context,
    );
    for (const record of transformedMenuItems) {
      try {
        const result = await this.processMongoRecordOnlyCreate(
          record,
          db,
          collectionName,
          context,
        );
        if (result.created) totalCreated++;
        if (result.skipped) totalSkipped++;
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
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
    const whereConditions = Array.isArray(uniqueWhere)
      ? uniqueWhere
      : [uniqueWhere];

    let existingRecord = null;
    for (const whereCondition of whereConditions) {
      const cleanedCondition = { ...whereCondition };
      for (const key in cleanedCondition) {
        if (Array.isArray(cleanedCondition[key])) {
          delete cleanedCondition[key];
        }
      }
      existingRecord = await db
        .collection(collectionName)
        .findOne(cleanedCondition);
      if (existingRecord) break;
    }

    if (existingRecord) {
      this.logger.log(
        `   Skipped (existing): ${this.getRecordIdentifier(record)}`,
      );
      if (this.afterUpsert) {
        await this.afterUpsert(
          { ...record, _id: existingRecord._id },
          false,
          context,
        );
      }
      return { created: false, skipped: true };
    }

    const cleanedRecord = this.cleanRecordForMongo(record);
    const result = await db.collection(collectionName).insertOne(cleanedRecord);
    this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
    if (this.afterUpsert) {
      await this.afterUpsert(
        { ...record, _id: result.insertedId },
        true,
        context,
      );
    }
    return { created: true, skipped: false };
  }

  async processWithQueryBuilder(
    records: any[],
    queryBuilder: any,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }
    const idField = DatabaseConfigService.getPkField();
    let totalCreated = 0;
    let totalSkipped = 0;

    const dropdownMenus = records.filter((r) => r.type === 'Dropdown Menu');
    const menuItems = records.filter((r) => r.type === 'Menu');

    const dropdownsWithoutParent = dropdownMenus.filter((r) => !r.parent);
    const dropdownsWithParent = dropdownMenus.filter((r) => r.parent);

    const processRecord = async (record: any) => {
      const uniqueWhere = this.getUniqueIdentifier(record);
      const whereConditions = Array.isArray(uniqueWhere)
        ? uniqueWhere
        : [uniqueWhere];
      let existingRecord = null;
      for (const wc of whereConditions) {
        const cleaned = { ...wc };
        for (const key in cleaned) {
          if (Array.isArray(cleaned[key])) delete cleaned[key];
        }
        existingRecord = await queryBuilder.findOne({
          table: tableName,
          where: cleaned,
        });
        if (existingRecord) break;
      }
      if (existingRecord) {
        this.logger.log(
          `   Skipped (existing): ${this.getRecordIdentifier(record)}`,
        );
        if (this.afterUpsert) {
          await this.afterUpsert(
            { ...record, [idField]: existingRecord[idField] },
            false,
            context,
          );
        }
        totalSkipped++;
        return;
      }
      const inserted = await queryBuilder.insert(tableName, record);
      this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
      if (this.afterUpsert) {
        await this.afterUpsert(
          { ...record, [idField]: inserted[idField] },
          true,
          context,
        );
      }
      totalCreated++;
    };

    const rawBatches = [dropdownsWithoutParent, dropdownsWithParent, menuItems];
    for (const rawBatch of rawBatches) {
      const batch = await this.transformRecords(rawBatch, context);
      for (const record of batch) {
        try {
          await processRecord(record);
        } catch (error) {
          this.logger.error(`Error: ${getErrorMessage(error)}`);
          this.logger.error(
            `   Record: ${JSON.stringify(record).substring(0, 200)}`,
          );
        }
      }
    }

    return { created: totalCreated, skipped: totalSkipped };
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
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
        if (!('parent' in transformed)) transformed.parent = null;
      }
      if (transformed.parent && typeof transformed.parent === 'string') {
        const parentLabel = transformed.parent;
        const parent = await this.queryBuilderService.findOne({
          table: 'menu_definition',
          where: { type: 'Dropdown Menu', label: parentLabel },
        });
        if (parent) {
          if (isMongoDB) {
            transformed.parent =
              typeof parent._id === 'string'
                ? new ObjectId(parent._id)
                : parent._id;
          } else {
            transformed.parentId = parent.id;
            delete transformed.parent;
          }
        } else {
          this.logger.warn(
            `Parent not found: ${parentLabel} for ${transformed.label}`,
          );
          if (isMongoDB) {
            transformed.parent = null;
          } else {
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
      conditions.push({
        type: record.type,
        label: record.label,
        parent: record.parent,
      });
    }
    return conditions;
  }
  protected getCompareFields(): string[] {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const parentField = isMongoDB ? 'parent' : 'parentId';
    return [
      'type',
      'label',
      'icon',
      'path',
      'isEnabled',
      'description',
      'order',
      'permission',
      parentField,
    ];
  }
  protected hasValueChanged(newValue: any, existingValue: any): boolean {
    if (
      typeof newValue === 'object' &&
      newValue?.id &&
      typeof existingValue === 'object' &&
      existingValue?.id
    ) {
      return newValue.id !== existingValue.id;
    }
    if (
      (newValue === null || newValue === undefined) &&
      existingValue &&
      typeof existingValue === 'object' &&
      existingValue.id
    ) {
      return true;
    }
    if (
      (existingValue === null || existingValue === undefined) &&
      newValue &&
      typeof newValue === 'object' &&
      newValue.id
    ) {
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
