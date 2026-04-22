import { Knex } from 'knex';
import { Logger } from '../../../shared/logger';
import { Db, ObjectId } from 'mongodb';
import {
  getManyToOneRelations,
  getScalarColumns,
  getUniqueFields,
} from '../utils/snapshot-meta.util';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
export interface UpsertResult {
  created: number;
  skipped: number;
}
export abstract class BaseTableProcessor {
  protected readonly logger = new Logger(this.constructor.name);
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    return records;
  }
  abstract getUniqueIdentifier(record: any): object | object[];
  async afterUpsert?(record: any, isNew: boolean, context?: any): Promise<void>;
  protected getRecordIdentifier(record: any): string {
    if (record.name) return record.name;
    if (record.label) return record.label;
    if (record.path) return record.path;
    if (record.type && record.label) return `${record.type}: ${record.label}`;
    if (record.email) return record.email;
    if (record.method) return record.method;
    return JSON.stringify(record).substring(0, 50) + '...';
  }
  protected async autoTransformFkFields(
    record: any,
    tableName: string,
    queryBuilder: any,
  ): Promise<any> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const relations = getManyToOneRelations(tableName);
    const transformed = { ...record };

    for (const rel of relations) {
      const rawValue = transformed[rel.propertyName];
      if (rawValue === undefined || rawValue === null) continue;
      if (typeof rawValue !== 'string') continue;

      const target = await queryBuilder.findOne({
        table: rel.targetTable,
        where: {
          [rel.lookupKey]: rawValue,
        },
      });

      if (!target) {
        this.logger.warn(
          `${rel.targetTable} '${rawValue}' not found for ${rel.propertyName}, skipping.`,
        );
        continue;
      }

      if (isMongoDB) {
        transformed[rel.propertyName] =
          typeof target._id === 'string'
            ? new ObjectId(target._id)
            : target._id;
      } else {
        transformed[`${rel.propertyName}Id`] = target.id;
        delete transformed[rel.propertyName];
      }
    }

    return transformed;
  }

  protected autoGetUniqueIdentifier(record: any, tableName: string): object {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const uniques = getUniqueFields(tableName);

    if (uniques.length > 0) {
      const fields = uniques[0];
      const where: any = {};
      for (const field of fields) {
        const relations = getManyToOneRelations(tableName);
        const rel = relations.find((r) => r.propertyName === field);
        if (rel && !isMongoDB) {
          where[`${field}Id`] = record[`${field}Id`];
        } else {
          const rawValue = record[field];
          if (
            rel &&
            isMongoDB &&
            rawValue &&
            typeof rawValue === 'object' &&
            !Array.isArray(rawValue)
          ) {
            where[field] = rawValue._id ?? rawValue.id ?? rawValue;
          } else {
            where[field] = rawValue;
          }
        }
      }
      return where;
    }

    if (record.name !== undefined) return { name: record.name };
    if (record.email !== undefined) return { email: record.email };
    if (record.key !== undefined) return { key: record.key };
    if (record.path !== undefined) return { path: record.path };
    return {};
  }

  protected autoGetCompareFields(tableName: string): string[] {
    return getScalarColumns(tableName);
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
    const transformedRecords = await this.transformRecords(records, context);
    let createdCount = 0;
    let skippedCount = 0;
    for (const record of transformedRecords) {
      try {
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
          existingRecord = await queryBuilder.findOne({
            table: tableName,
            where: cleanedCondition,
          });
          if (existingRecord) break;
        }
        if (existingRecord) {
          const hasChanges = this.detectRecordChanges(record, existingRecord);
          if (hasChanges) {
            const existingId = existingRecord[idField];
            await queryBuilder.update(tableName, existingId, record);
            skippedCount++;
            this.logger.debug(
              `   Updated: ${this.getRecordIdentifier(record)}`,
            );
          } else {
            skippedCount++;
            this.logger.debug(
              `   Skipped: ${this.getRecordIdentifier(record)}`,
            );
          }
          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, [idField]: existingRecord[idField] },
              false,
              context,
            );
          }
        } else {
          const inserted = await queryBuilder.insert(tableName, record);
          const insertedId = inserted[idField];
          createdCount++;
          this.logger.debug(`   Created: ${this.getRecordIdentifier(record)}`);
          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, [idField]: insertedId },
              true,
              context,
            );
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
        this.logger.error(`   Stack: ${error.stack}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }
    return { created: createdCount, skipped: skippedCount };
  }

  async processSql(
    records: any[],
    knex: Knex,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }
    const transformedRecords = await this.transformRecords(records, context);
    let createdCount = 0;
    let skippedCount = 0;
    for (const record of transformedRecords) {
      try {
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
          existingRecord = await knex(tableName)
            .where(cleanedCondition)
            .first();
          if (existingRecord) break;
        }
        if (existingRecord) {
          const hasChanges = this.detectRecordChanges(record, existingRecord);
          if (hasChanges) {
            await this.updateRecordKnex(
              existingRecord.id,
              record,
              knex,
              tableName,
            );
            skippedCount++;
            this.logger.debug(
              `   Updated: ${this.getRecordIdentifier(record)}`,
            );
          } else {
            skippedCount++;
            this.logger.debug(
              `   Skipped: ${this.getRecordIdentifier(record)}`,
            );
          }
          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, id: existingRecord.id },
              false,
              context,
            );
          }
        } else {
          const cleanedRecord = this.cleanRecordForKnex(record);
          const dbType = context?.dbType;
          this.logger.debug(
            `dbType: ${dbType}, cleanedRecord keys: ${Object.keys(cleanedRecord).join(', ')}`,
          );
          let insertedId: any;
          if (dbType === 'postgres') {
            this.logger.debug('Using PostgreSQL query builder');
            const result = await knex(tableName).insert(cleanedRecord, ['id']);
            insertedId = result[0]?.id || result[0];
            this.logger.debug(`Inserted ID: ${insertedId}`);
          } else {
            this.logger.debug('Using MySQL query builder');
            const result = await knex(tableName).insert(cleanedRecord);
            insertedId = Array.isArray(result) ? result[0] : result;
            this.logger.debug(`Inserted ID: ${insertedId}`);
          }
          createdCount++;
          this.logger.debug(`   Created: ${this.getRecordIdentifier(record)}`);
          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, id: insertedId },
              true,
              context,
            );
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
        this.logger.error(`   Stack: ${error.stack}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }
    return { created: createdCount, skipped: skippedCount };
  }
  protected detectRecordChanges(newRecord: any, existingRecord: any): boolean {
    const compareFields = this.getCompareFields();
    for (const field of compareFields) {
      if (this.hasValueChanged(newRecord[field], existingRecord[field])) {
        return true;
      }
    }
    return false;
  }
  protected getCompareFields(): string[] {
    return ['name', 'description'];
  }
  protected hasValueChanged(newValue: any, existingValue: any): boolean {
    if (newValue === null && existingValue === null) return false;
    if (newValue === undefined && existingValue === undefined) return false;
    if (newValue === null || existingValue === null) return true;
    if (newValue === undefined || existingValue === undefined) return true;
    if (typeof newValue === 'object' && !(newValue instanceof Date)) {
      let parsedExisting = existingValue;
      if (typeof existingValue === 'string') {
        try {
          parsedExisting = JSON.parse(existingValue);
        } catch (e) {}
      }
      if (typeof parsedExisting === 'object') {
        return JSON.stringify(newValue) !== JSON.stringify(parsedExisting);
      }
    }
    return newValue !== existingValue;
  }
  protected cleanRecordForKnex(record: any): any {
    const cleaned: any = {};
    for (const key in record) {
      if (key.startsWith('_')) {
        continue;
      }
      const value = record[key];
      if (Array.isArray(value)) {
        continue;
      }
      if (
        value !== null &&
        typeof value === 'object' &&
        !(value instanceof Date)
      ) {
        cleaned[key] = JSON.stringify(value);
      } else {
        cleaned[key] = value;
      }
    }
    return cleaned;
  }
  protected async updateRecordKnex(
    existingId: any,
    record: any,
    knex: Knex,
    tableName: string,
  ): Promise<void> {
    const cleanedRecord = this.cleanRecordForKnex(record);
    if (Object.keys(cleanedRecord).length > 0) {
      await knex(tableName).where('id', existingId).update(cleanedRecord);
    }
  }
  async processMongo(
    records: any[],
    db: Db,
    collectionName: string,
    context?: any,
  ): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }
    const transformedRecords = await this.transformRecords(records, context);
    let createdCount = 0;
    let skippedCount = 0;
    for (const record of transformedRecords) {
      try {
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
          const hasChanges = this.detectRecordChanges(record, existingRecord);
          if (hasChanges) {
            await this.updateRecordMongo(
              existingRecord._id,
              record,
              db,
              collectionName,
            );
            skippedCount++;
            this.logger.debug(
              `   Updated: ${this.getRecordIdentifier(record)}`,
            );
          } else {
            skippedCount++;
            this.logger.debug(
              `   Skipped: ${this.getRecordIdentifier(record)}`,
            );
          }
          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, _id: existingRecord._id },
              false,
              context,
            );
          }
        } else {
          const cleanedRecord = this.cleanRecordForMongo(record);
          const result = await db
            .collection(collectionName)
            .insertOne(cleanedRecord);
          const insertedId = result.insertedId;
          createdCount++;
          this.logger.debug(`   Created: ${this.getRecordIdentifier(record)}`);
          if (this.afterUpsert) {
            await this.afterUpsert(
              { ...record, _id: insertedId },
              true,
              context,
            );
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
        this.logger.error(`   Stack: ${error.stack}`);
        this.logger.error(
          `   Record: ${JSON.stringify(record).substring(0, 200)}`,
        );
      }
    }
    return { created: createdCount, skipped: skippedCount };
  }
  protected cleanRecordForMongo(record: any): any {
    const cleaned: any = {};
    for (const key in record) {
      if (key.startsWith('_') && key !== '_id') {
        continue;
      }
      const value = record[key];
      cleaned[key] = value;
    }
    return cleaned;
  }
  protected async updateRecordMongo(
    existingId: ObjectId,
    record: any,
    db: Db,
    collectionName: string,
  ): Promise<void> {
    const cleanedRecord = this.cleanRecordForMongo(record);
    if (Object.keys(cleanedRecord).length > 0) {
      await db
        .collection(collectionName)
        .updateOne({ _id: existingId }, { $set: cleanedRecord });
    }
  }
}
