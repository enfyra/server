import { Injectable } from '@nestjs/common';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { BcryptService } from '../../auth/services/bcrypt.service';
import { Knex } from 'knex';
import { Db } from 'mongodb';

@Injectable()
export class UserDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly bcryptService: BcryptService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    return records.map((record) => ({
      ...record,
      _plainPassword: record.password,
    }));
  }

  async processSql(records: any[], knex: Knex, tableName: string, context?: any): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    const transformedRecords = await this.transformRecords(records, context);
    const { randomUUID } = await import('crypto');

    let createdCount = 0;
    let skippedCount = 0;

    for (const record of transformedRecords) {
      try {
        const uniqueWhere = this.getUniqueIdentifier(record);
        const existingRecord = await knex(tableName).where(uniqueWhere).first();

        if (existingRecord) {
          skippedCount++;
          this.logger.log(`   Skipped: ${this.getRecordIdentifier(record)}`);
        } else {
          const cleanedRecord = this.cleanRecordForKnex(record);
          cleanedRecord.id = cleanedRecord.id || randomUUID();
          cleanedRecord.password = await this.bcryptService.hash(record._plainPassword);
          delete cleanedRecord._plainPassword;

          await knex(tableName).insert(cleanedRecord);
          createdCount++;
          this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);

          if (this.afterUpsert) {
            await this.afterUpsert({ ...record, id: cleanedRecord.id }, true, context);
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
      }
    }

    return { created: createdCount, skipped: skippedCount };
  }

  async processMongo(records: any[], db: Db, collectionName: string, context?: any): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    let createdCount = 0;
    let skippedCount = 0;

    for (const record of records) {
      try {
        const uniqueWhere = this.getUniqueIdentifier(record);
        const existingRecord = await db.collection(collectionName).findOne(uniqueWhere);

        if (existingRecord) {
          skippedCount++;
          this.logger.log(`   Skipped: ${this.getRecordIdentifier(record)}`);
        } else {
          const cleanedRecord = this.cleanRecordForMongo(record);

          if (record.password) {
            cleanedRecord.password = await this.bcryptService.hash(record.password);
          }

          cleanedRecord.allowedRoutePermissions = [];

          const result = await db.collection(collectionName).insertOne(cleanedRecord);
          createdCount++;
          this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);

          if (this.afterUpsert) {
            await this.afterUpsert({ ...record, _id: result.insertedId }, true, context);
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${error.message}`);
      }
    }

    return { created: createdCount, skipped: skippedCount };
  }

  getUniqueIdentifier(record: any): object {
    return { email: record.email };
  }

  protected getCompareFields(): string[] {
    return ['email', 'isRootAdmin', 'isSystem'];
  }
}