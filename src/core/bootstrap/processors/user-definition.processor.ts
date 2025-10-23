import { Injectable } from '@nestjs/common';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { BcryptService } from '../../auth/services/bcrypt.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { ObjectId } from 'mongodb';
import { Knex } from 'knex';
import { Db } from 'mongodb';

@Injectable()
export class UserDefinitionProcessor extends BaseTableProcessor {
  constructor(
    private readonly bcryptService: BcryptService,
    private readonly queryBuilder: QueryBuilderService,
  ) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    const transformedRecords = await Promise.all(
      records.map(async (record) => {
        const transformed = {
          ...record,
          _plainPassword: record.password,
        };

        // Add default values
        if (transformed.isRootAdmin === undefined) transformed.isRootAdmin = false;
        if (transformed.isSystem === undefined) transformed.isSystem = false;

        // Add timestamps for MongoDB
        if (isMongoDB) {
          const now = new Date();
          if (!transformed.createdAt) transformed.createdAt = now;
          if (!transformed.updatedAt) transformed.updatedAt = now;
        }

        // Handle role reference (many-to-one)
        if (record.role && typeof record.role === 'string') {
          const roleName = record.role;
          const role = await this.queryBuilder.findOneWhere('role_definition', {
            name: roleName,
          });

          if (!role) {
            this.logger.warn(`Role '${roleName}' not found for user ${record.email}, setting to null`);
            transformed.role = null;
          } else {
            if (isMongoDB) {
              // MongoDB: Store role as ObjectId
              transformed.role = typeof role._id === 'string'
                ? new ObjectId(role._id)
                : role._id;
            } else {
              // SQL: Convert to roleId
              transformed.roleId = role.id;
              delete transformed.role;
            }
          }
        }

        return transformed;
      }),
    );

    return transformedRecords;
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

    // Transform records first (handle role reference conversion)
    const transformedRecords = await this.transformRecords(records, context);

    let createdCount = 0;
    let skippedCount = 0;

    for (const record of transformedRecords) {
      try {
        const uniqueWhere = this.getUniqueIdentifier(record);
        const existingRecord = await db.collection(collectionName).findOne(uniqueWhere);

        if (existingRecord) {
          skippedCount++;
          this.logger.log(`   Skipped: ${this.getRecordIdentifier(record)}`);
        } else {
          const cleanedRecord = this.cleanRecordForMongo(record);

          if (record._plainPassword) {
            cleanedRecord.password = await this.bcryptService.hash(record._plainPassword);
            delete cleanedRecord._plainPassword;
          }

          // MongoDB: Initialize owner relation fields only
          // Many-to-one relation - always set, even if null
          cleanedRecord.role = cleanedRecord.role || null;

          // NOTE: allowedRoutePermissions is inverse M2M - NOT stored

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