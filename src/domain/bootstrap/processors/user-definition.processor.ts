import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { BcryptService } from '../../auth';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../../../shared/services';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { mapSequentially } from '../utils/map-sequentially.util';
import type { Knex } from 'knex';
import type { Db } from 'mongodb';

export class UserDefinitionProcessor extends BaseTableProcessor {
  private readonly bcryptService: BcryptService;
  private readonly queryBuilderService: IQueryBuilder;

  constructor(deps: {
    bcryptService: BcryptService;
    queryBuilderService: IQueryBuilder;
  }) {
    super();
    this.bcryptService = deps.bcryptService;
    this.queryBuilderService = deps.queryBuilderService;
  }

  async transformRecords(records: any[], _context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = await mapSequentially(
      records,
      async (record) => {
        const transformed = {
          ...record,
          _plainPassword: record.password,
        };
        if (transformed.isRootAdmin === undefined)
          transformed.isRootAdmin = false;
        if (transformed.isSystem === undefined) transformed.isSystem = false;
        if (isMongoDB) {
          const now = new Date();
          if (!transformed.createdAt) transformed.createdAt = now;
          if (!transformed.updatedAt) transformed.updatedAt = now;
        }

        const result = await this.autoTransformFkFields(
          transformed,
          'enfyra_user',
          this.queryBuilderService,
        );
        return result;
      },
    );
    return transformedRecords;
  }

  async processWithQueryBuilder(
    records: any[],
    queryBuilder: any,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    const existingRootAdmin = await queryBuilder.findOne({
      table: tableName,
      where: { isRootAdmin: true },
    });

    if (existingRootAdmin) {
      this.logger.log(
        `   RootAdmin already exists: ${existingRootAdmin.email}`,
      );
      return { created: 0, skipped: 0 };
    }

    const adminUser = await this.getAdminUserFromEnv();
    if (!adminUser) {
      this.logger.warn(
        `   No ADMIN_EMAIL/ADMIN_PASSWORD in .env, skipping rootAdmin creation`,
      );
      return { created: 0, skipped: 0 };
    }

    const transformedRecords = await this.transformRecords(
      [adminUser],
      context,
    );

    let createdCount = 0;
    let skippedCount = 0;

    for (const record of transformedRecords) {
      try {
        const uniqueWhere = this.getUniqueIdentifier(record);
        const existingRecord = await queryBuilder.findOne({
          table: tableName,
          where: uniqueWhere,
        });

        if (existingRecord) {
          skippedCount++;
          this.logger.log(`   Skipped: ${this.getRecordIdentifier(record)}`);
        } else {
          const insertData = { ...record };
          if (insertData._plainPassword) {
            insertData.password = await this.bcryptService.hash(
              insertData._plainPassword,
            );
            delete insertData._plainPassword;
          }
          const inserted = await queryBuilder.insert(tableName, insertData);
          createdCount++;
          this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
          if (this.afterUpsert) {
            const idField = queryBuilder.getPkField();
            await this.afterUpsert(
              { ...record, [idField]: inserted[idField] },
              true,
              context,
            );
          }
        }
      } catch (error) {
        this.logger.error(`Error: ${getErrorMessage(error)}`);
      }
    }
    return { created: createdCount, skipped: skippedCount };
  }

  async processSql(
    _records: any[],
    knex: Knex,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    const existingRootAdmin = await knex(tableName)
      .where({ isRootAdmin: true })
      .first();
    if (existingRootAdmin) {
      this.logger.log(
        `   RootAdmin already exists: ${existingRootAdmin.email}`,
      );
      return { created: 0, skipped: 0 };
    }

    const record = await this.prepareAdminRecord(context);
    if (!record) return { created: 0, skipped: 0 };
    if (await knex(tableName).where(this.getUniqueIdentifier(record)).first()) {
      return { created: 0, skipped: 1 };
    }

    const insertData = await this.hashPassword(record);
    const cleanedRecord = this.cleanRecordForKnex(
      this.prepareSqlInsertRecord(insertData, tableName),
    );
    const [inserted] =
      context?.dbType === 'postgres'
        ? await knex(tableName).insert(cleanedRecord, ['id'])
        : await knex(tableName).insert(cleanedRecord);
    if (this.afterUpsert) {
      await this.afterUpsert(
        { ...record, id: inserted?.id ?? inserted },
        true,
        context,
      );
    }
    this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
    return { created: 1, skipped: 0 };
  }

  async processMongo(
    _records: any[],
    db: Db,
    collectionName: string,
    context?: any,
  ): Promise<UpsertResult> {
    const collection = db.collection(collectionName);
    const existingRootAdmin = await collection.findOne({ isRootAdmin: true });
    if (existingRootAdmin) {
      this.logger.log(
        `   RootAdmin already exists: ${existingRootAdmin.email}`,
      );
      return { created: 0, skipped: 0 };
    }

    const record = await this.prepareAdminRecord(context);
    if (!record) return { created: 0, skipped: 0 };
    if (await collection.findOne(this.getUniqueIdentifier(record))) {
      return { created: 0, skipped: 1 };
    }

    const insertData = await this.hashPassword(record);
    const result = await collection.insertOne(
      this.cleanRecordForMongo(insertData),
    );
    if (this.afterUpsert) {
      await this.afterUpsert(
        { ...record, _id: result.insertedId },
        true,
        context,
      );
    }
    this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
    return { created: 1, skipped: 0 };
  }

  getUniqueIdentifier(record: any): object {
    return { email: record.email };
  }

  protected getCompareFields(): string[] {
    return ['email', 'isRootAdmin', 'isSystem'];
  }

  private async getAdminUserFromEnv(): Promise<any | null> {
    const adminEmail = process.env.ADMIN_EMAIL;
    const adminPassword = process.env.ADMIN_PASSWORD;
    if (!adminEmail || !adminPassword) {
      return null;
    }
    return {
      email: adminEmail,
      password: adminPassword,
      isRootAdmin: true,
      isSystem: true,
    };
  }

  private async prepareAdminRecord(context?: any): Promise<any | null> {
    const adminUser = await this.getAdminUserFromEnv();
    if (!adminUser) {
      this.logger.warn(
        `   No ADMIN_EMAIL/ADMIN_PASSWORD in .env, skipping rootAdmin creation`,
      );
      return null;
    }
    const [record] = await this.transformRecords([adminUser], context);
    return record ?? null;
  }

  private async hashPassword(record: any): Promise<any> {
    const insertData = { ...record };
    if (insertData._plainPassword) {
      insertData.password = await this.bcryptService.hash(
        insertData._plainPassword,
      );
      delete insertData._plainPassword;
    }
    return insertData;
  }
}
