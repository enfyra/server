import { Injectable } from '@nestjs/common';
import { BaseTableProcessor, UpsertResult } from './base-table-processor';
import { BcryptService } from '../../auth/services/bcrypt.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';

@Injectable()
export class UserDefinitionProcessor extends BaseTableProcessor {
  constructor(
    private readonly bcryptService: BcryptService,
    private readonly queryBuilder: QueryBuilderService,
  ) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    const isMongoDB = DatabaseConfigService.instanceIsMongoDb();
    const transformedRecords = await Promise.all(
      records.map(async (record) => {
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
          'user_definition',
          this.queryBuilder,
        );
        return result;
      }),
    );
    return transformedRecords;
  }

  async processWithQueryBuilder(
    records: any[],
    queryBuilder: any,
    tableName: string,
    context?: any,
  ): Promise<UpsertResult> {
    const existingRootAdmin = await queryBuilder.findOneWhere(
      tableName,
      { isRootAdmin: true },
    );

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
        const existingRecord = await queryBuilder.findOneWhere(
          tableName,
          uniqueWhere,
        );

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
          const inserted = await queryBuilder.insertAndGet(
            tableName,
            insertData,
          );
          createdCount++;
          this.logger.log(`   Created: ${this.getRecordIdentifier(record)}`);
          if (this.afterUpsert) {
            const idField = queryBuilder.isMongoDb() ? '_id' : 'id';
            await this.afterUpsert(
              { ...record, [idField]: inserted[idField] },
              true,
              context,
            );
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
}
