import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { BcryptService } from '../../auth/services/bcrypt.service';

@Injectable()
export class UserDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly bcryptService: BcryptService) {
    super();
  }

  async transformRecords(records: any[], context?: any): Promise<any[]> {
    // Keep plain password for later hashing
    return records.map((record) => ({
      ...record,
      _plainPassword: record.password,
    }));
  }
  
  // Override processKnex to handle user creation properly
  async processKnex(records: any[], knex: any, tableName: string, context?: any): Promise<any> {
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
          // User exists - NEVER update (skip entirely)
          skippedCount++;
          this.logger.log(`   ⏩ Skipped: ${this.getRecordIdentifier(record)}`);
        } else {
          // New user - generate UUID, hash password and insert
          const cleanedRecord = this.cleanRecordForKnex(record);
          cleanedRecord.id = cleanedRecord.id || randomUUID(); // Auto UUID
          cleanedRecord.password = await this.bcryptService.hash(record._plainPassword);
          delete cleanedRecord._plainPassword;
          
          await knex(tableName).insert(cleanedRecord);
          createdCount++;
          this.logger.log(`   ✅ Created: ${this.getRecordIdentifier(record)}`);
          
          if (this.afterUpsert) {
            await this.afterUpsert({ ...record, id: cleanedRecord.id }, true, context);
          }
        }
      } catch (error) {
        this.logger.error(`❌ Error: ${error.message}`);
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