import { Repository } from 'typeorm';
import { Logger } from '@nestjs/common';

export interface UpsertResult {
  created: number;
  skipped: number;
}

export abstract class BaseTableProcessor {
  protected readonly logger = new Logger(this.constructor.name);

  /**
   * Transform raw records before upsert (override if needed)
   */
  async transformRecords(records: any[], context?: any): Promise<any[]> {
    return records;
  }

  /**
   * Get unique identifier to find existing record (must implement)
   */
  abstract getUniqueIdentifier(record: any): object | object[];

  /**
   * Get human-readable identifier for logging (can be overridden)
   */
  protected getRecordIdentifier(record: any): string {
    // Default implementation - can be overridden in subclasses
    if (record.name) return record.name;
    if (record.label) return record.label;
    if (record.path) return record.path;
    if (record.type && record.label) return `${record.type}: ${record.label}`;
    if (record.email) return record.email;
    if (record.method) return record.method;
    return JSON.stringify(record).substring(0, 50) + '...';
  }

  /**
   * Process upsert for all records
   */
  async process(records: any[], repo: Repository<any>, context?: any): Promise<UpsertResult> {
    if (!records || records.length === 0) {
      return { created: 0, skipped: 0 };
    }

    // Transform records if needed
    const transformedRecords = await this.transformRecords(records, context);
    
    let createdCount = 0;
    let skippedCount = 0;

    for (const record of transformedRecords) {
      try {
        const uniqueWhere = this.getUniqueIdentifier(record);
        const whereConditions = Array.isArray(uniqueWhere) ? uniqueWhere : [uniqueWhere];

        // Try to find existing record
        let existingRecord = null;
        for (const whereCondition of whereConditions) {
          // Remove many-to-many fields from where condition to avoid query errors
          const cleanedCondition = { ...whereCondition };
          for (const key in cleanedCondition) {
            if (Array.isArray(cleanedCondition[key]) && cleanedCondition[key].length > 0 && typeof cleanedCondition[key][0] === 'object') {
              // This looks like a many-to-many relation, remove it
              delete cleanedCondition[key];
            }
          }
          
          existingRecord = await repo.findOne({ where: cleanedCondition });
          if (existingRecord) break;
        }

        if (existingRecord) {
          const hasChanges = this.detectRecordChanges(record, existingRecord);
          if (hasChanges) {
            await this.updateRecord(existingRecord.id, record, repo);
            skippedCount++; // Count as skipped since not created new
            const identifier = this.getRecordIdentifier(record);
            this.logger.log(`   🔄 Updated: ${identifier}`);
          } else {
            skippedCount++;
            const identifier = this.getRecordIdentifier(record);
            this.logger.log(`   ⏩ Skipped (no changes): ${identifier}`);
          }
        } else {
          // Create new record
          const created = repo.create(record);
          await repo.save(created);
          createdCount++;
          const identifier = this.getRecordIdentifier(record);
          this.logger.log(`   ✅ Created: ${identifier}`);
        }
      } catch (error) {
        this.logger.error(`❌ Error processing record: ${error.message}`);
        this.logger.debug(`Record: ${JSON.stringify(record)}`);
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
    return ['name', 'description']; // Default fields
  }

  protected hasValueChanged(newValue: any, existingValue: any): boolean {
    if (newValue === null && existingValue === null) return false;
    if (newValue === undefined && existingValue === undefined) return false;
    if (newValue === null || existingValue === null) return true;
    if (newValue === undefined || existingValue === undefined) return true;
    
    if (typeof newValue === 'object' && typeof existingValue === 'object') {
      return JSON.stringify(newValue) !== JSON.stringify(existingValue);
    }
    
    return newValue !== existingValue;
  }

  protected async updateRecord(existingId: any, record: any, repo: Repository<any>): Promise<void> {
    // Separate many-to-many fields from regular fields
    const regularFields: any = {};
    const manyToManyFields: any = {};
    
    for (const key in record) {
      if (Array.isArray(record[key]) && record[key].length > 0 && typeof record[key][0] === 'object') {
        // This looks like a many-to-many relation
        manyToManyFields[key] = record[key];
      } else {
        regularFields[key] = record[key];
      }
    }
    
    // Update regular fields first
    if (Object.keys(regularFields).length > 0) {
      await repo.update(existingId, regularFields);
    }
    
    // Then handle many-to-many relations using save
    if (Object.keys(manyToManyFields).length > 0) {
      await repo.save({
        id: existingId,
        ...manyToManyFields
      });
    }
  }
}