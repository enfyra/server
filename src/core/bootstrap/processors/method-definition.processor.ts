import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';

@Injectable()
export class MethodDefinitionProcessor extends BaseTableProcessor {
  constructor() {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    const isMongoDB = process.env.DB_TYPE === 'mongodb';

    return records.map((record) => {
      const transformed = {
        ...record,
        isSystem: true,
      };

      // MongoDB: Initialize owner M2M relations and add timestamps
      if (isMongoDB) {
        // Owner M2M relations - MUST store
        if (!transformed.routes) transformed.routes = [];
        if (!transformed.route_permissions) transformed.route_permissions = [];


        // Add timestamps
        const now = new Date();
        if (!transformed.createdAt) transformed.createdAt = now;
        if (!transformed.updatedAt) transformed.updatedAt = now;
      }

      return transformed;
    });
  }

  getUniqueIdentifier(record: any): object {
    return { method: record.method };
  }

  protected getCompareFields(): string[] {
    return ['method', 'isSystem'];
  }
}