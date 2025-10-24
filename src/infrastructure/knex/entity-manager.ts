import { Logger } from '@nestjs/common';
import { Knex } from 'knex';
import type { KnexService } from './knex.service';

export class KnexEntityManager {
  constructor(
    private knexOrTrx: Knex | Knex.Transaction,
    private hooks: any,
    private dbType: string,
    private logger: Logger,
    private service: KnexService,
  ) {}

  async insert(tableName: string, data: any): Promise<any> {
    this.logger.log(`[EntityManager.insert] Table: ${tableName}, Data keys: ${Object.keys(data).join(', ')}`);

    const originalKnex = this.service['knexInstance'];
    (this.service as any).knexInstance = this.knexOrTrx;

    try {
      let processedData = data;
      for (const hook of this.hooks.beforeInsert) {
        processedData = await hook(tableName, processedData);
      }

      let insertedId: any;
      if (this.dbType === 'pg' || this.dbType === 'postgres') {
        const isJunctionTable = tableName.split('_').length >= 4; // e.g., "hook_definition_methods_method_definition" has 4+ parts

        if (isJunctionTable) {
          await this.knexOrTrx(tableName).insert(processedData);
          insertedId = null; // No auto-generated id for junction tables
        } else {
          const result = await this.knexOrTrx(tableName).insert(processedData).returning('id');
          insertedId = result[0]?.id || result[0];
        }
      } else {
        const result = await this.knexOrTrx(tableName).insert(processedData);
        insertedId = Array.isArray(result) ? result[0] : result;
      }

      const recordId = insertedId || data.id;
      this.logger.log(`   Inserted record ID: ${recordId}`);

      let hookResult = recordId;
      for (const hook of this.hooks.afterInsert) {
        hookResult = await hook(tableName, hookResult);
      }

      return recordId;
    } finally {
      (this.service as any).knexInstance = originalKnex;
    }
  }

  async update(tableName: string, recordId: any, data: any): Promise<void> {
    data.id = recordId;

    const originalKnex = this.service['knexInstance'];
    (this.service as any).knexInstance = this.knexOrTrx;

    try {
      let processedData = data;
      for (const hook of this.hooks.beforeUpdate) {
        processedData = await hook(tableName, processedData);
      }

      if (Object.keys(processedData).length > 0) {
        await this.knexOrTrx(tableName).where('id', recordId).update(processedData);
      }

      for (const hook of this.hooks.afterUpdate) {
        await hook(tableName, recordId);
      }
    } finally {
      (this.service as any).knexInstance = originalKnex;
    }
  }
}
