import { Logger } from '@nestjs/common';
import { Knex } from 'knex';
import type { KnexService } from './knex.service';

/**
 * Internal EntityManager for transaction-aware operations
 * This is used internally by KnexService to handle insert/update with hooks and cascades
 * All database operations use knexOrTrx, which can be either Knex instance or Transaction
 */
export class KnexEntityManager {
  constructor(
    private knexOrTrx: Knex | Knex.Transaction,
    private hooks: any,
    private dbType: string,
    private logger: Logger,
    private service: KnexService,
  ) {}

  /**
   * Insert with hooks and cascades
   * All operations (main insert + cascades) use the same knexOrTrx
   */
  async insert(tableName: string, data: any): Promise<any> {
    this.logger.log(`[EntityManager.insert] Table: ${tableName}, Data keys: ${Object.keys(data).join(', ')}`);

    // Temporarily replace knexInstance so hooks use knexOrTrx
    const originalKnex = this.service['knexInstance'];
    (this.service as any).knexInstance = this.knexOrTrx;

    try {
      // Run beforeInsert hooks
      let processedData = data;
      for (const hook of this.hooks.beforeInsert) {
        processedData = await hook(tableName, processedData);
      }

      // Perform insert using knexOrTrx
      let insertedId: any;
      if (this.dbType === 'pg' || this.dbType === 'postgres') {
        // Detect junction tables: they have composite keys with pattern like "tableA_relationName_tableB"
        // Junction tables have multiple underscores and contain two table names
        const isJunctionTable = tableName.split('_').length >= 4; // e.g., "hook_definition_methods_method_definition" has 4+ parts

        if (isJunctionTable) {
          // Junction table without id column - just insert without returning
          await this.knexOrTrx(tableName).insert(processedData);
          insertedId = null; // No auto-generated id for junction tables
        } else {
          // Regular table with auto-increment id
          const result = await this.knexOrTrx(tableName).insert(processedData).returning('id');
          insertedId = result[0]?.id || result[0];
        }
      } else {
        const result = await this.knexOrTrx(tableName).insert(processedData);
        insertedId = Array.isArray(result) ? result[0] : result;
      }

      const recordId = insertedId || data.id;
      this.logger.log(`   Inserted record ID: ${recordId}`);

      // Run afterInsert hooks (cascades)
      let hookResult = recordId;
      for (const hook of this.hooks.afterInsert) {
        hookResult = await hook(tableName, hookResult);
      }

      return recordId;
    } finally {
      // Restore original knexInstance
      (this.service as any).knexInstance = originalKnex;
    }
  }

  /**
   * Update with hooks and cascades
   * All operations (main update + cascades) use the same knexOrTrx
   */
  async update(tableName: string, recordId: any, data: any): Promise<void> {
    // Add recordId to data so hooks can use it
    data.id = recordId;

    // Temporarily replace knexInstance so hooks use knexOrTrx
    const originalKnex = this.service['knexInstance'];
    (this.service as any).knexInstance = this.knexOrTrx;

    try {
      // Run beforeUpdate hooks
      let processedData = data;
      for (const hook of this.hooks.beforeUpdate) {
        processedData = await hook(tableName, processedData);
      }

      // Perform update using knexOrTrx
      if (Object.keys(processedData).length > 0) {
        await this.knexOrTrx(tableName).where('id', recordId).update(processedData);
      }

      // Run afterUpdate hooks (cascades)
      for (const hook of this.hooks.afterUpdate) {
        await hook(tableName, recordId);
      }
    } finally {
      // Restore original knexInstance
      (this.service as any).knexInstance = originalKnex;
    }
  }
}
