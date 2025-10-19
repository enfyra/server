import { Logger } from '@nestjs/common';
import type { MetadataCacheService } from '../../cache/services/metadata-cache.service';
import { applyRelations } from './knex-helpers/query-with-relations';
import type { HookRegistry, HookEvent } from '../hooks/hook-registry';

export class QueryWrapper {
  constructor(
    private metadataCacheService: MetadataCacheService,
    private logger: Logger,
    private runHooks: (event: HookEvent, ...args: any[]) => Promise<any>,
  ) {}

  /**
   * Wraps a Knex query builder to intercept insert/update/delete/select operations
   * and run hooks before and after each operation
   */
  wrapQueryBuilder(qb: any): any {
    const self = this;
    const originalInsert = qb.insert;
    const originalUpdate = qb.update;
    const originalDelete = qb.delete || qb.del;
    const originalSelect = qb.select;
    const originalThen = qb.then;
    const tableName = qb._single?.table;

    qb._relationMetadata = null;
    qb._joinedRelations = new Set();

    qb.insert = async function(data: any, ...rest: any[]) {
      const processedData = await self.runHooks('beforeInsert', tableName, data);
      const result = await originalInsert.call(this, processedData, ...rest);
      return self.runHooks('afterInsert', tableName, result);
    };

    qb.update = async function(data: any, ...rest: any[]) {
      const processedData = await self.runHooks('beforeUpdate', tableName, data);
      const result = await originalUpdate.call(this, processedData, ...rest);
      return self.runHooks('afterUpdate', tableName, result);
    };

    qb.delete = qb.del = async function(...args: any[]) {
      await self.runHooks('beforeDelete', tableName, args);
      const result = await originalDelete.call(this, ...args);
      return self.runHooks('afterDelete', tableName, result);
    };

    qb.select = function(...fields: any[]) {
      const flatFields = fields.flat();
      const processedFields: string[] = [];

      for (const field of flatFields) {
        if (typeof field === 'string') {
          const parts = field.split('.');
          if (parts.length >= 2 && this._joinedRelations.has(parts[0])) {
            const relationName = parts[0];
            const columnName = parts[1];
            processedFields.push(`${relationName}.${columnName} as ${relationName}_${columnName}`);
          } else {
            processedFields.push(field);
          }
        } else {
          processedFields.push(field);
        }
      }

      return originalSelect.call(this, ...processedFields);
    };

    qb.then = function(onFulfilled: any, onRejected: any) {
      self.runHooks('beforeSelect', this, tableName);

      return originalThen.call(this, async (result: any) => {
        let processedResult = await self.runHooks('afterSelect', tableName, result);

        if (this._joinedRelations.size > 0) {
          const { nestJoinedData } = require('./knex-helpers/nest-joined-data');
          const relations = Array.from(this._joinedRelations);
          processedResult = nestJoinedData(processedResult, relations, tableName);
        }

        return onFulfilled ? onFulfilled(processedResult) : processedResult;
      }, onRejected);
    };

    qb.relations = function(relationNames: string[], metadataGetter?: (tableName: string) => any) {
      if (!relationNames || relationNames.length === 0) return this;

      const getter = metadataGetter || ((tbl: string) => self.metadataCacheService?.lookupTableByName(tbl));
      applyRelations(this, tableName, relationNames, getter);
      relationNames.forEach(r => this._joinedRelations.add(r.split('.')[0]));

      return this;
    };

    return qb;
  }
}
