import { Logger } from '../../../shared/logger';
import type { MetadataCacheService } from '../../cache';
import type { HookEvent } from '../hooks/hook-registry';

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
    const runHooks = (event: HookEvent, ...args: any[]) =>
      this.runHooks(event, ...args);
    const originalInsert = qb.insert;
    const originalUpdate = qb.update;
    const originalDelete = qb.delete || qb.del;
    const originalThen = qb.then;
    const tableName = qb._single?.table;

    qb.insert = async function (data: any, ...rest: any[]) {
      const processedData = await runHooks('beforeInsert', tableName, data);
      const result = await originalInsert.call(this, processedData, ...rest);
      return runHooks('afterInsert', tableName, result);
    };

    qb.update = async function (data: any, ...rest: any[]) {
      const processedData = await runHooks('beforeUpdate', tableName, data);
      const result = await originalUpdate.call(this, processedData, ...rest);
      return runHooks('afterUpdate', tableName, result);
    };

    qb.delete = qb.del = async function (...args: any[]) {
      await runHooks('beforeDelete', tableName, args);
      const result = await originalDelete.call(this, ...args);
      return runHooks('afterDelete', tableName, result);
    };

    qb.then = function (onFulfilled: any, onRejected: any) {
      runHooks('beforeSelect', this, tableName);

      return originalThen.call(
        this,
        async (result: any) => {
          const processedResult = await runHooks(
            'afterSelect',
            tableName,
            result,
          );
          return onFulfilled ? onFulfilled(processedResult) : processedResult;
        },
        onRejected,
      );
    };

    return qb;
  }
}
