import { Knex } from 'knex';
import { appendFileSync } from 'node:fs';

export class KnexEntityManager {
  private static readonly idColumnCache = new Map<string, boolean>();

  constructor(
    private knexOrTrx: Knex | Knex.Transaction,
    private hooks: any,
    private dbType: string,
    private cascadeContextMap?: Map<string, any>,
  ) {}

  private async tableHasIdColumn(tableName: string): Promise<boolean> {
    const cached = KnexEntityManager.idColumnCache.get(tableName);
    if (cached !== undefined) return cached;

    const info = await this.knexOrTrx(tableName).columnInfo('id');
    const hasId = Boolean(info);
    KnexEntityManager.idColumnCache.set(tableName, hasId);
    return hasId;
  }

  async insert(tableName: string, data: any): Promise<any> {
    let processedData = data;
    for (const hook of this.hooks.beforeInsert) {
      processedData = await hook(tableName, processedData);
    }

    let insertedId: any;
    if (this.dbType === 'pg' || this.dbType === 'postgres') {
      if (await this.tableHasIdColumn(tableName)) {
        const result = await this.knexOrTrx(tableName)
          .insert(processedData)
          .returning('id');
        insertedId = result[0]?.id || result[0];
      } else {
        await this.knexOrTrx(tableName).insert(processedData);
        insertedId = null;
      }
    } else {
      const result = await this.knexOrTrx(tableName).insert(processedData);
      insertedId = Array.isArray(result) ? result[0] : result;
    }

    const recordId =
      insertedId ||
      (Array.isArray(processedData)
        ? processedData[0]?.id
        : processedData?.id) ||
      data.id;

    let hookResult = recordId;
    for (const hook of this.hooks.afterInsert) {
      hookResult = await hook(tableName, hookResult);
    }

    return recordId;
  }

  async insertMany(tableName: string, rows: any[]): Promise<any[]> {
    if (rows.length === 0) return [];

    const traceStarted = Date.now();
    const beforeHookDurations = new Array(this.hooks.beforeInsert.length).fill(
      0,
    );
    const afterHookDurations = new Array(this.hooks.afterInsert.length).fill(0);
    const relationDataByIndex = rows.map((row) => this.extractRelationData(row));
    const beforeHooksStarted = Date.now();
    let processedRows: any = rows;
    for (
      let hookIndex = 0;
      hookIndex < this.hooks.beforeInsert.length;
      hookIndex++
    ) {
      const hook = this.hooks.beforeInsert[hookIndex];
      const hookStarted = Date.now();
      processedRows = await hook(tableName, processedRows);
      beforeHookDurations[hookIndex] += Date.now() - hookStarted;
    }
    this.cascadeContextMap?.delete(tableName);
    const processedArray = Array.isArray(processedRows)
      ? processedRows
      : [processedRows];
    const prepared = rows.map((row, index) => ({
      original: row,
      data: processedArray[index],
      relationData: { relationData: relationDataByIndex[index] },
    }));
    const beforeHooksMs = Date.now() - beforeHooksStarted;

    let recordIds: any[] = [];
    const sqlInsertStarted = Date.now();
    if (this.dbType === 'pg' || this.dbType === 'postgres') {
      if (await this.tableHasIdColumn(tableName)) {
        const result = await this.knexOrTrx(tableName)
          .insert(prepared.map((item) => item.data))
          .returning('id');
        recordIds = result.map((item: any) => item?.id ?? item);
      } else {
        await this.knexOrTrx(tableName).insert(
          prepared.map((item) => item.data),
        );
        recordIds = prepared.map(() => null);
      }
    } else {
      const result = await this.knexOrTrx(tableName).insert(
        prepared.map((item) => item.data),
      );
      const firstId = Array.isArray(result) ? result[0] : result;
      recordIds = prepared.map(
        (item, index) => item.data?.id ?? firstId + index,
      );
    }
    const sqlInsertMs = Date.now() - sqlInsertStarted;

    const afterHooksStarted = Date.now();
    const afterInsertManyHooks = this.hooks.afterInsertMany || [];
    const batchedAfterInsertHooks = new Set(
      afterInsertManyHooks.length > 0
        ? this.hooks.afterInsert.filter(
            (hook: any) => hook.batchedByAfterInsertMany,
          )
        : [],
    );
    if (afterInsertManyHooks.length > 0) {
      const entries = prepared.map((item, index) => ({
        recordId: recordIds[index] || item.data?.id || item.original?.id,
        contextData: item.relationData || {},
      }));
      for (
        let hookIndex = 0;
        hookIndex < afterInsertManyHooks.length;
        hookIndex++
      ) {
        const hook = afterInsertManyHooks[hookIndex];
        const hookStarted = Date.now();
        await hook(tableName, entries);
        afterHookDurations[hookIndex] += Date.now() - hookStarted;
      }
    }
    for (let index = 0; index < prepared.length; index++) {
      const recordId =
        recordIds[index] ||
        prepared[index].data?.id ||
        prepared[index].original?.id;
      if (this.cascadeContextMap) {
        this.cascadeContextMap.set(tableName, prepared[index].relationData || {});
      }
      for (let hookIndex = 0; hookIndex < this.hooks.afterInsert.length; hookIndex++) {
        const hook = this.hooks.afterInsert[hookIndex];
        if (batchedAfterInsertHooks.has(hook)) continue;
        const hookStarted = Date.now();
        await hook(tableName, recordId);
        afterHookDurations[hookIndex] += Date.now() - hookStarted;
      }
    }
    const afterHooksMs = Date.now() - afterHooksStarted;
    this.trace('knex_insert_many', {
      tableName,
      count: rows.length,
      beforeHooksMs,
      sqlInsertMs,
      afterHooksMs,
      beforeHookDurations,
      afterHookDurations,
      totalMs: Date.now() - traceStarted,
    });

    return recordIds;
  }

  async update(tableName: string, recordId: any, data: any): Promise<void> {
    data.id = recordId;

    let processedData = data;
    for (const hook of this.hooks.beforeUpdate) {
      processedData = await hook(tableName, processedData);
    }

    if (Object.keys(processedData).length > 0) {
      await this.knexOrTrx(tableName)
        .where('id', recordId)
        .update(processedData);
    }

    for (const hook of this.hooks.afterUpdate) {
      await hook(tableName, recordId);
    }
  }

  private trace(event: string, data: Record<string, any>): void {
    const traceFile = process.env.DYNAMIC_CREATE_BATCH_TRACE_FILE;
    if (!traceFile) return;
    appendFileSync(
      traceFile,
      JSON.stringify({ event, ts: Date.now(), ...data }) + '\n',
    );
  }

  private extractRelationData(data: any): Record<string, any> {
    const relationData: Record<string, any> = {};
    if (!data || typeof data !== 'object' || Array.isArray(data)) {
      return relationData;
    }
    for (const key in data) {
      const value = data[key];
      if (Array.isArray(value)) {
        relationData[key] = value;
      } else if (
        value &&
        typeof value === 'object' &&
        !Buffer.isBuffer(value) &&
        !(value instanceof Date)
      ) {
        relationData[key] = value;
      }
    }
    return relationData;
  }
}
