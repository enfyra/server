import { Knex } from 'knex';

export class KnexEntityManager {
  private static readonly idColumnCache = new Map<string, boolean>();

  constructor(
    private knexOrTrx: Knex | Knex.Transaction,
    private hooks: any,
    private dbType: string,
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
}
