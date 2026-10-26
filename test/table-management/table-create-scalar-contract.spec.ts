import { expect, it } from 'vitest';
import { SqlTableCreateService } from '../../src/modules/table-management/services/sql-table-create.service';
import { MongoTableCreateService } from '../../src/modules/table-management/services/mongo-table-create.service';

it.each(['postgres', 'mysql', 'mongodb'])('persists explicit validateBody values through the %s create writer', async backend => {
  for (const value of [undefined, true, false]) {
    let saved: any;
    const stop = new Error('captured metadata insert');
    const query = { where: () => query, first: async () => null, insert: async (data: unknown) => { saved = data; throw stop; } };
    const trx: any = () => query;
    trx.rollback = async () => {};
    trx.isCompleted = () => false;
    const knex = { transaction: async () => trx, schema: { hasTable: async () => false } };
    const prototype = backend === 'mongodb' ? MongoTableCreateService.prototype : SqlTableCreateService.prototype;
    const service = Object.create(prototype);
    Object.assign(service, {
      tableValidationService: { validateColumns: () => {}, validateRelations: () => {} },
      policyService: { checkSchemaMigration: async () => ({ allowed: true }) },
      queryBuilderService: { getDatabaseType: () => backend, getKnex: () => knex, getMongoDb: () => ({ listCollections: () => ({ toArray: async () => [] }) }), findOne: async () => null, insert: async (_table: string, data: unknown) => { saved = data; throw stop; } },
      runWithSchemaLock: async (_key: string, work: () => Promise<unknown>) => work(),
      validateAllColumnsUnique: () => {}, assertNotAborted: () => {}, logger: { error: () => {}, warn: () => {}, log: () => {} },
    });
    await service.createTable({ name: 'scalar_fixture', columns: [{ name: backend === 'mongodb' ? '_id' : 'id', type: backend === 'mongodb' ? 'objectId' : 'int', isPrimary: true }], ...(value !== undefined ? { validateBody: value } : {}) }).catch(() => {});
    expect(saved).toBeDefined();
    expect(saved.validateBody).toBe(value ?? true);
  }
});
