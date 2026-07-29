import { AsyncLocalStorage } from 'async_hooks';
import { beforeEach, describe, expect, it, vi } from 'vitest';

let activeAbortSignal: AbortSignal | undefined;

vi.mock('@enfyra/kernel', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@enfyra/kernel')>();
  return {
    ...actual,
    getIoAbortSignal: () => activeAbortSignal,
  };
});

import { KnexHookManagerService } from '../../src/engines/knex/services/knex-hook-manager.service';

function createService() {
  return new KnexHookManagerService({
    runtimeRegistryService: {
      getTableMetadata: vi.fn(),
    } as any,
    replicationManager: {} as any,
  });
}

function createMockKnex(name: string) {
  const builders: any[] = [];
  const knexFn: any = (tableName: string) => {
    const builder: any = {
      _single: { table: tableName },
      _statements: [] as any[],
      _knexInstance: name,
      _executedStatements: null as any,
    };
    builder.insert = vi.fn(async function (this: any) {
      this._executedStatements = [...(this._statements || [])];
      return { inserted: true };
    });
    builder.update = vi.fn(async function (this: any) {
      this._executedStatements = [...(this._statements || [])];
      return { updated: true };
    });
    builder.delete = vi.fn(async function (this: any) {
      this._executedStatements = [...(this._statements || [])];
      return { deleted: true };
    });
    builder.del = builder.delete;
    builder.then = vi.fn((resolve: any) => Promise.resolve([]).then(resolve));
    builders.push(builder);
    return builder;
  };
  knexFn._name = name;
  knexFn._builders = builders;
  knexFn.transaction = vi.fn(async (cb: any) => {
    const trx: any = (tableName: string) => knexFn(tableName);
    trx.commit = vi.fn();
    trx.rollback = vi.fn();
    trx.isCompleted = vi.fn(() => false);
    trx._name = `${name}:trx`;
    return cb(trx);
  });
  return knexFn;
}

describe('Replica write routing', () => {
  beforeEach(() => {
    activeAbortSignal = undefined;
    vi.clearAllMocks();
  });

  it('copies WHERE statements from replica builder to master builder on delete', async () => {
    const service = createService();
    const masterKnex = createMockKnex('master');
    const replicaKnex = createMockKnex('replica');

    const replicaBuilder: any = replicaKnex('users');
    replicaBuilder._statements = [
      { grouping: 'where', type: 'whereBasic', column: 'id', operator: '=', value: 42, not: false, bool: 'and', asColumn: false },
    ];

    const knexContext = new AsyncLocalStorage<any>();
    const cascadeContext = new AsyncLocalStorage<Map<string, any>>();

    const wrapped = service.wrapQueryBuilder(
      replicaBuilder,
      replicaKnex as any,
      () => masterKnex as any,
      knexContext,
      cascadeContext,
    );

    await wrapped.delete();

    expect(masterKnex.transaction).toHaveBeenCalledOnce();
    const masterBuilder = masterKnex._builders[0];
    expect(masterBuilder).toBeDefined();
    expect(masterBuilder._statements).toHaveLength(1);
    expect(masterBuilder._statements[0]).toMatchObject({
      column: 'id',
      operator: '=',
      value: 42,
    });
    expect(masterBuilder._executedStatements).toHaveLength(1);
    expect(masterBuilder._executedStatements[0]).toMatchObject({ column: 'id', value: 42 });
  });

  it('copies WHERE statements on update with replica routing', async () => {
    const service = createService();
    const masterKnex = createMockKnex('master');
    const replicaKnex = createMockKnex('replica');

    const replicaBuilder: any = replicaKnex('orders');
    replicaBuilder._statements = [
      { grouping: 'where', type: 'whereBasic', column: 'status', operator: '=', value: 'pending', not: false, bool: 'and', asColumn: false },
      { grouping: 'where', type: 'whereBasic', column: 'user_id', operator: '=', value: 7, not: false, bool: 'and', asColumn: false },
    ];

    const knexContext = new AsyncLocalStorage<any>();
    const cascadeContext = new AsyncLocalStorage<Map<string, any>>();

    const wrapped = service.wrapQueryBuilder(
      replicaBuilder,
      replicaKnex as any,
      () => masterKnex as any,
      knexContext,
      cascadeContext,
    );

    await wrapped.update({ status: 'done' });

    const masterBuilder = masterKnex._builders[0];
    expect(masterBuilder._statements).toHaveLength(2);
    expect(masterBuilder._executedStatements).toHaveLength(2);
  });

  it('opens transaction on master knex, not replica', async () => {
    const service = createService();
    const masterKnex = createMockKnex('master');
    const replicaKnex = createMockKnex('replica');

    const replicaBuilder: any = replicaKnex('items');
    replicaBuilder._statements = [];

    const knexContext = new AsyncLocalStorage<any>();
    const cascadeContext = new AsyncLocalStorage<Map<string, any>>();

    const wrapped = service.wrapQueryBuilder(
      replicaBuilder,
      replicaKnex as any,
      () => masterKnex as any,
      knexContext,
      cascadeContext,
    );

    await wrapped.delete();

    expect(masterKnex.transaction).toHaveBeenCalledOnce();
    expect(replicaKnex.transaction).not.toHaveBeenCalled();
  });

  it('does not create new builder when already on master', async () => {
    const service = createService();
    const masterKnex = createMockKnex('master');

    const masterBuilder: any = masterKnex('products');
    masterBuilder._statements = [
      { grouping: 'where', type: 'whereBasic', column: 'id', operator: '=', value: 1, not: false, bool: 'and', asColumn: false },
    ];

    const knexContext = new AsyncLocalStorage<any>();
    const cascadeContext = new AsyncLocalStorage<Map<string, any>>();

    const wrapped = service.wrapQueryBuilder(
      masterBuilder,
      masterKnex as any,
      () => masterKnex as any,
      knexContext,
      cascadeContext,
    );

    await wrapped.delete();

    expect(masterBuilder._executedStatements).toHaveLength(1);
    expect(masterBuilder._executedStatements[0]).toMatchObject({ column: 'id', value: 1 });
    expect(masterKnex._builders).toHaveLength(1);
  });

  it('preserves multiple WHERE clauses without mutation', async () => {
    const service = createService();
    const masterKnex = createMockKnex('master');
    const replicaKnex = createMockKnex('replica');

    const originalStatements = [
      { grouping: 'where', type: 'whereBasic', column: 'a', operator: '=', value: 1, not: false, bool: 'and', asColumn: false },
      { grouping: 'where', type: 'whereBasic', column: 'b', operator: '>', value: 5, not: false, bool: 'and', asColumn: false },
    ];

    const replicaBuilder: any = replicaKnex('data');
    replicaBuilder._statements = originalStatements;

    const knexContext = new AsyncLocalStorage<any>();
    const cascadeContext = new AsyncLocalStorage<Map<string, any>>();

    const wrapped = service.wrapQueryBuilder(
      replicaBuilder,
      replicaKnex as any,
      () => masterKnex as any,
      knexContext,
      cascadeContext,
    );

    await wrapped.delete();

    const masterBuilder = masterKnex._builders[0];
    expect(masterBuilder._statements).toHaveLength(2);
    expect(masterBuilder._statements).not.toBe(originalStatements);
    expect(originalStatements).toHaveLength(2);
  });
});
