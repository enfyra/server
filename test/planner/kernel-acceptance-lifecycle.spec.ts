import { BatchExecutionBudget, QueryBuilderService, createEnfyraKernel } from '@enfyra/kernel';

function service(knexService: Record<string, unknown>) {
  return new QueryBuilderService({
    knexService,
    databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false },
    lazyRef: {},
  });
}

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

describe('Kernel acceptance lifecycle regressions', () => {
  it('does not publish any rows when snapshotting an input batch fails', async () => {
    const write = vi.fn(async () => {});
    const query = service({ insertManyWithCascade: write });
    await expect(query.insertWithOptions({ table: 'items', batch: true, data: [{ id: 1 }, { id: 2, fn() {} }] })).rejects.toThrow();
    await query.flushBatchInserts();
    expect(write).not.toHaveBeenCalled();
  });

  it('attempts good rows in both halves after isolating a bad bulk row', async () => {
    const inserted: number[] = [];
    const query = service({ insertManyWithCascade: async (_table: string, rows: { id: number }[]) => {
      if (rows.some((row) => row.id === 1)) throw new Error('bad row');
      inserted.push(...rows.map((row) => row.id));
    } });
    await query.insertWithOptions({ table: 'items', batch: true, data: [{ id: 1 }, { id: 2 }, { id: 3 }] });
    await expect(query.flushBatchInserts()).rejects.toThrow('bad row');
    expect(inserted).toEqual([2, 3]);
  });

  it('drains remaining tables after every initial worker fails', async () => {
    const attempted: string[] = [];
    const query = service({ insertManyWithCascade: async (table: string) => {
      attempted.push(table);
      if (table === 'bad') throw new Error('bad table');
    } });
    (query as any).batchInsertBuffer.batchInsertConcurrency = 1;
    await query.insert('bad', { id: 1 }, { batch: true });
    await query.insert('good', { id: 2 }, { batch: true });
    await expect(query.flushBatchInserts()).rejects.toThrow('bad table');
    expect(attempted).toEqual(['bad', 'good']);
  });

  it('serializes concurrent flush callers even after a shared wait', async () => {
    const gate = deferred();
    let active = 0;
    let maximum = 0;
    const query = service({ insertManyWithCascade: async () => {
      active++;
      maximum = Math.max(maximum, active);
      await gate.promise;
      await new Promise((resolve) => setTimeout(resolve, 5));
      active--;
    } });
    await query.insertWithOptions({ table: 'items', batch: true, data: [{ id: 1 }, { id: 2 }, { id: 3 }] });
    (query as any).batchInsertBuffer.batchInsertMaxSize = 1;
    const calls = [query.flushBatchInserts(), query.flushBatchInserts(), query.flushBatchInserts()];
    gate.resolve();
    await Promise.all(calls);
    expect(maximum).toBe(1);
    expect(active).toBe(0);
  });

  it('does not let trace write failures drop accepted data', async () => {
    const write = vi.fn(async () => {});
    const query = service({ insertManyWithCascade: write });
    (query as any).batchInsertBuffer.batchInsertTraceFile = '/dev/null/not-a-file';
    await expect(query.insert('items', { id: 1 }, { batch: true })).resolves.toMatchObject({ accepted: true });
    await query.flushBatchInserts();
    expect(write).toHaveBeenCalledTimes(1);
  });

  it('preserves undefined rejection reasons from background flushes', async () => {
    const query = service({ insertManyWithCascade: async () => { throw undefined; } });
    await query.insert('items', { id: 1 }, { batch: true });
    await (query as any).batchInsertBuffer.flushBatchInsertBuffer().catch(() => {});
    await expect(query.flushBatchInserts()).rejects.toBeUndefined();
  });

  it('makes concurrent shutdown callers await the same flush outcome', async () => {
    const gate = deferred();
    const kernel = createEnfyraKernel({
      knexService: { insertManyWithCascade: async () => { await gate.promise; throw new Error('flush failed'); } },
      databaseConfigService: { getDbType: () => 'postgres', isMongoDb: () => false } as any,
      lazyRef: {},
    });
    await kernel.queryBuilderService.insert('items', { id: 1 }, { batch: true });
    const first = kernel.onDestroy();
    const second = kernel.onDestroy();
    const settled = Promise.allSettled([first, second]);
    gate.resolve();
    const outcomes = await settled;
    expect(outcomes.map((outcome) => outcome.status)).toEqual(['rejected', 'rejected']);
  });

  it('does not start granted work after a sibling cancels the budget', async () => {
    const budget = new BatchExecutionBudget(2);
    const third = vi.fn(async () => {});
    const failure = new Error('cancel');
    const outcomes = await Promise.allSettled([
      budget.run(async () => {}),
      budget.run(() => Promise.resolve().then(() => { throw failure; })),
      budget.run(third),
    ]);
    expect(third).not.toHaveBeenCalled();
    expect(outcomes[2]).toMatchObject({ status: 'rejected', reason: failure });
  });
});
