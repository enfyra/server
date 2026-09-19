import { QueryBuilderService } from '@enfyra/kernel';

function createService(options: {
  dbType?: 'postgres' | 'mongodb';
  knexService?: Record<string, any>;
  mongoService?: Record<string, any>;
} = {}) {
  const dbType = options.dbType ?? 'postgres';
  return new QueryBuilderService({
    databaseConfigService: {
      getDbType: () => dbType,
      isMongoDb: () => dbType === 'mongodb',
    },
    knexService: options.knexService,
    mongoService: options.mongoService,
    lazyRef: {},
  });
}

describe('QueryBuilderService contracts', () => {
  it('routes policy and field checks through the active database backend', async () => {
    const calls: string[] = [];
    const service = createService({
      dbType: 'mongodb',
      knexService: {
        runWithPolicy: async (_check: unknown, callback: () => Promise<string>) => {
          calls.push('knex-policy');
          return callback();
        },
        runWithFieldPermissionCheck: async (
          _check: unknown,
          callback: () => Promise<string>,
        ) => {
          calls.push('knex-field');
          return callback();
        },
      },
      mongoService: {
        runWithPolicy: async (_check: unknown, callback: () => Promise<string>) => {
          calls.push('mongo-policy');
          return callback();
        },
        runWithFieldPermissionCheck: async (
          _check: unknown,
          callback: () => Promise<string>,
        ) => {
          calls.push('mongo-field');
          return callback();
        },
      },
    });

    await expect(service.runWithPolicy(async () => {}, async () => 'policy'))
      .resolves.toBe('policy');
    await expect(service.runWithFieldPermissionCheck(
      async () => {},
      async () => 'field',
    )).resolves.toBe('field');

    expect(calls).toEqual(['mongo-policy', 'mongo-field']);
  });

  it('fails closed when the active backend cannot install policy context', async () => {
    const callback = vi.fn(async () => 'unsafe');
    const service = createService({ dbType: 'mongodb' });

    await expect(service.runWithPolicy(async () => {}, callback))
      .rejects.toThrow('MongoDB policy context is not available');
    expect(callback).not.toHaveBeenCalled();
  });

  it('preserves every filter operator and string-match semantic', () => {
    const service = createService() as any;

    const conditions = service.conditions.filterToWhere({
      age: { _gte: 18, _lte: 65 },
      name: { _starts_with: 'A' },
      bio: { _contains: 'safe' },
      suffix: { _ends_with: 'Z' },
    });

    expect(conditions).toEqual([
      { field: 'age', operator: '>=', value: 18 },
      { field: 'age', operator: '<=', value: 65 },
      { field: 'name', operator: '_starts_with', value: 'A' },
      { field: 'bio', operator: '_contains', value: 'safe' },
      { field: 'suffix', operator: '_ends_with', value: 'Z' },
    ]);
  });

  it('snapshots deferred batch rows before reporting acceptance', async () => {
    const inserted: Record<string, unknown>[][] = [];
    const service = createService({
      knexService: {
        insertManyWithCascade: async (_table: string, rows: Record<string, unknown>[]) => {
          inserted.push(rows);
        },
      },
    });
    const row = { id: 1, nested: { value: 'before' } };

    await service.insert('items', row, { batch: true });
    row.nested.value = 'after';
    await service.flushBatchInserts();

    expect(inserted).toEqual([[
      { id: 1, nested: { value: 'before' } },
    ]]);
  });

  it('rejects flush when an isolated batch row cannot be persisted', async () => {
    const service = createService({
      knexService: {
        insertManyWithCascade: async () => {
          throw new Error('write failed');
        },
      },
    });

    await service.insert('items', { id: 1 }, { batch: true });

    await expect(service.flushBatchInserts()).rejects.toThrow('write failed');
  });

  it('does not retry successful per-item inserts when a sibling fails', async () => {
    const attempts = new Map<number, number>();
    const service = createService({
      knexService: {
        insertWithCascade: async (_table: string, row: { id: number }) => {
          attempts.set(row.id, (attempts.get(row.id) ?? 0) + 1);
          if (row.id === 2) throw new Error('row failed');
        },
      },
    });

    await service.insert('items', { id: 1 }, { batch: true });
    await service.insert('items', { id: 2 }, { batch: true });

    await expect(service.flushBatchInserts()).rejects.toThrow('row failed');
    expect(attempts).toEqual(new Map([
      [1, 1],
      [2, 1],
    ]));
  });

  it('waits for every limited worker before propagating the first failure', async () => {
    const service = createService() as any;
    let delayedFinished = false;

    await expect(service.batchInsertBuffer.runLimited(
      ['fail', 'delay'],
      async (item: string) => {
        if (item === 'fail') throw new Error('worker failed');
        await new Promise((resolve) => setTimeout(resolve, 30));
        delayedFinished = true;
      },
    )).rejects.toThrow('worker failed');

    expect(delayedFinished).toBe(true);
  });

  it('rejects fractional positive integer configuration values', () => {
    const previous = process.env.DYNAMIC_CREATE_BATCH_CONCURRENCY;
    process.env.DYNAMIC_CREATE_BATCH_CONCURRENCY = '0.5';
    try {
      const service = createService() as any;
      expect(service.batchInsertBuffer.batchInsertConcurrency).toBe(5);
    } finally {
      if (previous === undefined) {
        delete process.env.DYNAMIC_CREATE_BATCH_CONCURRENCY;
      } else {
        process.env.DYNAMIC_CREATE_BATCH_CONCURRENCY = previous;
      }
    }
  });

  it('normalizes Mongo ObjectId strings for updateMany and deleteMany', async () => {
    const filters: Record<string, any>[] = [];
    const id = '507f1f77bcf86cd799439011';
    const collection = {
      async updateMany(filter: Record<string, any>) {
        filters.push(filter);
        return { modifiedCount: 1 };
      },
      find(filter: Record<string, any>) {
        filters.push(filter);
        return {
          async toArray() {
            return [];
          },
        };
      },
    };
    const service = createService({
      dbType: 'mongodb',
      mongoService: {
        collection: () => collection,
        ioSignalPublic: () => ({}),
      },
    });

    await service.updateMany('items', [id], { safe: true }, '_id');
    await service.deleteMany('items', [id], '_id');

    for (const filter of filters) {
      expect(filter._id.$in[0]?._bsontype).toBe('ObjectId');
      expect(filter._id.$in[0].toHexString()).toBe(id);
    }
  });

  it('accepts only the complete Mongo SELECT 1 health query', async () => {
    const command = vi.fn(async () => ({ ok: 1 }));
    const service = createService({
      dbType: 'mongodb',
      mongoService: {
        getDb: () => ({ command }),
      },
    });

    await expect(service.raw(' SELECT 1; ')).resolves.toEqual({ ok: 1 });
    await expect(service.raw('not select 1')).rejects.toThrow(
      'String queries not supported for MongoDB',
    );
    expect(command).toHaveBeenCalledTimes(1);
  });
});
