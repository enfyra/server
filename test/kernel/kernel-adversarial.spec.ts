import {
  BatchFetchEngine,
  BatchExecutionBudget,
  CHUNKED_FETCH_CONCURRENCY,
  WHERE_IN_CHUNK_SIZE,
  chunkedFetch,
  parseBatchFields,
  perParentRun,
} from '@enfyra/kernel';

describe('kernel batch utilities adversarial behavior', () => {
  test('chunkedFetch preserves input chunk order even when chunks finish out of order', async () => {
    const values = Array.from(
      { length: WHERE_IN_CHUNK_SIZE * 2 + 37 },
      (_, index) => index,
    );
    const seenChunks: number[][] = [];

    const result = await chunkedFetch(values, async (chunk: number[]) => {
      seenChunks.push(chunk);
      const chunkIndex = Math.floor(chunk[0] / WHERE_IN_CHUNK_SIZE);
      await new Promise((resolve) =>
        setTimeout(resolve, chunkIndex === 0 ? 20 : 1),
      );
      return chunk.map((value) => value * 2);
    });

    expect(seenChunks).toHaveLength(3);
    expect(result).toEqual(values.map((value) => value * 2));
  });

  test('chunkedFetch limits concurrent chunk fetches', async () => {
    const values = Array.from(
      { length: WHERE_IN_CHUNK_SIZE * 6 + 1 },
      (_, index) => index,
    );
    let active = 0;
    let maxActive = 0;

    await chunkedFetch(values, async (chunk: number[]) => {
      active += 1;
      maxActive = Math.max(maxActive, active);
      await new Promise((resolve) => setTimeout(resolve, 5));
      active -= 1;
      return chunk;
    });

    expect(maxActive).toBeLessThanOrEqual(CHUNKED_FETCH_CONCURRENCY);
    expect(maxActive).toBeGreaterThan(1);
  });

  test('chunkedFetch supports a caller-specific chunk size', async () => {
    const chunks: number[][] = [];
    const result = await chunkedFetch(
      [1, 2, 3, 4, 5],
      async (chunk: number[]) => {
        chunks.push(chunk);
        return chunk;
      },
      2,
    );

    expect(chunks).toEqual([[1, 2], [3, 4], [5]]);
    expect(result).toEqual([1, 2, 3, 4, 5]);
  });

  test('chunkedFetch propagates fetch errors instead of returning partial data', async () => {
    const values = Array.from(
      { length: WHERE_IN_CHUNK_SIZE + 1 },
      (_, index) => index,
    );

    await expect(
      chunkedFetch(values, async (chunk: number[]) => {
        if (chunk[0] >= WHERE_IN_CHUNK_SIZE) {
          throw new Error('chunk failed');
        }
        return chunk;
      }),
    ).rejects.toThrow('chunk failed');
  });

  test('perParentRun limits concurrency and maps results by parent id string', async () => {
    const parentIds = Array.from({ length: 25 }, (_, index) => index + 1);
    let active = 0;
    let maxActive = 0;

    const result = await perParentRun(
      parentIds,
      async (id: number) => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await new Promise((resolve) => setTimeout(resolve, 3));
        active -= 1;
        return id * 10;
      },
      4,
    );

    expect(maxActive).toBeLessThanOrEqual(4);
    expect(result.size).toBe(parentIds.length);
    expect(result.get('1')).toBe(10);
    expect(result.get('25')).toBe(250);
  });

  test('perParentRun returns an empty map without calling the worker for empty input', async () => {
    let called = false;
    const result = await perParentRun(
      [],
      async () => {
        called = true;
        return 1;
      },
      4,
    );

    expect(called).toBe(false);
    expect(result.size).toBe(0);
  });

  test('BatchExecutionBudget caps aggregate relation I/O concurrency', async () => {
    const budget = new BatchExecutionBudget(2);
    let active = 0;
    let maxActive = 0;

    await Promise.all(
      Array.from({ length: 8 }, () =>
        budget.run(async () => {
          active += 1;
          maxActive = Math.max(maxActive, active);
          await new Promise((resolve) => setTimeout(resolve, 3));
          active -= 1;
        }),
      ),
    );

    expect(maxActive).toBe(2);
  });

  test('BatchExecutionBudget cancels queued relation I/O after a failure', async () => {
    const budget = new BatchExecutionBudget(1);
    let queuedTaskRan = false;

    const results = await Promise.allSettled([
      budget.run(async () => {
        throw new Error('query failed');
      }),
      budget.run(async () => {
        queuedTaskRan = true;
      }),
    ]);

    expect(results[0].status).toBe('rejected');
    expect(results[1].status).toBe('rejected');
    expect(queuedTaskRan).toBe(false);
  });

  test('parseBatchFields keeps root wildcards and groups nested paths by first relation', () => {
    const parsed = parseBatchFields([
      '*',
      'id',
      'author.name',
      'author.company.name',
      'comments.body',
      'comments.author.name',
    ]);

    expect(parsed.rootFields).toEqual(['*', 'id']);
    expect(parsed.subRelations.get('author')).toEqual(['name', 'company.name']);
    expect(parsed.subRelations.get('comments')).toEqual([
      'body',
      'author.name',
    ]);
  });

  test('BatchFetchEngine deduplicates inverse parent ids before adapter fetch', async () => {
    let receivedParentIds: number[] = [];
    const adapter: any = {
      pkField: 'id',
      keyOf: (value: any) => String(value),
      buildScalarRef: (value: any) => ({ id: value }),
      getTargetPkField: () => 'id',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => [],
      fetchInverse: async (
        _targetTable: string,
        _fkField: string,
        parentIds: number[],
      ) => {
        receivedParentIds = parentIds;
        return {
          docs: [{ id: 10, parentId: 1 }],
          groupKeyField: 'parentId',
        };
      },
      fetchM2M: async () => ({ grouped: new Map(), docs: [] }),
      resolveOwnerFkKey: () => 'ownerId',
      resolveInverseFkField: () => 'parentId',
      resolveParentPk: () => 'id',
    };
    const metadataGetter = async (table: string) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    });
    const engine = new BatchFetchEngine(adapter, metadataGetter);
    const parents = [{ id: 1 }, { id: 1 }];

    await engine.execute(
      parents,
      [
        {
          relationName: 'children',
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
          userLimit: 2,
        },
      ],
      3,
      0,
      'parents',
    );

    expect(receivedParentIds).toEqual([1]);
    expect(parents[0].children).toHaveLength(1);
    expect(parents[1].children).toHaveLength(1);
  });

  test('BatchFetchEngine traces adapter-reported strategy and roundtrips', async () => {
    const traceEntries: Array<{ stage: string; meta?: Record<string, any> }> =
      [];
    const adapter: any = {
      pkField: 'id',
      keyOf: (value: any) => String(value),
      buildScalarRef: (value: any) => ({ id: value }),
      getTargetPkField: () => 'id',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => [],
      fetchInverse: async () => ({
        docs: [],
        groupKeyField: 'parentId',
        stats: {
          strategy: 'partitioned-top-k',
          roundtrips: 1,
        },
      }),
      fetchM2M: async () => ({ grouped: new Map(), docs: [] }),
      resolveOwnerFkKey: () => 'ownerId',
      resolveInverseFkField: () => 'parentId',
      resolveParentPk: () => 'id',
    };
    const metadataGetter = async (table: string) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    });
    const engine = new BatchFetchEngine(adapter, metadataGetter, {
      dur(stage: string, _startTs: number, meta?: Record<string, unknown>) {
        traceEntries.push({ stage, meta });
        return 0;
      },
    });

    await engine.execute(
      [{ id: 1 }, { id: 2 }],
      [
        {
          relationName: 'children',
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
          userLimit: 2,
        },
      ],
      3,
      0,
      'parents',
    );

    const entry = traceEntries.find((item) => item.stage.includes('children'));
    expect(entry?.meta?.strategy).toBe('partitioned-top-k');
    expect(entry?.meta?.roundtrips).toBe(1);
  });
});
