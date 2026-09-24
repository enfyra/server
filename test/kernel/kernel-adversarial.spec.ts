import {
  BatchExecutionBudget,
  BatchFetchEngine,
  CHUNKED_FETCH_CONCURRENCY,
  WHERE_IN_CHUNK_SIZE,
  chunkedFetch,
  parseBatchFields,
  perParentRun,
} from '@enfyra/kernel';

describe('kernel batch utilities adversarial behavior', () => {
  test('chunkedFetch returns immediately for an empty input', async () => {
    let calls = 0;
    await expect(
      chunkedFetch([], async () => {
        calls += 1;
        return [];
      }),
    ).resolves.toEqual([]);
    expect(calls).toBe(0);
  });

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

  test('chunkedFetch stops dequeuing and settles active chunks before rejecting', async () => {
    const started: number[] = [];
    let activeFinished = false;
    const values = [0, 1, 2, 3, 4];

    await expect(
      chunkedFetch(
        values,
        async (chunk: number[]) => {
          started.push(chunk[0]);
          if (chunk[0] === 0) throw new Error('chunk failed');
          await new Promise((resolve) => setTimeout(resolve, 5));
          activeFinished = true;
          return chunk;
        },
        1,
      ),
    ).rejects.toThrow('chunk failed');
    expect(started).toEqual([0, 1, 2, 3]);
    expect(activeFinished).toBe(true);
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

  test('perParentRun rejects invalid concurrency instead of skipping input', async () => {
    await expect(perParentRun([1], async (id) => id, 0)).rejects.toThrow(
      'positive integer',
    );
    await expect(perParentRun([1], async (id) => id, 1.5)).rejects.toThrow(
      'positive integer',
    );
  });

  test('perParentRun invokes exact duplicate IDs only once', async () => {
    const calls: number[] = [];
    const result = await perParentRun(
      [1, 1, 2],
      async (id) => {
        calls.push(id);
        return id * 10;
      },
      2,
    );
    expect(calls.sort()).toEqual([1, 2]);
    expect(result).toEqual(new Map([['1', 10], ['2', 20]]));
  });

  test('perParentRun rejects colliding string keys', async () => {
    await expect(perParentRun([1, '1'], async (id) => id, 2)).rejects.toThrow(
      'identity collision',
    );
  });

  test('perParentRun stops dequeuing after failure and settles active work before rejecting', async () => {
    const started: number[] = [];
    let activeFinished = false;
    await expect(
      perParentRun(
        [1, 2, 3, 4],
        async (id) => {
          started.push(id);
          if (id === 1) throw new Error('failed');
          await new Promise((resolve) => setTimeout(resolve, 5));
          activeFinished = true;
          return id;
        },
        2,
      ),
    ).rejects.toThrow('failed');
    expect(started).toEqual([1, 2]);
    expect(activeFinished).toBe(true);
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

  test('BatchExecutionBudget reserves a released permit for the selected waiter', async () => {
    const budget = new BatchExecutionBudget(1);
    let releaseFirst!: () => void;
    const firstGate = new Promise<void>((resolve) => {
      releaseFirst = resolve;
    });
    let active = 0;
    let maxActive = 0;
    const runTracked = (gate?: Promise<void>) =>
      budget.run(async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        if (gate) await gate;
        await Promise.resolve();
        active -= 1;
      });

    const first = runTracked(firstGate);
    await Promise.resolve();
    const selectedWaiter = runTracked();
    releaseFirst();
    let lateArrival!: Promise<void>;
    queueMicrotask(() => {
      lateArrival = runTracked();
    });
    await first;
    await Promise.resolve();
    await Promise.all([selectedWaiter, lateArrival]);

    expect(maxActive).toBe(1);
  });

  test('BatchExecutionBudget revokes a granted waiter after concurrent failure', async () => {
    const budget = new BatchExecutionBudget(2);
    let releaseSuccess!: () => void;
    let releaseFailure!: () => void;
    const successGate = new Promise<void>((resolve) => {
      releaseSuccess = resolve;
    });
    const failureGate = new Promise<void>((resolve) => {
      releaseFailure = resolve;
    });
    let waiterRan = false;

    const success = budget.run(async () => successGate);
    const failure = budget.run(async () => {
      await failureGate;
      throw new Error('concurrent failure');
    });
    await Promise.resolve();
    const waiter = budget.run(async () => {
      waiterRan = true;
    });
    releaseSuccess();
    queueMicrotask(releaseFailure);

    const results = await Promise.allSettled([success, failure, waiter]);
    expect(results[1].status).toBe('rejected');
    expect(results[2].status).toBe('rejected');
    expect(waiterRan).toBe(false);
  });

  test('BatchExecutionBudget treats undefined rejection as cancellation', async () => {
    const budget = new BatchExecutionBudget(1);
    let queuedTaskRan = false;
    const results = await Promise.allSettled([
      budget.run(async () => {
        throw undefined;
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

  test('BatchFetchEngine validates owner sort metadata even without references', async () => {
    const adapter: any = {
      keyOf: String,
      resolveOwnerFkKey: () => 'ownerId',
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    await expect(
      engine.execute([{ ownerId: null }], [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id'],
          userSort: 'missing',
        },
      ]),
    ).rejects.toThrow(/does not exist/i);
  });

  test('BatchFetchEngine applies filters to primary-key-only owner relations', async () => {
    let fetchOwnerCalled = false;
    const adapter: any = {
      keyOf: String,
      buildScalarRef: (value: any) => ({ id: value }),
      getTargetPkField: () => 'id',
      resolveFields: () => ({
        isPkOnly: true,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => {
        fetchOwnerCalled = true;
        return { docs: [] };
      },
      resolveOwnerFkKey: () => 'ownerId',
      resolveParentPk: () => 'id',
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents = [{ id: 1, ownerId: 10 }];

    await engine.execute(parents, [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
        userFilter: { active: { _eq: true } },
      },
    ]);

    expect(fetchOwnerCalled).toBe(true);
    expect(parents[0].owner).toBeNull();
  });

  test('BatchFetchEngine initializes empty inverse and many-to-many relations', async () => {
    const adapter: any = {
      keyOf: String,
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      resolveInverseFkField: () => 'parentId',
      resolveParentPk: () => 'id',
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents: any[] = [{ other: 1 }];

    await engine.execute(
      parents,
      [
        {
          relationName: 'children',
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
        },
        {
          relationName: 'profile',
          type: 'one-to-one',
          isInverse: true,
          targetTable: 'profiles',
          fields: ['id'],
        },
        {
          relationName: 'tags',
          type: 'many-to-many',
          targetTable: 'tags',
          fields: ['id'],
        },
      ],
      3,
      0,
      'parents',
    );

    expect(parents[0].children).toEqual([]);
    expect(parents[0].profile).toBeNull();
    expect(parents[0].tags).toEqual([]);
  });

  test('BatchFetchEngine preserves explicit fields when deep adds only filter options', async () => {
    let nestedFields: string[] | undefined;
    const adapter: any = {
      keyOf: String,
      resolveOwnerFkKey: () => 'ownerId',
      getTargetPkField: () => 'id',
      buildScalarRef: (value: any) => ({ id: value }),
      resolveFields: (fields: string[]) => {
        if (fields.some((field) => field === 'profile' || field.startsWith('profile.'))) {
          return {
            isPkOnly: false,
            nestedDescs: [
              {
                relationName: 'profile',
                type: 'one-to-one',
                isInverse: true,
                targetTable: 'profiles',
                fields: ['name'],
              },
            ],
            fetchSpec: { selectCols: ['id'], pkCol: 'id' },
          };
        }
        nestedFields = fields;
        return {
          isPkOnly: true,
          nestedDescs: [],
          fetchSpec: { selectCols: ['id'], pkCol: 'id' },
        };
      },
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
      fetchInverse: async () => ({ docs: [], groupKeyField: 'userId' }),
      resolveInverseFkField: () => 'userId',
      resolveParentPk: () => 'id',
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [
        {
          propertyName: 'profile',
          type: 'one-to-one',
          isInverse: true,
          targetTableName: 'profiles',
        },
      ],
    }));

    await engine.execute([{ ownerId: 10 }], [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['profile.name'],
        nestedDeep: { profile: { filter: { active: { _eq: true } } } },
      },
    ]);

    expect(nestedFields).toEqual(['name']);
  });

  test.each([
    { profile: { fields: ['name', 7] } },
    { profile: { sort: ['name', 7] } },
    { profile: { limit: 'invalid' } },
    { profile: { limit: [1] } },
    { profile: { page: 0 } },
    { missing: {} },
  ])('BatchFetchEngine rejects malformed nested deep descriptors %#', async (nestedDeep) => {
    let fetchOwnerCalls = 0;
    const adapter: any = {
      keyOf: String,
      resolveOwnerFkKey: () => 'ownerId',
      getTargetPkField: () => 'id',
      buildScalarRef: (value: any) => ({ id: value }),
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => {
        fetchOwnerCalls += 1;
        return { docs: [{ id: 10 }] };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [
        {
          propertyName: 'profile',
          type: 'one-to-one',
          targetTableName: 'profiles',
        },
      ],
    }));
    await expect(
      engine.execute([{ ownerId: 10 }], [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id'],
          nestedDeep,
        },
      ]),
    ).rejects.toThrow(/fields|sort|limit|page|nested deep relation/i);
    expect(fetchOwnerCalls).toBe(0);
  });

  test.each([NaN, Infinity, -1, 1.5])(
    'BatchFetchEngine rejects invalid maximum depth %s',
    async (maxDepth) => {
      const engine = new BatchFetchEngine({} as any, async () => null);
      await expect(
        engine.execute(
          [],
          [
            {
              relationName: 'children',
              type: 'one-to-many',
              targetTable: 'children',
              fields: ['id'],
            },
          ],
          maxDepth,
        ),
      ).rejects.toThrow(/maximum query depth/i);
    },
  );

  test.each([NaN, Infinity, -1, 1.5])(
    'BatchFetchEngine rejects invalid current depth %s',
    async (currentDepth) => {
      const engine = new BatchFetchEngine({} as any, async () => null);
      await expect(
        engine.execute(
          [],
          [
            {
              relationName: 'children',
              type: 'one-to-many',
              targetTable: 'children',
              fields: ['id'],
            },
          ],
          3,
          currentDepth,
        ),
      ).rejects.toThrow(/current query depth/i);
    },
  );

  test('BatchFetchEngine fails closed when supplied parent metadata is missing', async () => {
    const engine = new BatchFetchEngine({} as any, async () => null);
    await expect(
      engine.execute(
        [],
        [
          {
            relationName: 'children',
            type: 'one-to-many',
            targetTable: 'children',
            fields: ['id'],
          },
        ],
        3,
        0,
        'parents',
      ),
    ).rejects.toThrow(/parent table/i);
  });

  test('BatchFetchEngine defines relation keys without changing object prototypes', async () => {
    const relationName = '__proto__';
    const adapter: any = {
      keyOf: String,
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'parentId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchInverse: async () => ({
        docs: [{ id: 2, parentId: 1 }],
        groupKeyField: 'parentId',
      }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parent = { id: 1 };
    const originalPrototype = Object.getPrototypeOf(parent);

    await engine.execute(
      [parent],
      [
        {
          relationName,
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
        },
      ],
      3,
      0,
      'parents',
    );

    expect(Object.getPrototypeOf(parent)).toBe(originalPrototype);
    expect(Object.prototype.hasOwnProperty.call(parent, relationName)).toBe(true);
    expect((parent as any)[relationName]).toEqual([{ id: 2, parentId: 1 }]);
  });

  test('BatchFetchEngine validates nested deep options for empty parent results', async () => {
    let fetchOwnerCalls = 0;
    const adapter: any = {
      keyOf: String,
      resolveOwnerFkKey: () => 'ownerId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => {
        fetchOwnerCalls += 1;
        return { docs: [] };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [
        {
          propertyName: 'profile',
          type: 'one-to-one',
          targetTableName: 'profiles',
        },
      ],
    }));

    await expect(
      engine.execute([], [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id'],
          nestedDeep: { profile: null },
        },
      ]),
    ).rejects.toThrow(/nested deep option/i);
    expect(fetchOwnerCalls).toBe(0);
  });

  test('BatchFetchEngine validates inverse descriptors before empty-id returns', async () => {
    let fetchInverseCalls = 0;
    const adapter: any = {
      keyOf: String,
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'parentId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchInverse: async () => {
        fetchInverseCalls += 1;
        return { docs: [], groupKeyField: 'parentId' };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));

    await expect(
      engine.execute(
        [{ other: 1 }],
        [
          {
            relationName: 'children',
            type: 'one-to-many',
            targetTable: 'children',
            fields: ['id'],
            userSort: 'missing',
          },
        ],
        3,
        0,
        'parents',
      ),
    ).rejects.toThrow(/does not exist/i);
    expect(fetchInverseCalls).toBe(0);
  });

  test('BatchFetchEngine ignores inherited nested deep options', async () => {
    const adapter: any = {
      keyOf: String,
      resolveOwnerFkKey: () => 'ownerId',
      getTargetPkField: () => 'id',
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'ownerId',
      fetchInverse: async () => ({ docs: [], groupKeyField: 'ownerId' }),
      resolveFields: (_fields: string[], targetMeta: any) => ({
        isPkOnly: false,
        nestedDescs:
          targetMeta.name === 'users'
            ? [
                {
                  relationName: 'constructor',
                  type: 'one-to-one',
                  isInverse: true,
                  targetTable: 'constructor_profiles',
                  fields: ['id'],
                },
              ]
            : [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations:
        table === 'users'
          ? [
              {
                propertyName: 'constructor',
                type: 'one-to-one',
                isInverse: true,
                targetTableName: 'constructor_profiles',
              },
              {
                propertyName: 'profile',
                type: 'one-to-one',
                isInverse: true,
                targetTableName: 'profiles',
              },
            ]
          : [],
    }));

    const parents = [{ ownerId: 10 }];
    await expect(
      engine.execute(parents, [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['constructor.id'],
          nestedDeep: { profile: {} },
        },
      ]),
    ).resolves.toBeUndefined();
    expect(parents[0].owner).toEqual({
      id: 10,
      constructor: null,
      profile: null,
    });
  });

  test('BatchFetchEngine gives each parent its own empty relation array', async () => {
    const adapter: any = {
      keyOf: String,
      resolveParentPk: () => 'id',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents: any[] = [{ other: 1 }, { other: 2 }];
    await engine.execute(
      parents,
      [
        {
          relationName: 'children',
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
        },
      ],
      3,
      0,
      'parents',
    );
    parents[0].children.push('mutated');
    expect(parents[1].children).toEqual([]);
  });

  test('BatchFetchEngine rejects null nested deep entries', async () => {
    const adapter: any = {
      keyOf: String,
      resolveOwnerFkKey: () => 'ownerId',
      getTargetPkField: () => 'id',
      buildScalarRef: (value: any) => ({ id: value }),
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [
        {
          propertyName: 'profile',
          type: 'one-to-one',
          targetTableName: 'profiles',
        },
      ],
    }));
    const parents = [{ ownerId: 10 }];

    await expect(
      engine.execute(parents, [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id'],
          nestedDeep: { profile: null },
        },
      ]),
    ).rejects.toThrow(/nested deep option/i);
  });

  test('BatchFetchEngine settles sibling descriptors before rejecting', async () => {
    let slowFinished = false;
    const adapter: any = {
      keyOf: String,
      buildScalarRef: (value: any) => ({ id: value }),
      getTargetPkField: () => 'id',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      resolveOwnerFkKey: (desc: any) => desc.relationName,
      fetchOwner: async (_table: string, _ids: any[], _spec: any, desc: any) => {
        if (desc.relationName === 'failed') throw new Error('descriptor failed');
        await new Promise((resolve) => setTimeout(resolve, 5));
        slowFinished = true;
        return { docs: [{ id: 2 }] };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents = [{ failed: 1, slow: 2 }];

    await expect(
      engine.execute(parents, [
        {
          relationName: 'failed',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['name'],
        },
        {
          relationName: 'slow',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['name'],
        },
      ]),
    ).rejects.toThrow('descriptor failed');
    expect(slowFinished).toBe(true);
    expect(parents).toEqual([{ failed: 1, slow: 2 }]);
  });

  test('BatchFetchEngine validates every descriptor before dispatching adapter work', async () => {
    let fetchOwnerCalls = 0;
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: (desc: any) => `${desc.relationName}Id`,
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => {
        fetchOwnerCalls += 1;
        return { docs: [] };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));

    await expect(
      engine.execute(
        [{ ownerId: 1 }],
        [
          {
            relationName: 'owner',
            type: 'many-to-one',
            targetTable: 'users',
            fields: ['name'],
          },
          {
            relationName: 'invalid',
            type: 'many-to-one',
            targetTable: 'users',
            fields: {} as any,
          },
        ],
      ),
    ).rejects.toThrow(/fields/i);
    expect(fetchOwnerCalls).toBe(0);
  });

  test('BatchFetchEngine rejects duplicate sibling relation descriptors', async () => {
    const engine = new BatchFetchEngine({} as any, async () => null);
    const descriptor = {
      relationName: 'children',
      type: 'one-to-many' as const,
      targetTable: 'children',
      fields: ['id'],
    };
    await expect(
      engine.execute([], [descriptor, { ...descriptor }]),
    ).rejects.toThrow(/duplicate deep relation/i);
  });

  test('BatchFetchEngine keeps sibling relation outputs during helper cleanup', async () => {
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: (desc: any) => `${desc.relationName}Id`,
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async (_table: string, ids: number[]) => ({
        docs: ids.map((id) => ({ id })),
      }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents = [{ ownerId: 10, editorId: 20 }];

    await engine.execute(parents, [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
        injectedParentFields: ['editor'],
      },
      {
        relationName: 'editor',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
      },
    ]);

    expect(parents[0]).toEqual({
      ownerId: 10,
      editorId: 20,
      owner: { id: 10 },
      editor: { id: 20 },
    });
  });

  test('BatchFetchEngine does not evaluate unrelated parent getters while staging', async () => {
    let getterCalls = 0;
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: () => 'ownerId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parent = { ownerId: 10 };
    Object.defineProperty(parent, 'lazy', {
      get() {
        getterCalls += 1;
        return 'value';
      },
      enumerable: true,
      configurable: true,
    });

    await engine.execute([parent], [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
      },
    ]);

    expect(getterCalls).toBe(0);
    expect((parent as any).owner).toEqual({ id: 10 });
  });

  test('BatchFetchEngine stages non-enumerable parent identity keys', async () => {
    let receivedParentIds: number[] = [];
    const adapter: any = {
      keyOf: String,
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'parentId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchInverse: async (
        _table: string,
        _field: string,
        parentIds: number[],
      ) => {
        receivedParentIds = parentIds;
        return { docs: [], groupKeyField: 'parentId' };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parent = {};
    Object.defineProperty(parent, 'id', {
      value: 1,
      enumerable: false,
      configurable: true,
    });

    await engine.execute(
      [parent],
      [
        {
          relationName: 'children',
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
        },
      ],
      3,
      0,
      'parents',
    );

    expect(receivedParentIds).toEqual([1]);
    expect((parent as any).children).toEqual([]);
  });

  test('BatchFetchEngine preserves explicitly selected owner foreign keys', async () => {
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: () => 'ownerId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents = [{ ownerId: 10 }];

    await engine.execute(parents, [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
      },
    ]);

    expect(parents).toEqual([{ ownerId: 10, owner: { id: 10 } }]);
  });

  test('BatchFetchEngine removes only declared hydration helper fields', async () => {
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: () => 'ownerId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents = [{ ownerId: 10 }];

    await engine.execute(parents, [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
        injectedParentFields: ['ownerId'],
      },
    ]);

    expect(parents).toEqual([{ owner: { id: 10 } }]);
  });

  test('BatchFetchEngine validates the full commit before mutating parents', async () => {
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: () => 'ownerId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchOwner: async () => ({ docs: [{ id: 10 }, { id: 20 }] }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const first = { ownerId: 10 };
    const second = Object.preventExtensions({ ownerId: 20 });
    const parents = [first, second];

    await expect(
      engine.execute(parents, [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id'],
        },
      ]),
    ).rejects.toThrow(/non-extensible/i);
    expect(first).toEqual({ ownerId: 10 });
    expect(second).toEqual({ ownerId: 20 });
  });

  test('BatchFetchEngine resolves each descriptor projection once', async () => {
    let resolveFieldsCalls = 0;
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveOwnerFkKey: () => 'ownerId',
      resolveFields: () => {
        resolveFieldsCalls += 1;
        return {
          isPkOnly: false,
          nestedDescs: [],
          fetchSpec: { selectCols: ['id'], pkCol: 'id' },
        };
      },
      fetchOwner: async () => ({ docs: [{ id: 10 }] }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));

    await engine.execute([{ ownerId: 10 }], [
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['id'],
      },
    ]);

    expect(resolveFieldsCalls).toBe(1);
  });

  test('BatchFetchEngine preserves nested relations rooted at the inverse foreign key', async () => {
    const adapter: any = {
      keyOf: String,
      getTargetPkField: () => 'id',
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'author',
      resolveOwnerFkKey: () => 'author',
      resolveFields: (_fields: string[], targetMeta: any) => ({
        isPkOnly: false,
        nestedDescs:
          targetMeta.name === 'comments'
            ? [
                {
                  relationName: 'author',
                  type: 'many-to-one',
                  targetTable: 'users',
                  fields: ['name'],
                },
              ]
            : [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchInverse: async () => ({
        docs: [{ id: 2, author: 1 }],
        groupKeyField: 'author',
      }),
      fetchOwner: async () => ({ docs: [{ id: 1, name: 'Ada' }] }),
      postProcessInverseChild: (doc: any, fkField: string, requested: boolean) => {
        if (!requested) delete doc[fkField];
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations:
        table === 'comments'
          ? [
              {
                propertyName: 'author',
                type: 'many-to-one',
                targetTableName: 'users',
              },
            ]
          : [],
    }));
    const parents = [{ id: 1 }];

    await engine.execute(
      parents,
      [
        {
          relationName: 'comments',
          type: 'one-to-many',
          targetTable: 'comments',
          fields: ['author.name'],
        },
      ],
      3,
      0,
      'posts',
    );

    expect(parents[0].comments[0].author).toEqual({ id: 1, name: 'Ada' });
  });

  test('BatchFetchEngine assigns empty inverse relations to parents without primary keys', async () => {
    const adapter: any = {
      keyOf: (value: any) => {
        if (value == null) throw new Error('missing identity');
        return String(value);
      },
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'postId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchInverse: async () => ({
        docs: [{ id: 2, postId: 1 }],
        groupKeyField: 'postId',
      }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents: any[] = [{ id: 1 }, { name: 'missing-id' }];

    await engine.execute(
      parents,
      [
        {
          relationName: 'comments',
          type: 'one-to-many',
          targetTable: 'comments',
          fields: ['id'],
        },
      ],
      3,
      0,
      'posts',
    );

    expect(parents[0].comments).toEqual([{ id: 2, postId: 1 }]);
    expect(parents[1].comments).toEqual([]);
  });

  test('BatchFetchEngine excludes parents without primary keys from many-to-many fetches', async () => {
    let receivedParents: any[] = [];
    const adapter: any = {
      keyOf: (value: any) => {
        if (value == null) throw new Error('missing identity');
        return String(value);
      },
      resolveParentPk: () => 'id',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchM2M: async (parents: any[]) => {
        receivedParents = parents;
        return {
          grouped: new Map([['1', [{ id: 3 }]]]),
          docs: [{ id: 3 }],
        };
      },
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents: any[] = [{ id: 1 }, { name: 'missing-id' }];

    await engine.execute(
      parents,
      [
        {
          relationName: 'tags',
          type: 'many-to-many',
          targetTable: 'tags',
          fields: ['id'],
        },
      ],
      3,
      0,
      'posts',
    );

    expect(receivedParents).toHaveLength(1);
    expect(receivedParents[0].id).toBe(1);
    expect(parents[0].tags).toEqual([{ id: 3 }]);
    expect(parents[1].tags).toEqual([]);
  });

  test('BatchFetchEngine preserves inverse descriptor child columns on parents', async () => {
    const adapter: any = {
      keyOf: String,
      resolveParentPk: () => 'id',
      resolveInverseFkField: () => 'ownerId',
      resolveFields: () => ({
        isPkOnly: false,
        nestedDescs: [],
        fetchSpec: { selectCols: ['id'], pkCol: 'id' },
      }),
      fetchInverse: async () => ({ docs: [], groupKeyField: 'ownerId' }),
    };
    const engine = new BatchFetchEngine(adapter, async (table) => ({
      name: table,
      columns: [{ name: 'id', type: 'integer' }],
      relations: [],
    }));
    const parents = [{ id: 1, ownerId: 'parent-owned-value' }];

    await engine.execute(
      parents,
      [
        {
          relationName: 'children',
          type: 'one-to-many',
          targetTable: 'children',
          fields: ['id'],
          fkColumn: 'ownerId',
        },
      ],
      3,
      0,
      'parents',
    );

    expect(parents[0].ownerId).toBe('parent-owned-value');
  });

  test.each([{ userLimit: 0 }, { userLimit: 1 }, { userPage: 2 }])(
    'BatchFetchEngine applies owner pagination instead of using the primary-key shortcut %#',
    async (pagination) => {
      let fetchOwnerCalls = 0;
      const adapter: any = {
        keyOf: String,
        buildScalarRef: (value: any) => ({ id: value }),
        getTargetPkField: () => 'id',
        resolveOwnerFkKey: () => 'ownerId',
        resolveFields: () => ({
          isPkOnly: true,
          nestedDescs: [],
          fetchSpec: { selectCols: ['id'], pkCol: 'id' },
        }),
        fetchOwner: async () => {
          fetchOwnerCalls += 1;
          return { docs: [] };
        },
      };
      const engine = new BatchFetchEngine(adapter, async (table) => ({
        name: table,
        columns: [{ name: 'id', type: 'integer' }],
        relations: [],
      }));

      await engine.execute([{ ownerId: 1 }], [
        {
          relationName: 'owner',
          type: 'many-to-one',
          targetTable: 'users',
          fields: ['id'],
          ...pagination,
        },
      ]);
      expect(fetchOwnerCalls).toBe(1);
    },
  );

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
