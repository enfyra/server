import { ObjectId } from 'mongodb';
import { describe, expect, it } from 'vitest';
import { MongoBatchAdapter, resolveMongoJunctionInfo } from '@enfyra/kernel';

function metadata() {
  return {
    tables: new Map([
      [
        'users',
        {
          name: 'users',
          columns: [
            { name: '_id', isNullable: false },
            { name: 'teamId', isNullable: false },
          ],
          relations: [
            {
              propertyName: 'team',
              type: 'many-to-one',
              targetTableName: 'teams',
              foreignKeyColumn: 'teamId',
            },
            {
              propertyName: 'posts',
              type: 'one-to-many',
              targetTableName: 'posts',
              mappedBy: 'authorId',
            },
          ],
        },
      ],
      [
        'teams',
        {
          name: 'teams',
          columns: [{ name: '_id', isNullable: false }],
          relations: [],
        },
      ],
      [
        'posts',
        {
          name: 'posts',
          columns: [
            { name: '_id', isNullable: false },
            { name: 'authorId', isNullable: false },
            { name: 'teamId', isNullable: false },
          ],
          relations: [
            {
              propertyName: 'team',
              type: 'many-to-one',
              targetTableName: 'teams',
              foreignKeyColumn: 'teamId',
            },
          ],
        },
      ],
      [
        'targets',
        {
          name: 'targets',
          columns: [
            { name: '_id', isNullable: false },
            { name: 'teamId', isNullable: false },
          ],
          relations: [
            {
              propertyName: 'team',
              type: 'many-to-one',
              targetTableName: 'teams',
              foreignKeyColumn: 'teamId',
            },
          ],
        },
      ],
    ]),
  };
}

function runtime() {
  const findFilters: any[] = [];
  const aggregatePipelines: any[][] = [];
  const collection = {
    find(filter: any) {
      findFilters.push(filter);
      return { async toArray() { return []; } };
    },
    aggregate(pipeline: any[]) {
      aggregatePipelines.push(pipeline);
      return { async toArray() { return []; } };
    },
  };
  return {
    db: { collection: () => collection } as any,
    findFilters,
    aggregatePipelines,
  };
}

describe('Mongo batch adapter contracts', () => {
  it('intersects owner and inverse relation scopes with caller filters', async () => {
    const ownerRuntime = runtime();
    const owner = new MongoBatchAdapter(ownerRuntime.db, metadata());
    await owner.fetchOwner(
      'users',
      ['required-id'],
      { projection: { _id: 1 } },
      {
        relationName: 'owner',
        type: 'many-to-one',
        targetTable: 'users',
        fields: ['_id'],
        userFilter: { _id: { _eq: 'caller-id' } },
      },
    );
    expect(ownerRuntime.findFilters[0]).toEqual({
      $and: [
        { _id: { $in: ['required-id'] } },
        { _id: 'caller-id' },
      ],
    });

    const inverseRuntime = runtime();
    const inverse = new MongoBatchAdapter(inverseRuntime.db, metadata());
    await inverse.fetchInverse(
      'posts',
      'authorId',
      ['required-parent'],
      { projection: { _id: 1 } },
      {
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['_id'],
        userFilter: { authorId: { _eq: 'caller-parent' } },
      },
    );
    expect(inverseRuntime.findFilters[0]).toEqual({
      $and: [
        { authorId: { $in: ['required-parent'] } },
        { authorId: 'caller-parent' },
      ],
    });
  });

  it('uses type-aware map keys while preserving ObjectId normalization', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata());
    const id = new ObjectId();

    expect(adapter.keyOf(1)).not.toBe(adapter.keyOf('1'));
    expect(adapter.keyOf(null)).not.toBe(adapter.keyOf(''));
    expect(adapter.keyOf({ value: 1 })).not.toBe(adapter.keyOf({ value: 2 }));
    expect(adapter.keyOf({ a: 'x,"b":string:y' })).not.toBe(
      adapter.keyOf({ a: 'x', b: 'y' }),
    );
    expect(adapter.keyOf([1])).not.toBe(adapter.keyOf({ 0: 1 }));
    expect(adapter.keyOf(new Date(0))).not.toBe(adapter.keyOf(new Date(1)));
    expect(adapter.keyOf({ a: 1, b: 2 })).not.toBe(
      adapter.keyOf({ b: 2, a: 1 }),
    );
    expect(adapter.keyOf(id)).not.toBe(adapter.keyOf(id.toHexString()));
  });

  it('deduplicates owner ids before issuing chunked reads', async () => {
    const calls: any[][] = [];
    const db = {
      collection() {
        return {
          find(filter: any) {
            calls.push(filter._id.$in);
            return { async toArray() { return []; } };
          },
        };
      },
    } as any;
    const adapter = new MongoBatchAdapter(db, metadata());

    const result = await adapter.fetchOwner(
      'users',
      ['same-id', 'same-id'],
      { projection: { _id: 1 } },
    );

    expect(calls).toEqual([['same-id']]);
    expect(result.stats?.roundtrips).toBe(1);
  });

  it('batches dotted inverse sorts and supports pending FK renames', async () => {
    const pipelines: any[][] = [];
    const db = {
      collection() {
        return {
          aggregate(pipeline: any[]) {
            pipelines.push(pipeline);
            return {
              async toArray() {
                return [{ _id: 'child-1', oldAuthorId: 'parent-1' }];
              },
            };
          },
        };
      },
    } as any;
    const adapter = new MongoBatchAdapter(
      db,
      metadata(),
      new Map([
        ['posts', [{ oldName: 'oldAuthorId', newName: 'authorId' }]],
      ]),
    );

    const result = await adapter.fetchInverse(
      'posts',
      'authorId',
      ['parent-1', 'parent-2'],
      {
        projection: { _id: 1, authorId: 1 },
        pendingRenames: [{ oldName: 'oldAuthorId', newName: 'authorId' }],
        hiddenFields: ['oldAuthorId'],
      },
      {
        relationName: 'posts',
        type: 'one-to-many',
        targetTable: 'posts',
        fields: ['_id'],
        userSort: 'team._id',
      },
    );

    expect(pipelines).toHaveLength(1);
    expect(pipelines[0][0]).toEqual({
      $match: {
        $or: [
          { authorId: { $in: ['parent-1', 'parent-2'] } },
          { oldAuthorId: { $in: ['parent-1', 'parent-2'] } },
        ],
      },
    });
    expect(result.docs[0].authorId).toBe('parent-1');
    expect(result.docs[0]).not.toHaveProperty('oldAuthorId');
  });

  it('maps dotted logical ids and creates collision-free sort aliases', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata()) as any;
    const sort = adapter.buildSortFromTokens([
      'team.id',
      'a_b.c.value',
      'a.b_c.value',
    ]);
    const keys = Object.keys(sort);

    expect(keys.some((key) => key.endsWith('._id'))).toBe(true);
    expect(new Set(keys).size).toBe(3);
  });

  it('uses a total comparator compatible with MongoDB simple ordering', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata()) as any;
    expect(adapter.compareSortValues('Z', 'a')).toBeLessThan(0);
    expect(adapter.compareSortValues('é', 'z')).toBeGreaterThan(0);
    expect(adapter.compareSortValues(20, '10')).toBeLessThan(0);
    expect(adapter.compareSortValues(Number.NaN, Number.NaN)).toBe(0);
    expect(adapter.compareSortValues(Number.POSITIVE_INFINITY, Number.POSITIVE_INFINITY)).toBe(0);
    expect(Number.isNaN(adapter.compareSortValues(Number.NaN, 1))).toBe(false);
  });

  it('preserves projection mode while exposing hidden sort and tie-breaker fields', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata()) as any;

    expect(
      adapter.projectionWithSortFields({ name: 1, _id: 0 }, 'name'),
    ).toEqual({
      projection: { name: 1, _id: 1 },
      hiddenFields: ['_id'],
    });
    expect(
      adapter.projectionWithSortFields({ secret: 0, _id: 0 }, 'name'),
    ).toEqual({
      projection: { secret: 0 },
      hiddenFields: ['_id'],
    });
    expect(
      adapter.projectionWithSortFields({ _id: false }, 'name'),
    ).toEqual({
      projection: undefined,
      hiddenFields: ['_id'],
    });
  });

  it('applies nested public projections path-wise', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata()) as any;
    const excluded = [
      { _id: '1', profile: { name: 'visible', secret: 'hidden' } },
    ];
    adapter.applyPublicProjection(excluded, { 'profile.secret': 0 });
    expect(excluded).toEqual([
      { _id: '1', profile: { name: 'visible' } },
    ]);

    const included = [
      { _id: '1', profile: { name: 'visible', secret: 'hidden' }, extra: true },
    ];
    adapter.applyPublicProjection(included, { 'profile.name': 1, _id: 0 });
    expect(included).toEqual([{ profile: { name: 'visible' } }]);
  });

  it('adds a stable _id tie-breaker to aggregate sorts', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata()) as any;
    const pipeline = adapter.buildSortAggregatePipeline(
      'users',
      { active: true },
      'team._id',
    );

    expect(pipeline).toContainEqual({
      $sort: expect.objectContaining({ _id: 1 }),
    });
  });

  it('rejects inverse dotted relation sorts instead of emitting an invalid join', () => {
    const adapter = new MongoBatchAdapter(runtime().db, metadata()) as any;

    expect(() =>
      adapter.buildSortAggregatePipeline(
        'users',
        { active: true },
        'posts._id',
      ),
    ).toThrow('Dotted sort through inverse or to-many relation');
  });

  it('checks target existence before applying bounded PK-only M2M pagination', async () => {
    const pipelines: any[][] = [];
    const db = {
      collection() {
        return {
          aggregate(pipeline: any[]) {
            pipelines.push(pipeline);
            return { async toArray() { return []; } };
          },
        };
      },
    } as any;
    const adapter = new MongoBatchAdapter(db, metadata());

    await adapter.fetchM2M(
      [{ _id: 'parent-1' }],
      {
        relationName: 'targets',
        type: 'many-to-many',
        targetTable: 'targets',
        fields: ['_id'],
        userLimit: 1,
      },
      { name: 'parents', columns: [], relations: [] },
      { name: 'targets', columns: [{ name: '_id' }], relations: [] },
      { projection: { _id: 1 } },
    );

    const pipeline = pipelines[0];
    const lookupIndex = pipeline.findIndex((stage) => stage.$lookup);
    const topNIndex = pipeline.findIndex((stage) => stage.$group);
    expect(lookupIndex).toBeGreaterThan(-1);
    expect(lookupIndex).toBeLessThan(topNIndex);
  });

  it('keeps _id internally for M2M correlation when public projection excludes it', async () => {
    const info = resolveMongoJunctionInfo('parents', {
      type: 'many-to-many',
      propertyName: 'targets',
      targetTable: 'targets',
    })!;
    let targetProjection: any;
    const db = {
      collection(name: string) {
        if (name === info.junctionName) {
          return {
            find() {
              return {
                async toArray() {
                  return [
                    {
                      [info.selfColumn]: 'parent-1',
                      [info.otherColumn]: 'target-1',
                    },
                  ];
                },
              };
            },
          };
        }
        return {
          find(_filter: any, options: any) {
            targetProjection = options?.projection;
            return {
              async toArray() {
                return [{ _id: 'target-1', name: 'visible' }];
              },
            };
          },
        };
      },
    } as any;
    const adapter = new MongoBatchAdapter(db, metadata());

    const result = await adapter.fetchM2M(
      [{ _id: 'parent-1' }],
      {
        relationName: 'targets',
        type: 'many-to-many',
        targetTable: 'targets',
        fields: ['name'],
      },
      { name: 'parents', columns: [], relations: [] },
      {
        name: 'targets',
        columns: [{ name: '_id' }, { name: 'name' }],
        relations: [],
      },
      { projection: { name: 1, _id: 0 } },
    );

    expect(targetProjection._id).toBe(1);
    expect(result.grouped.get(adapter.keyOf('parent-1'))).toEqual([
      { name: 'visible' },
    ]);
  });

  it('chunks dotted M2M target aggregation beyond the Mongo $in bound', async () => {
    const info = resolveMongoJunctionInfo('parents', {
      type: 'many-to-many',
      propertyName: 'targets',
      targetTable: 'targets',
    })!;
    const targetIds = Array.from({ length: 5_001 }, (_, index) => `target-${index}`);
    let targetAggregateCalls = 0;
    const db = {
      collection(name: string) {
        if (name === info.junctionName) {
          return {
            find() {
              return {
                async toArray() {
                  return targetIds.map((targetId) => ({
                    [info.selfColumn]: 'parent-1',
                    [info.otherColumn]: targetId,
                  }));
                },
              };
            },
          };
        }
        return {
          aggregate() {
            targetAggregateCalls += 1;
            return { async toArray() { return []; } };
          },
        };
      },
    } as any;
    const adapter = new MongoBatchAdapter(db, metadata());

    await adapter.fetchM2M(
      [{ _id: 'parent-1' }],
      {
        relationName: 'targets',
        type: 'many-to-many',
        targetTable: 'targets',
        fields: ['_id'],
        userSort: 'team._id',
      },
      { name: 'parents', columns: [], relations: [] },
      { name: 'targets', columns: [{ name: '_id' }], relations: [] },
      { projection: { _id: 1 } },
    );

    expect(targetAggregateCalls).toBe(2);
  });

  it('does not fabricate unbounded PK-only M2M targets from dangling edges', async () => {
    const info = resolveMongoJunctionInfo('parents', {
      type: 'many-to-many',
      propertyName: 'targets',
      targetTable: 'targets',
    })!;
    const db = {
      collection(name: string) {
        if (name === info.junctionName) {
          return {
            aggregate() {
              return {
                async toArray() {
                  return [];
                },
              };
            },
          };
        }
        return {
          find() {
            return { async toArray() { return []; } };
          },
        };
      },
    } as any;
    const adapter = new MongoBatchAdapter(db, metadata());

    const result = await adapter.fetchM2M(
      [{ _id: 'parent-1' }],
      {
        relationName: 'targets',
        type: 'many-to-many',
        targetTable: 'targets',
        fields: ['_id'],
      },
      { name: 'parents', columns: [], relations: [] },
      { name: 'targets', columns: [{ name: '_id' }], relations: [] },
      { projection: { _id: 1 } },
    );

    expect(result.docs).toEqual([]);
    expect(result.grouped.get(adapter.keyOf('parent-1'))).toEqual([]);
  });
});
