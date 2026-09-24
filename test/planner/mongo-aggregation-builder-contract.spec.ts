import { describe, expect, it } from 'vitest';
import {
  QueryPlanner,
  buildMongoSortSpec,
  buildMongoSortSpecFromPlan,
  checkIfFilterContainsIsNull,
  executeAggregationPipeline,
} from '@enfyra/kernel';

function runtimeMetadata() {
  return {
    tables: new Map([
      [
        'posts',
        {
          columns: [{ name: '_id', isPrimary: true }, { name: 'title' }],
          relations: [
            {
              propertyName: 'author',
              type: 'many-to-one',
              targetTableName: 'users',
              foreignKeyColumn: 'authorId',
            },
            {
              propertyName: 'comments',
              type: 'one-to-many',
              targetTableName: 'comments',
              mappedBy: 'postId',
              foreignKeyColumn: 'postId',
            },
          ],
        },
      ],
      [
        'users',
        {
          columns: [{ name: '_id', isPrimary: true }, { name: 'name' }],
          relations: [],
        },
      ],
      [
        'comments',
        {
          columns: [
            { name: '_id', isPrimary: true },
            { name: 'postId' },
            { name: 'name' },
          ],
          relations: [],
        },
      ],
    ]),
  };
}

function collection() {
  return {
    aggregate() {
      throw new Error('pipeline-only tests must not execute MongoDB');
    },
  } as any;
}

function db() {
  return {
    collection() {
      return { ...collection(), find: () => ({ toArray: async () => [] }) };
    },
  } as any;
}

function hydrationRuntime(rows: Record<string, any[]> = {}) {
  const pipelines: Array<{ table: string; pipeline: any[] }> = [];
  const finds: Array<{ table: string; filter: any; options: any }> = [];
  const database = {
    collection(table: string) {
      return {
        aggregate(pipeline: any[]) {
          pipelines.push({ table, pipeline });
          return { toArray: async () => structuredClone(rows[table] ?? []) };
        },
        find(filter: any, options: any) {
          finds.push({ table, filter, options });
          return { toArray: async () => structuredClone(rows[table] ?? []) };
        },
      };
    },
  };
  return { db: database as any, collection: database.collection('posts') as any, pipelines, finds };
}

describe('Mongo aggregation builder contracts', () => {
  it('rejects logical nested filters that require unsupported relation lookup semantics', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({
      propertyName: 'author',
      type: 'many-to-one',
      targetTableName: 'users',
      foreignKeyColumn: 'authorId',
    });

    await expect(
      executeAggregationPipeline(
        collection(),
        {
          table: 'posts',
          sort: [{ field: 'comments.name', direction: 'asc' }],
          mongoRawFilter: { comments: { _or: [
            { author: { name: { _eq: 'Ada' } } },
            { name: { _eq: 'fallback' } },
          ] } },
          mongoFieldsExpanded: {
            scalarFields: ['_id'],
            relations: [
              {
                propertyName: 'comments',
                targetTable: 'comments',
                localField: '_id',
                foreignField: 'postId',
                type: 'many',
                nestedFields: ['name'],
              },
            ],
          },
        } as any,
        {
          db: db(),
          metadata,
          dbType: 'mongodb',
          execute: false,
        },
      ),
    ).rejects.toThrow(/logical relation filter is not supported/i);
  });

  it('requires a filtered nested to-many relation to contain a match', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({
      propertyName: 'replies',
      type: 'one-to-many',
      targetTableName: 'replies',
      foreignKeyColumn: 'commentId',
    });
    metadata.tables.set('replies', {
      columns: [
        { name: '_id', isPrimary: true },
        { name: 'commentId' },
        { name: 'status' },
      ],
      relations: [],
    });

    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [{ field: 'comments.name', direction: 'asc' }],
        mongoRawFilter: { comments: { replies: { status: { _eq: 'open' } } } },
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'comments',
              targetTable: 'comments',
              localField: '_id',
              foreignField: 'postId',
              type: 'many',
              nestedFields: ['name'],
            },
          ],
        },
      } as any,
      { db: db(), metadata, dbType: 'mongodb', execute: false },
    );

    const sortLookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(sortLookup.pipeline).toContainEqual({
      $match: { 'replies.0': { $exists: true } },
    });
    const projection = sortLookup.pipeline.find((stage: any) => stage.$project);
    expect(projection.$project).not.toHaveProperty('replies');
  });

  it('does not expose unrequested owner identities in explicit projections', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        mongoFieldsExpanded: { scalarFields: ['title'], relations: [] },
      } as any,
      { db: db(), metadata: runtimeMetadata(), dbType: 'mongodb', execute: false },
    );

    const projection = result.pipeline.find((stage) => stage.$project)?.$project;
    expect(projection).toEqual({ _id: 1, title: 1 });
  });

  it('joins filter-only inverse one-to-one relations in the inverse direction', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({
      propertyName: 'detail',
      type: 'one-to-one',
      isInverse: true,
      targetTableName: 'comment_details',
      foreignKeyColumn: 'commentId',
    });
    metadata.tables.set('comment_details', {
      columns: [
        { name: '_id', isPrimary: true },
        { name: 'commentId' },
        { name: 'active' },
      ],
      relations: [],
    });

    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [{ field: 'comments.name', direction: 'asc' }],
        mongoRawFilter: { comments: { detail: { active: { _eq: true } } } },
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'comments',
              targetTable: 'comments',
              localField: '_id',
              foreignField: 'postId',
              type: 'many',
              nestedFields: ['name'],
            },
          ],
        },
      } as any,
      { db: db(), metadata, dbType: 'mongodb', execute: false },
    );

    const commentsLookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    const detailLookup = commentsLookup.pipeline.find(
      (stage: any) => stage.$lookup?.as === 'detail',
    ).$lookup;
    expect(detailLookup.let).toEqual({ localId: '$_id' });
    expect(detailLookup.pipeline[0].$match.$expr.$and).toContainEqual({ $eq: ['$commentId', '$$localId'] });
  });

  it('joins filter-only many-to-many targets through their junction', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({
      propertyName: 'tags', type: 'many-to-many', targetTableName: 'tags',
      junctionTableName: 'comment_tags', junctionSourceColumn: 'commentId', junctionTargetColumn: 'tagId',
    });
    metadata.tables.set('tags', { columns: [{ name: '_id', isPrimary: true }, { name: 'name' }], relations: [] });
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts', sort: [{ field: 'comments.name', direction: 'asc' }],
      mongoRawFilter: { comments: { tags: { name: { _eq: 'typescript' } } } },
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: ['name'] }] },
    } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    const lookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(lookup.pipeline.find((stage: any) => stage.$lookup?.as === 'tags').$lookup.from).toBe('comment_tags');
    expect(lookup.pipeline).toContainEqual({ $match: { 'tags.0': { $exists: true } } });
  });
  it.each([
    { name: { _is_null: true } },
    { _not: { name: { _eq: 'blocked' } } },
    { _and: [{ name: { _is_null: true } }, { name: { _neq: 'blocked' } }] },
  ])('requires a linked child satisfying nested target predicates %#', async (filter) => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({
      propertyName: 'author', type: 'many-to-one', targetTableName: 'users', foreignKeyColumn: 'authorId',
    });
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts',
      sort: [{ field: 'comments.name', direction: 'asc' }],
      mongoRawFilter: { comments: { author: filter } },
      mongoFieldsExpanded: {
        scalarFields: ['_id'],
        relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: ['name'] }],
      },
    } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });

    const lookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(lookup.pipeline).toContainEqual({ $match: { author: { $ne: null } } });
  });

  it.each([false, true])('rejects invalid deep pagination before root IO (execute=%s)', async (execute) => {
    const aggregate = vi.fn(() => ({ toArray: async () => [] }));
    await expect(executeAggregationPipeline({ aggregate } as any, {
      table: 'posts',
      deep: { comments: { limit: -1 } },
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{
        propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: ['name'],
      }] },
    } as any, { db: db(), metadata: runtimeMetadata(), dbType: 'mongodb', execute })).rejects.toThrow(/limit/i);
    expect(aggregate).not.toHaveBeenCalled();
  });

  it('does not extract a partial relation predicate from mixed NOT', async () => {
    const metadata = runtimeMetadata();
    const targetDb = { collection: () => ({ find: () => ({ toArray: async () => [] }) }) } as any;
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts',
      mongoRawFilter: { _not: { _and: [
        { title: { _eq: 'archived' } },
        { author: { name: { _eq: 'Ada' } } },
      ] } },
      sort: [{ field: 'author.name', direction: 'asc' }],
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{
        propertyName: 'author', targetTable: 'users', localField: 'authorId', foreignField: '_id', type: 'one', nestedFields: ['name'],
      }] },
    } as any, { db: targetDb, metadata, dbType: 'mongodb', execute: false });

    const lookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(lookup.pipeline.filter((stage: any) => stage.$match && !stage.$match.$expr)).toEqual([]);
  });

  it('applies nested deep filters and pagination through batch hydration', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({ propertyName: 'replies', type: 'one-to-many', targetTableName: 'replies', foreignKeyColumn: 'commentId' });
    metadata.tables.set('replies', { columns: [{ name: '_id', isPrimary: true }, { name: 'commentId' }, { name: 'status' }], relations: [] });
    const runtime = hydrationRuntime({ posts: [{ _id: 'p' }], comments: [{ _id: 'c', postId: 'p', name: 'x' }] });
    const result = await executeAggregationPipeline(runtime.collection, {
      table: 'posts', sort: [{ field: 'comments.name', direction: 'asc' }],
      deep: { comments: { fields: ['name'], deep: { replies: { fields: ['_id'], filter: { status: { _eq: 'open' } }, sort: '-status', limit: 2, page: 2 } } } },
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: [] }] },
    } as any, { db: runtime.db, metadata, dbType: 'mongodb' });
    const child = runtime.pipelines.find((item) => item.table === 'replies')!.pipeline;
    expect(child[0]).toEqual({ $match: { $and: [{ commentId: { $in: ['c'] } }, { status: 'open' }] } });
    expect(child.find((stage) => stage.$group).$group.__enfyra_top_n_documents__.$topN).toEqual(expect.objectContaining({ n: 4, sortBy: { status: -1, _id: 1 } }));
    expect(result.results[0].comments[0].replies).toEqual([]);
  });

  it('supports aggregate relation sorts without response relation expansion', async () => {
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts',
      plan: {
        hasRelationSort: true,
        sortItems: [{ joinId: null, field: '_count(comments)', fullPath: '_count(comments)', direction: 'desc', function: 'count', relationName: 'comments', relationMeta: runtimeMetadata().tables.get('posts').relations[1] }],
      },
    } as any, { db: db(), metadata: runtimeMetadata(), dbType: 'mongodb', execute: false });

    expect(result.pipeline).toContainEqual({ $sort: { __sort_count_comments: -1 } });
  });

  it('projects all requested metadata scalars explicitly instead of leaking undeclared fields', async () => {
    const metadata = runtimeMetadata();
    const result = await executeAggregationPipeline(collection(), {
      table: 'users', mongoFieldsExpanded: { scalarFields: ['_id', 'name'], relations: [] },
    } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    expect(result.pipeline.at(-1)).toEqual({ $project: { _id: 1, name: 1 } });
  });

  it('never correlates null or missing keys to a null Mongo target identity', async () => {
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts', sort: [{ field: 'author.name', direction: 'asc' }],
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'author', targetTable: 'users', localField: 'authorId', foreignField: '_id', type: 'one', nestedFields: ['_id'] }] },
    } as any, { db: db(), metadata: runtimeMetadata(), dbType: 'mongodb', execute: false });
    const lookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(lookup.pipeline[0].$match.$expr.$and).toEqual([
      { $ne: [{ $ifNull: ['$$localId', null] }, null] },
      { $ne: [{ $ifNull: ['$_id', null] }, null] },
      { $eq: ['$_id', '$$localId'] },
    ]);
  });

  it.each([undefined, { scalarFields: ['_id'], relations: [] }])('rejects raw relation sorts without their expansion %#', async (mongoFieldsExpanded) => {
    await expect(executeAggregationPipeline(collection(), {
      table: 'posts', sort: [{ field: 'author.name', direction: 'asc' }], mongoFieldsExpanded,
    } as any, { db: db(), metadata: runtimeMetadata(), dbType: 'mongodb', execute: false })).rejects.toThrow(/expanded relation|sort expansion/i);
  });

  it('treats nested _id as a target field rather than an operator', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({ propertyName: 'author', type: 'many-to-one', targetTableName: 'users', foreignKeyColumn: 'authorId' });
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts', sort: [{ field: 'comments.name', direction: 'asc' }],
      mongoRawFilter: { comments: { author: { _id: { _eq: 'u' } } } },
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: ['name'] }] },
    } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    const parent = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    const author = parent.pipeline.find((stage: any) => stage.$lookup?.as === 'author')?.$lookup;
    expect(author).toBeDefined();
    expect(author.pipeline).toContainEqual({ $match: { _id: 'u' } });
    expect(parent.pipeline).toContainEqual({ $match: { author: { $ne: null } } });
  });

  it('preserves id members inside scalar embedded objects', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('posts').columns.push({ name: 'address' });
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts', sort: [{ field: 'address.id', direction: 'asc' }],
      mongoFieldsExpanded: { scalarFields: ['address'], relations: [] },
    } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    expect(result.pipeline).toContainEqual({ $sort: { 'address.id': 1 } });
  });

  it('keeps embedded-document sorts distinct from relation sorts at the low-level boundary', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('posts').columns.push({ name: 'address' });
    const result = await executeAggregationPipeline(collection(), {
      table: 'posts', sort: [{ field: 'address.city', direction: 'asc' }],
      mongoFieldsExpanded: { scalarFields: ['address'], relations: [] },
    } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    expect(result.pipeline).toContainEqual({ $sort: { 'address.city': 1 } });
    expect(result.pipeline.some((stage) => stage.$lookup)).toBe(false);
  });

  it('uses a real target id column for planned relation aggregate sorting', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').columns.push({ name: 'id' });
    const plan = new QueryPlanner().plan({ tableName: 'posts', fields: ['_id'], sort: '_max(comments.id)', metadata, dbType: 'mongodb' });
    const result = await executeAggregationPipeline(collection(), { table: 'posts', plan } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    const lookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(lookup.pipeline.find((stage: any) => stage.$group).$group.value).toEqual({ $max: '$id' });
  });

  it('preserves a physical id in low-level select projection', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('posts').columns.push({ name: 'id' });
    const result = await executeAggregationPipeline(collection(), { table: 'posts', select: ['id'] } as any, { db: db(), metadata, dbType: 'mongodb', execute: false });
    expect(result.pipeline.at(-1)).toEqual({ $project: { _id: 0, id: 1 } });
  });

  it.each([undefined, { scalarFields: ['_id'], relations: [] }])('executes low-level zero-limit queries without a limit stage %#', async (mongoFieldsExpanded) => {
    const toArray = vi.fn(async () => [{ _id: 'post' }]);
    const aggregate = vi.fn(() => ({ toArray }));
    const result = await executeAggregationPipeline({ aggregate } as any, { table: 'posts', limit: 0, mongoFieldsExpanded } as any, { db: db(), metadata: runtimeMetadata(), dbType: 'mongodb' });
    expect(result.results).toEqual([{ _id: 'post' }]);
    expect(aggregate).toHaveBeenCalledOnce();
    expect(aggregate.mock.calls[0][0]).not.toEqual(expect.arrayContaining([expect.objectContaining({ $limit: expect.anything() })]));
  });

  it('keeps count-only pipelines free of sorting and model projections', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        mongoCountOnly: true,
        select: ['title'],
        sort: [{ field: 'title', direction: 'asc' }],
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    expect(result.pipeline).toEqual([{ $count: 'count' }]);
  });

  it('excludes implicit Mongo _id when it was not selected', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      { table: 'posts', select: ['title'] } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    expect(result.pipeline).toContainEqual({
      $project: { _id: 0, title: 1 },
    });
  });

  it('does not emit empty sort stages for empty plans', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        plan: {
          hasRelationFilters: false,
          hasRelationSort: false,
          sortItems: [],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    expect(result.pipeline).not.toContainEqual({ $sort: {} });
  });

  it('does not materialize sorted relations for count-only queries', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        mongoCountOnly: true,
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'author',
              targetTable: 'users',
              localField: 'authorId',
              foreignField: '_id',
              type: 'one',
              nestedFields: ['name'],
            },
          ],
        },
        plan: {
          hasRelationFilters: false,
          hasRelationSort: true,
          sortItems: [
            {
              joinId: 'author',
              field: 'name',
              fullPath: 'author.name',
              direction: 'asc',
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    expect(result.pipeline.some((stage) => stage.$lookup)).toBe(false);
  });

  it('rejects relation sorts when relation expansion is unavailable', async () => {
    await expect(
      executeAggregationPipeline(
        collection(),
        {
          table: 'posts',
          plan: {
            hasRelationFilters: false,
            hasRelationSort: true,
            sortItems: [
              {
                joinId: 'author',
                field: 'name',
                fullPath: 'author.name',
                direction: 'asc',
              },
            ],
          },
        } as any,
        {
          db: db(),
          metadata: runtimeMetadata(),
          dbType: 'mongodb',
          execute: false,
        },
      ),
    ).rejects.toThrow('MongoDB relation sort requires expanded relation fields');
  });

  it('uses planned relation sorts to materialize the sorted relation', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'author',
              targetTable: 'users',
              localField: 'authorId',
              foreignField: '_id',
              type: 'one',
              nestedFields: ['name'],
            },
          ],
        },
        plan: {
          hasRelationFilters: false,
          hasRelationSort: true,
          sortItems: [
            {
              joinId: 'author',
              field: 'name',
              fullPath: 'author.name',
              direction: 'asc',
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    expect(result.pipeline).toContainEqual({
      $lookup: expect.objectContaining({
        from: 'users',
        as: '__enfyra_relation_sort_0',
      }),
    });
    expect(result.pipeline).toContainEqual({ $sort: { '__enfyra_relation_sort_0.name': 1 } });
  });

  it('uses the junction collection for sorted many-to-many relations', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('posts').relations.push({
      propertyName: 'tags',
      type: 'many-to-many',
      targetTableName: 'tags',
      junctionTableName: 'posts_tags',
      junctionSourceColumn: 'postId',
      junctionTargetColumn: 'tagId',
    });
    metadata.tables.set('tags', {
      columns: [
        { name: '_id', isPrimary: true },
        { name: 'name' },
      ],
      relations: [],
    });

    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [{ field: 'tags.name', direction: 'asc' }],
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'tags',
              targetTable: 'tags',
              localField: '_id',
              foreignField: '_id',
              type: 'many',
              nestedFields: ['name'],
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata,
        dbType: 'mongodb',
        execute: false,
      },
    );

    const lookups = result.pipeline
      .filter((stage) => stage.$lookup)
      .map((stage) => stage.$lookup);
    expect(lookups[0].from).toBe('posts_tags');
    expect(lookups[0].pipeline).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          $lookup: expect.objectContaining({
            from: 'tags',
            let: { localId: '$tagId' },
          }),
        }),
      ]),
    );
    expect(lookups[lookups.length - 1].from).toBe('posts_tags');
  });

  it('hydrates sorted relations through the same batch filter, projection and pagination path', async () => {
    const runtime = hydrationRuntime({ posts: [{ _id: 'p' }] });
    await executeAggregationPipeline(runtime.collection, {
      table: 'posts', sort: [{ field: 'comments.name', direction: 'asc' }],
      deep: { comments: { fields: ['_id'], filter: { name: { _eq: 'alice' } }, sort: '-name', page: 2, limit: 3 } },
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: ['name'] }] },
    } as any, { db: runtime.db, metadata: runtimeMetadata(), dbType: 'mongodb' });
    const root = runtime.pipelines.find((item) => item.table === 'posts')!.pipeline;
    const child = runtime.pipelines.find((item) => item.table === 'comments')!.pipeline;
    expect(root.filter((stage) => stage.$lookup)).toHaveLength(1);
    expect(root.find((stage) => stage.$lookup).$lookup.pipeline).not.toContainEqual({ $match: { name: 'alice' } });
    expect(child[0]).toEqual({ $match: { $and: [{ postId: { $in: ['p'] } }, { name: 'alice' }] } });
    const topN = child.find((stage) => stage.$group).$group.__enfyra_top_n_documents__.$topN;
    expect(topN.sortBy).toEqual({ name: -1, _id: 1 });
    expect(topN.n).toBe(6);
    expect(child.at(-1).$project.name).toBeUndefined();
  });

  it('applies default deep limits after sorting parent rows', async () => {
    const runtime = hydrationRuntime({ posts: [{ _id: 'p' }] });
    await executeAggregationPipeline(runtime.collection, {
      table: 'posts', sort: [{ field: 'comments.name', direction: 'asc' }],
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: [] }] },
    } as any, { db: runtime.db, metadata: runtimeMetadata(), dbType: 'mongodb' });
    const child = runtime.pipelines.find((item) => item.table === 'comments')!.pipeline;
    expect(child.find((stage) => stage.$group).$group.__enfyra_top_n_documents__.$topN.n).toBe(10);
  });

  it('does not expose a to-one sort field or replace its physical relation key', async () => {
    const runtime = hydrationRuntime({ posts: [{ _id: 'p', authorId: 'u' }], users: [{ _id: 'u' }] });
    const result = await executeAggregationPipeline(runtime.collection, {
      table: 'posts', sort: [{ field: 'author.name', direction: 'asc' }],
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'author', targetTable: 'users', localField: 'authorId', foreignField: '_id', type: 'one', nestedFields: ['_id'] }] },
    } as any, { db: runtime.db, metadata: runtimeMetadata(), dbType: 'mongodb' });
    expect(runtime.finds[0].options.projection).toEqual({ _id: 1 });
    expect(result.results).toEqual([{ _id: 'p', author: { _id: 'u' } }]);
  });

  it('keeps root relation predicates out of batch response hydration', async () => {
    const runtime = hydrationRuntime({ posts: [{ _id: 'p' }] });
    await executeAggregationPipeline(runtime.collection, {
      table: 'posts', sort: [{ field: 'comments.name', direction: 'asc' }],
      mongoRawFilter: { comments: { name: { _eq: 'root-only' } } },
      deep: { comments: { filter: { name: { _eq: 'response-only' } } } },
      mongoFieldsExpanded: { scalarFields: ['_id'], relations: [{ propertyName: 'comments', targetTable: 'comments', localField: '_id', foreignField: 'postId', type: 'many', nestedFields: ['name'] }] },
    } as any, { db: runtime.db, metadata: runtimeMetadata(), dbType: 'mongodb' });
    const child = runtime.pipelines.find((item) => item.table === 'comments')!.pipeline;
    expect(child[0]).toEqual({ $match: { $and: [{ postId: { $in: ['p'] } }, { name: 'response-only' }] } });
  });

  it('does not turn a deep relation filter into a root cardinality filter', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [{ field: 'author.name', direction: 'asc' }],
        deep: { author: { filter: { name: { _eq: 'alice' } } } },
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'author',
              targetTable: 'users',
              localField: 'authorId',
              foreignField: '_id',
              type: 'one',
              nestedFields: ['name'],
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    expect(result.pipeline).not.toContainEqual({
      $match: { author: { $ne: null } },
    });
  });

  it('records debug pipelines without JSON-serializing BSON-compatible values', async () => {
    const debugLog: any[] = [];
    const value = 9_007_199_254_740_993n;
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        where: [{ field: 'title', operator: '=', value }],
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        debugLog,
        execute: false,
      },
    );

    expect(result.pipeline[0].$match.title).toBe(value);
    expect(debugLog[0].pipeline[0].$match.title).toBe(value);
  });

  it('maps logical ids in dotted MongoDB sort paths', () => {
    expect(
      buildMongoSortSpec([{ field: 'author.id', direction: 'asc' }]),
    ).toEqual({ 'author._id': 1 });
    expect(
      buildMongoSortSpecFromPlan([
        {
          joinId: 'author',
          field: '_id',
          fullPath: 'author._id',
          direction: 'desc',
        },
      ]),
    ).toEqual({ 'author._id': -1 });
  });

  it('preserves unselected relation sort fields until lookup ordering completes', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [{ field: 'comments.name', direction: 'desc' }],
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'comments',
              targetTable: 'comments',
              localField: '_id',
              foreignField: 'postId',
              type: 'many',
              nestedFields: ['_id'],
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    const sortLookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(sortLookup.pipeline).toContainEqual({
      $project: expect.objectContaining({ name: 1 }),
    });
    expect(sortLookup.pipeline).toContainEqual({
      $sort: { name: -1, _id: 1 },
    });
  });

  it('uses every same-relation sort key when selecting a to-many child', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [
          { field: 'comments.name', direction: 'asc' },
          { field: 'comments.postId', direction: 'desc' },
        ],
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'comments',
              targetTable: 'comments',
              localField: '_id',
              foreignField: 'postId',
              type: 'many',
              nestedFields: ['name', 'postId'],
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    const sortLookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(sortLookup.pipeline).toContainEqual({
      $sort: { name: 1, postId: -1, _id: 1 },
    });
  });

  it('maps nested logical ids inside relation lookup sorts', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('comments').relations.push({ propertyName: 'author', type: 'many-to-one', targetTableName: 'users', foreignKeyColumn: 'authorId' });
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        sort: [{ field: 'comments.author.id', direction: 'asc' }],
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'comments',
              targetTable: 'comments',
              localField: '_id',
              foreignField: 'postId',
              type: 'many',
              nestedFields: ['author.id'],
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata,
        dbType: 'mongodb',
        execute: false,
      },
    );

    const sortLookup = result.pipeline.find((stage) => stage.$lookup)?.$lookup;
    expect(sortLookup.pipeline).toContainEqual({
      $sort: { 'author._id': 1, _id: 1 },
    });
  });

  it.each(['id', '_id'])('preserves the canonical planned root field %s', (field) => {
    expect(
      buildMongoSortSpecFromPlan([
        { joinId: null, field, fullPath: field, direction: 'desc' },
      ]),
    ).toEqual({ [field]: -1 });
  });

  it('preserves prototype-like Mongo sort fields as own keys', () => {
    const direct = buildMongoSortSpec([
      { field: '__proto__', direction: 'asc' },
    ]);
    const planned = buildMongoSortSpecFromPlan([
      {
        joinId: null,
        field: '__proto__',
        fullPath: '__proto__',
        direction: 'desc',
      },
    ]);

    expect(Object.prototype.hasOwnProperty.call(direct, '__proto__')).toBe(true);
    expect(direct.__proto__).toBe(1);
    expect(Object.prototype.hasOwnProperty.call(planned, '__proto__')).toBe(true);
    expect(planned.__proto__).toBe(-1);
  });

  it('rejects relation-sort aliases that collide with root fields', async () => {
    const metadata = runtimeMetadata();
    metadata.tables.get('posts').columns.push({
      name: '__sort_count_comments',
    });

    await expect(
      executeAggregationPipeline(
        collection(),
        {
          table: 'posts',
          plan: {
            hasRelationFilters: false,
            hasRelationSort: true,
            sortItems: [
              {
                joinId: 'comments',
                field: 'comments',
                fullPath: 'comments',
                direction: 'asc',
                function: 'count',
                relationName: 'comments',
                relationMeta: {
                  type: 'one-to-many',
                  targetTableName: 'comments',
                  foreignKeyColumn: 'postId',
                },
              },
            ],
          },
          mongoFieldsExpanded: {
            scalarFields: ['_id'],
            relations: [],
          },
        } as any,
        {
          db: db(),
          metadata,
          dbType: 'mongodb',
          execute: false,
        },
      ),
    ).rejects.toThrow('MongoDB relation sort alias conflicts with a root field');
  });

  it('does not pre-hydrate selected relations for aggregate sorts', async () => {
    const result = await executeAggregationPipeline(
      collection(),
      {
        table: 'posts',
        limit: 5,
        plan: {
          hasRelationFilters: false,
          hasRelationSort: true,
          sortItems: [
            {
              joinId: 'comments',
              field: 'comments',
              fullPath: 'comments',
              direction: 'desc',
              function: 'count',
              relationName: 'comments',
              relationMeta: {
                propertyName: 'comments',
                type: 'one-to-many',
                targetTableName: 'comments',
                foreignKeyColumn: 'postId',
              },
            },
          ],
        },
        mongoFieldsExpanded: {
          scalarFields: ['_id'],
          relations: [
            {
              propertyName: 'comments',
              targetTable: 'comments',
              localField: '_id',
              foreignField: 'postId',
              type: 'many',
              nestedFields: ['name'],
            },
          ],
        },
      } as any,
      {
        db: db(),
        metadata: runtimeMetadata(),
        dbType: 'mongodb',
        execute: false,
      },
    );

    const relationLookups = result.pipeline.filter(
      (stage) => stage.$lookup?.as === 'comments',
    );
    expect(relationLookups).toEqual([]);
    expect(result.pipeline).toContainEqual({ $limit: 5 });
  });

  it('keeps aggregate-sort aliases distinct for ambiguous relation paths', () => {
    const first = buildMongoSortSpecFromPlan([
      {
        joinId: 'orders_us',
        field: 'total',
        fullPath: 'orders_us.total',
        direction: 'asc',
        function: 'max',
        relationName: 'orders_us',
        aggregateField: 'total',
      },
      {
        joinId: 'orders',
        field: 'us_total',
        fullPath: 'orders.us_total',
        direction: 'desc',
        function: 'max',
        relationName: 'orders',
        aggregateField: 'us_total',
      },
    ]);

    expect(Object.keys(first)).toHaveLength(2);
  });

  it('continues scanning sibling predicates after logical operators', () => {
    expect(
      checkIfFilterContainsIsNull({
        _or: [{ status: { _eq: 'active' } }],
        deletedAt: { _is_null: true },
      }),
    ).toBe(true);

    expect(
      checkIfFilterContainsIsNull({
        _not: { status: { _eq: 'deleted' } },
        deletedAt: { _eq: null },
      }),
    ).toBe(true);
    expect(
      checkIfFilterContainsIsNull({
        _and: [
          { deletedAt: { _is_null: true } },
          { active: { _eq: true } },
        ],
      }),
    ).toBe(true);
    expect(
      checkIfFilterContainsIsNull({
        _not: { deletedAt: { _is_null: true } },
      }),
    ).toBe(true);
    expect(checkIfFilterContainsIsNull(null)).toBe(false);
    expect(
      checkIfFilterContainsIsNull({ field: { $nin: [null] } }),
    ).toBe(false);

    const inherited = Object.create({ _is_null: true });
    expect(checkIfFilterContainsIsNull({ field: inherited })).toBe(false);

    const cyclic: any = {};
    cyclic.self = cyclic;
    expect(checkIfFilterContainsIsNull(cyclic)).toBe(false);
  });
});
