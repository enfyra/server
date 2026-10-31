import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import type { Cradle } from '../../../src/container';
import type { TDynamicContext } from '../../../src/shared/types';

export async function verifyTemporalRuntime(c: Cradle): Promise<string[]> {
  assert.equal(process.env.NODE_ENV, 'test');
  const uri = new URL(process.env.DB_URI!);
  assert.ok(['127.0.0.1', 'localhost'].includes(uri.hostname));
  assert.match(uri.pathname.slice(1), /^enfyra_fresh_matrix_[a-f0-9]{32}$/);
  const query = c.queryBuilderService;
  const pk = query.getPkField();
  const textType = query.isMongoDb() ? 'string' : 'varchar';
  const context = {
    $user: { isRootAdmin: true, roles: [] },
    $query: {},
    $body: {},
  } as TDynamicContext;
  const suffix = randomUUID().replaceAll('-', '').slice(0, 10);
  const parent = `temporal_groups_${suffix}`;
  const events = `temporal_events_${suffix}`;
  const tableRepo = c.dynamicRepositoryFactory.create('enfyra_table', context);
  const primary = {
    name: pk,
    type: query.isMongoDb() ? 'objectId' : 'int',
    isPrimary: true,
    isGenerated: true,
    isNullable: false,
  };
  const parentResult = await tableRepo.create({
    data: {
      name: parent,
      validateBody: true,
      columns: [primary, { name: 'label', type: textType, isNullable: false }],
    },
  });
  const parentId = parentResult.data?.[0]?.[pk];
  assert.ok(parentId, 'Temporal parent schema did not return an identity');
  await tableRepo.create({
    data: {
      name: events,
      validateBody: true,
      columns: [
        primary,
        { name: 'label', type: textType, isNullable: false },
        { name: 'day', type: 'date', isNullable: false },
        { name: 'occurredAt', type: 'datetime', isNullable: false },
        { name: 'amount', type: 'int', isNullable: false },
      ],
      relations: [
        {
          propertyName: 'group',
          type: 'many-to-one',
          targetTable: { [pk]: parentId },
          inversePropertyName: 'events',
          isNullable: true,
        },
      ],
    },
  });
  if (query.isMongoDb()) {
    assert.ok(
      c.runtimeRegistryService
        .requireMetadata()
        .tables.get(parent)
        ?.relations?.some(
          (relation: { propertyName: string }) =>
            relation.propertyName === 'events',
        ),
      'Mongo inverse relation must activate with its owning table creation',
    );
  }
  const repo = c.dynamicRepositoryFactory.create(events, context);
  const groups = c.dynamicRepositoryFactory.create(parent, context);
  const group = (await groups.create({ data: { label: 'calendar-group' } }))
    .data[0];
  const start = '2026-10-26T17:00:00.000Z';
  const end = '2026-10-27T17:00:00.000Z';
  const valid = {
    label: 'start',
    day: '2026-10-27',
    occurredAt: start,
    amount: 2,
    group: group[pk],
  };
  await c.mutationBodyValidationService.validate({
    tableName: events,
    mode: 'create',
    body: valid,
    user: context.$user,
  });
  const first = (await repo.create({ data: valid })).data[0];
  assert.ok(first[pk]);
  const normalizeInstant = (value: string | Date) =>
    new Date(value).toISOString();
  const single = (
    await repo.find({
      filter: { [pk]: { _eq: first[pk] } },
      fields: [pk, 'day', 'occurredAt'],
    })
  ).data[0];
  assert.equal(single.day, '2026-10-27');
  assert.equal(normalizeInstant(single.occurredAt), start);

  const rows = [
    {
      label: 'before',
      day: '2026-10-26',
      occurredAt: '2026-10-26T16:59:59.999Z',
      amount: 1,
      group: group[pk],
    },
    {
      label: 'middle',
      day: '2026-10-27',
      occurredAt: '2026-10-27T06:00:00.123Z',
      amount: 3,
      group: group[pk],
    },
    {
      label: 'last',
      day: '2026-10-27',
      occurredAt: '2026-10-27T16:59:59.999Z',
      amount: 4,
      group: group[pk],
    },
    {
      label: 'end',
      day: '2026-10-28',
      occurredAt: end,
      amount: 5,
      group: group[pk],
    },
  ];
  await repo.createMany({ data: rows });
  const labels = async (filter: Record<string, unknown>) => {
    const result = await repo.find({ filter, fields: ['label'], limit: -1 });
    return result.data.map((row: { label: string }) => row.label).sort();
  };
  const inside = ['last', 'middle', 'start'];
  assert.deepEqual(
    await labels({ occurredAt: { _gte: start, _lt: end } }),
    inside,
  );
  assert.deepEqual(await labels({ occurredAt: { _gte: end } }), ['end']);
  assert.deepEqual(await labels({ occurredAt: { _lt: start } }), ['before']);
  assert.deepEqual(await labels({ occurredAt: { _between: [start, end] } }), [
    'end',
    ...inside,
  ]);
  assert.deepEqual(await labels({ occurredAt: { _in: [start, end] } }), [
    'end',
    'start',
  ]);
  assert.deepEqual(
    await labels({
      _and: [{ occurredAt: { _gte: start } }, { occurredAt: { _lt: end } }],
    }),
    inside,
  );
  assert.deepEqual(
    await labels({
      _or: [{ occurredAt: { _lt: start } }, { occurredAt: { _gte: end } }],
    }),
    ['before', 'end'],
  );
  assert.deepEqual(await labels({ day: { _eq: '2026-10-27' } }), inside);
  assert.deepEqual(
    await labels({
      group: { label: { _eq: 'calendar-group' } },
      occurredAt: { _gte: start, _lt: end },
    }),
    inside,
  );
  const deep = await groups.find({
    filter: { [pk]: { _eq: group[pk] } },
    fields: [pk],
    deep: {
      events: {
        fields: ['label', 'day', 'occurredAt'],
        filter: { occurredAt: { _gte: start, _lt: end } },
        limit: -1,
      },
    },
  });
  assert.deepEqual(
    deep.data[0].events.map((row: { label: string }) => row.label).sort(),
    inside,
  );
  assert.ok(
    deep.data[0].events.every(
      (row: { day: string }) => row.day === '2026-10-27',
    ),
  );

  for (const patch of [
    { day: '0000-01-01' },
    { day: '2024-02-30' },
    { day: start },
    { occurredAt: '0000-01-01T00:00:00Z' },
    { occurredAt: '2026-10-27' },
    { occurredAt: '2026-10-27T00:00:00+07:00' },
    { occurredAt: '2026-10-27T00:00:00.123456Z' },
    { occurredAt: '2026-02-30T00:00:00Z' },
  ]) {
    await assert.rejects(
      c.mutationBodyValidationService.validate({
        tableName: events,
        mode: 'create',
        body: { ...valid, ...patch },
        user: context.$user,
      }),
      { statusCode: 400 },
    );
    await assert.rejects(repo.create({ data: { ...valid, ...patch } }));
    const field = Object.keys(patch)[0];
    await assert.rejects(
      repo.find({
        filter: { [field]: { _eq: patch[field as keyof typeof patch] } },
      }),
    );
  }
  assert.equal((await repo.find({ fields: [pk], limit: -1 })).data.length, 5);
  await assert.rejects(
    repo.createMany({
      data: [
        { ...valid, label: 'bulk-valid' },
        { ...valid, day: '2024-02-30' },
      ],
    }),
  );
  assert.equal((await repo.find({ fields: [pk], limit: -1 })).data.length, 5);

  const aggregate = await repo.aggregate({
    filter: { occurredAt: { _gte: start, _lt: end } },
    measures: { total: { sum: 'amount' }, entries: { count: pk } },
  });
  assert.equal(Number(aggregate.data[0].total), 9);
  assert.equal(Number(aggregate.data[0].entries), 3);
  const calendarBuckets = await repo.aggregate({
    dimensions: [{ field: 'day', bucket: 'day', timezone: 'America/New_York' }],
    measures: { entries: { count: pk } },
  });
  assert.equal(calendarBuckets.data.length, 3);
  assert.deepEqual(
    calendarBuckets.data.map((row: { day: string }) => row.day).sort(),
    ['2026-10-26', '2026-10-27', '2026-10-28'],
  );
  const utcBuckets = await repo.aggregate({
    dimensions: [{ field: 'occurredAt', bucket: 'day', timezone: 'UTC' }],
    measures: { entries: { count: pk } },
  });
  assert.deepEqual(
    utcBuckets.data
      .map((row: { occurredAt: string | Date }) =>
        normalizeInstant(row.occurredAt),
      )
      .sort(),
    ['2026-10-26T00:00:00.000Z', '2026-10-27T00:00:00.000Z'],
  );
  for (const [timezone, expected] of [
    [
      'Asia/Ho_Chi_Minh',
      [
        '2026-10-25T17:00:00.000Z',
        '2026-10-26T17:00:00.000Z',
        '2026-10-27T17:00:00.000Z',
      ],
    ],
    [
      'America/New_York',
      ['2026-10-26T04:00:00.000Z', '2026-10-27T04:00:00.000Z'],
    ],
  ] as const) {
    const buckets = await repo.aggregate({
      dimensions: [{ field: 'occurredAt', bucket: 'day', timezone }],
      measures: { entries: { count: pk } },
    });
    assert.deepEqual(
      buckets.data
        .map((row: { occurredAt: string | Date }) =>
          normalizeInstant(row.occurredAt),
        )
        .sort(),
      expected,
    );
  }
  const instant = '2040-05-20T12:34:56.789Z';
  await repo.update({
    id: first[pk],
    data: { day: '2024-02-29', occurredAt: instant },
  });
  const updated = (
    await repo.find({
      filter: { [pk]: { _eq: first[pk] } },
      fields: [pk, 'day', 'occurredAt'],
    })
  ).data[0];
  assert.equal(updated.day, '2024-02-29');
  assert.equal(normalizeInstant(updated.occurredAt), instant);
  await repo.create({
    batch: true,
    data: { ...valid, label: 'queued', occurredAt: instant },
  });
  await query.flushBatchInserts();
  const queued = (
    await repo.find({
      filter: { label: { _eq: 'queued' } },
      fields: [pk, 'day', 'occurredAt'],
    })
  ).data[0];
  assert.equal(queued.day, valid.day);
  assert.equal(normalizeInstant(queued.occurredAt), instant);
  await repo.delete({ id: queued[pk] });
  assert.deepEqual(await labels({ label: { _eq: 'queued' } }), []);
  const dstRows = [
    ['spring-before', '2026-03-08', '2026-03-08T06:59:59.999Z'],
    ['spring-after', '2026-03-08', '2026-03-08T07:00:00.000Z'],
    ['fall-before', '2026-11-01', '2026-11-01T05:30:00.000Z'],
    ['fall-after', '2026-11-01', '2026-11-01T06:30:00.000Z'],
  ];
  await repo.createMany({
    data: dstRows.map(([label, day, occurredAt]) => ({
      label,
      day,
      occurredAt,
      amount: 1,
    })),
  });
  const dstBuckets = await repo.aggregate({
    filter: { label: { _in: dstRows.map(([label]) => label) } },
    dimensions: [
      { field: 'occurredAt', bucket: 'day', timezone: 'America/New_York' },
    ],
    measures: { entries: { count: pk } },
  });
  assert.deepEqual(
    dstBuckets.data
      .map((row: { occurredAt: string | Date; entries: number }) => [
        normalizeInstant(row.occurredAt),
        Number(row.entries),
      ])
      .sort(),
    [
      ['2026-03-08T05:00:00.000Z', 2],
      ['2026-11-01T04:00:00.000Z', 2],
    ],
  );
  return [
    'schema-create-and-activation',
    'single-and-bulk-CRUD',
    'calendar-and-millisecond-roundtrip',
    'UTC-range-between-in-and-or',
    'relation-and-deep-filter',
    'invalid-input-and-bulk-no-partial-write',
    'aggregate-and-calendar-buckets',
    'after-2038-and-buffered-write',
  ];
}
