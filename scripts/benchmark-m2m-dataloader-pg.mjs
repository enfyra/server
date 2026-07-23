import knex from 'knex';
import { executeBatchFetches } from '@enfyra/kernel';
import { performance } from 'node:perf_hooks';

const connection =
  process.env.PG_TEST_URI || 'postgresql://root:1234@localhost:5432/postgres';
const postCount = Number(process.env.M2M_BENCH_POSTS || 100_000);
const targetCount = Number(process.env.M2M_BENCH_TARGETS || 2_000_000);
const fanout = Number(process.env.M2M_BENCH_FANOUT || 200);
const sampleParentCount = Number(process.env.M2M_BENCH_SAMPLE_PARENTS || 2_000);
const relationLimit = Number(process.env.M2M_BENCH_LIMIT || 10);
const measuredRuns = Number(process.env.M2M_BENCH_RUNS || 3);
const perParentConcurrency = Number(
  process.env.M2M_BENCH_PER_PARENT_CONCURRENCY || 16,
);
const sharedTopTargets = process.env.M2M_BENCH_SHARED_TOP !== '0';
const keepSchema = process.env.M2M_BENCH_KEEP_SCHEMA === '1';
const schema = `__m2m_bench_${Date.now()}`;
const postsTable = `${schema}.posts`;
const targetsTable = `${schema}.comments`;
const junctionTable = `${schema}.post_comments`;

if (
  !Number.isInteger(postCount) ||
  !Number.isInteger(targetCount) ||
  !Number.isInteger(fanout) ||
  !Number.isInteger(sampleParentCount) ||
  postCount < 1 ||
  targetCount <= 4_000 ||
  fanout < 20 ||
  sampleParentCount < 1 ||
  sampleParentCount > postCount ||
  relationLimit < 1
) {
  throw new Error('Invalid M2M benchmark dimensions');
}

const db = knex({
  client: 'pg',
  connection,
  pool: { min: 0, max: 4 },
});
const quotedSchema = `"${schema}"`;
const quotedPosts = `${quotedSchema}."posts"`;
const quotedTargets = `${quotedSchema}."comments"`;
const quotedJunction = `${quotedSchema}."post_comments"`;
const parentIds = Array.from(
  { length: sampleParentCount },
  (_, index) => index + 1,
);

const metadata = {
  [postsTable]: {
    name: postsTable,
    columns: [{ name: 'id', type: 'integer', isPrimary: true }],
    relations: [
      {
        propertyName: 'comments',
        type: 'many-to-many',
        targetTableName: targetsTable,
        targetTable: targetsTable,
        junctionTableName: junctionTable,
        junctionSourceColumn: 'post_id',
        junctionTargetColumn: 'comment_id',
        isInverse: false,
      },
    ],
  },
  [targetsTable]: {
    name: targetsTable,
    columns: [
      { name: 'id', type: 'integer', isPrimary: true },
      { name: 'score', type: 'integer' },
      { name: 'payload', type: 'text' },
    ],
    relations: [],
  },
};
const metadataGetter = async (table) => metadata[table] || null;

const edgeTopKSql = `
  SELECT source_id, target_id
  FROM (
    SELECT
      junction.post_id AS source_id,
      junction.comment_id AS target_id,
      ROW_NUMBER() OVER (
        PARTITION BY junction.post_id
        ORDER BY target.score DESC, target.id ASC
      ) AS relation_rank
    FROM ${quotedJunction} AS junction
    JOIN ${quotedTargets} AS target
      ON target.id = junction.comment_id
    WHERE junction.post_id = ANY(?::int[])
  ) AS ranked
  WHERE relation_rank <= ?
  ORDER BY source_id, relation_rank
`;

const joinedTopKSql = `
  SELECT source_id, id, score, payload
  FROM (
    SELECT
      junction.post_id AS source_id,
      target.id,
      target.score,
      target.payload,
      ROW_NUMBER() OVER (
        PARTITION BY junction.post_id
        ORDER BY target.score DESC, target.id ASC
      ) AS relation_rank
    FROM ${quotedJunction} AS junction
    JOIN ${quotedTargets} AS target
      ON target.id = junction.comment_id
    WHERE junction.post_id = ANY(?::int[])
  ) AS ranked
  WHERE relation_rank <= ?
  ORDER BY source_id, relation_rank
`;

const perParentSql = `
  SELECT target.id, target.score, target.payload
  FROM ${quotedJunction} AS junction
  JOIN ${quotedTargets} AS target
    ON target.id = junction.comment_id
  WHERE junction.post_id = ?
  ORDER BY target.score DESC, target.id ASC
  LIMIT ?
`;

function median(values) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

function percentile(values, ratio) {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.ceil(sorted.length * ratio) - 1];
}

async function countedRun(run) {
  let queries = 0;
  const countQuery = () => {
    queries += 1;
  };
  db.on('query', countQuery);
  const started = performance.now();
  try {
    const result = await run();
    return {
      ms: performance.now() - started,
      queries,
      ...result,
    };
  } finally {
    db.removeListener('query', countQuery);
  }
}

async function measure(name, run) {
  await run();
  const samples = [];
  for (let index = 0; index < measuredRuns; index += 1) {
    samples.push(await countedRun(run));
  }
  const durations = samples.map((sample) => sample.ms);
  return {
    name,
    medianMs: Number(median(durations).toFixed(2)),
    p95Ms: Number(percentile(durations, 0.95).toFixed(2)),
    minMs: Number(Math.min(...durations).toFixed(2)),
    maxMs: Number(Math.max(...durations).toFixed(2)),
    queries: samples[0].queries,
    relationRows: samples[0].relationRows,
    uniqueTargets: samples[0].uniqueTargets,
  };
}

async function runHybrid() {
  const parents = parentIds.map((id) => ({ id }));
  await executeBatchFetches(
    db,
    parents,
    [
      {
        relationName: 'comments',
        type: 'many-to-many',
        targetTable: targetsTable,
        fields: ['id', 'score', 'payload'],
        junctionTableName: junctionTable,
        junctionSourceColumn: 'post_id',
        junctionTargetColumn: 'comment_id',
        isInverse: false,
        userSort: '-score',
        userLimit: relationLimit,
      },
    ],
    metadataGetter,
    3,
    0,
    postsTable,
    'postgres',
    metadata,
  );
  const relationRows = parents.reduce(
    (total, parent) => total + parent.comments.length,
    0,
  );
  const uniqueTargets = new Set(
    parents.flatMap((parent) => parent.comments.map((comment) => comment.id)),
  ).size;
  return { relationRows, uniqueTargets };
}

async function runJoinedTopK() {
  const result = await db.raw(joinedTopKSql, [parentIds, relationLimit]);
  return {
    relationRows: result.rows.length,
    uniqueTargets: new Set(result.rows.map((row) => row.id)).size,
  };
}

async function runPerParent() {
  const rows = new Array(parentIds.length);
  let cursor = 0;
  async function worker() {
    while (cursor < parentIds.length) {
      const index = cursor;
      cursor += 1;
      const result = await db.raw(perParentSql, [
        parentIds[index],
        relationLimit,
      ]);
      rows[index] = result.rows;
    }
  }
  await Promise.all(
    Array.from(
      {
        length: Math.min(perParentConcurrency, parentIds.length),
      },
      worker,
    ),
  );
  return {
    relationRows: rows.reduce((total, row) => total + row.length, 0),
    uniqueTargets: new Set(
      rows.flatMap((row) => row.map((target) => target.id)),
    ).size,
  };
}

async function explain(sql, bindings) {
  const result = await db.raw(
    `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${sql}`,
    bindings,
  );
  const report = result.rows[0]['QUERY PLAN'][0];
  const plan = report.Plan;
  return {
    planningMs: report['Planning Time'],
    executionMs: report['Execution Time'],
    node: plan['Node Type'],
    rows: plan['Actual Rows'],
    sharedHitBlocks: plan['Shared Hit Blocks'] || 0,
    sharedReadBlocks: plan['Shared Read Blocks'] || 0,
    tempReadBlocks: plan['Temp Read Blocks'] || 0,
    tempWrittenBlocks: plan['Temp Written Blocks'] || 0,
  };
}

async function setup() {
  console.log(
    JSON.stringify({
      stage: 'setup',
      schema,
      postCount,
      targetCount,
      fanout,
      sharedTopTargets,
      junctionRows: postCount * fanout,
    }),
  );
  await db.raw(`CREATE SCHEMA ${quotedSchema}`);
  await db.raw(`
    CREATE UNLOGGED TABLE ${quotedPosts} (
      id integer PRIMARY KEY
    )
  `);
  await db.raw(`
    CREATE UNLOGGED TABLE ${quotedTargets} (
      id integer PRIMARY KEY,
      score integer NOT NULL,
      payload text NOT NULL
    )
  `);
  await db.raw(`
    CREATE UNLOGGED TABLE ${quotedJunction} (
      post_id integer NOT NULL,
      comment_id integer NOT NULL
    )
  `);

  let started = performance.now();
  await db.raw(
    `
    INSERT INTO ${quotedPosts} (id)
    SELECT id
    FROM generate_series(1, ?) AS id
  `,
    [postCount],
  );
  console.log(
    JSON.stringify({
      stage: 'insert-posts',
      ms: Number((performance.now() - started).toFixed(2)),
    }),
  );

  started = performance.now();
  await db.raw(
    `
    INSERT INTO ${quotedTargets} (id, score, payload)
    SELECT
      id,
      CASE
        WHEN ?::boolean THEN
          CASE
            WHEN id <= 4000 THEN 10000000 - id
            ELSE id % 1000000
          END
        ELSE ((id::bigint * 48271) % 2147483647)::integer
      END,
      repeat(md5(id::text), 4)
    FROM generate_series(1, ?) AS id
  `,
    [sharedTopTargets, targetCount],
  );
  console.log(
    JSON.stringify({
      stage: 'insert-targets',
      ms: Number((performance.now() - started).toFixed(2)),
    }),
  );

  started = performance.now();
  await db.raw(
    `
    INSERT INTO ${quotedJunction} (post_id, comment_id)
    SELECT
      post_id,
      CASE
        WHEN edge_number <= 20
          THEN (((post_id - 1) % 200) * 20) + edge_number
        ELSE 4001 + (
          (
            post_id::bigint * 7919 +
            edge_number::bigint * 104729
          ) % (? - 4000)
        )::integer
      END
    FROM generate_series(1, ?) AS post_id
    CROSS JOIN generate_series(1, ?) AS edge_number
  `,
    [targetCount, postCount, fanout],
  );
  console.log(
    JSON.stringify({
      stage: 'insert-junction',
      rows: postCount * fanout,
      ms: Number((performance.now() - started).toFixed(2)),
    }),
  );

  started = performance.now();
  await db.raw(`
    SET maintenance_work_mem = '512MB';
    CREATE INDEX post_comments_source_target_idx
      ON ${quotedJunction} (post_id, comment_id);
    ANALYZE ${quotedPosts};
    ANALYZE ${quotedTargets};
    ANALYZE ${quotedJunction};
  `);
  console.log(
    JSON.stringify({
      stage: 'index-analyze',
      ms: Number((performance.now() - started).toFixed(2)),
    }),
  );
}

async function main() {
  try {
    const version = await db.raw('SELECT version() AS version');
    console.log(
      JSON.stringify({
        stage: 'postgres',
        version: version.rows[0].version,
        poolMax: 4,
      }),
    );
    await setup();

    const size = await db.raw(
      `
      SELECT pg_size_pretty(sum(pg_total_relation_size(class.oid))) AS size
      FROM pg_class AS class
      JOIN pg_namespace AS namespace
        ON namespace.oid = class.relnamespace
      WHERE namespace.nspname = ?
        AND class.relkind IN ('r', 'i')
    `,
      [schema],
    );
    console.log(
      JSON.stringify({
        stage: 'dataset-ready',
        size: size.rows[0].size,
        sharedTopTargets,
        sampleParentCount,
        candidateEdges: sampleParentCount * fanout,
        requestedRows: sampleParentCount * relationLimit,
      }),
    );

    const strategies = [
      ['hybrid-edge-loader', runHybrid],
      ['one-stage-joined-top-k', runJoinedTopK],
      ['old-per-parent-c16', runPerParent],
    ];
    const results = [];
    for (const [name, run] of strategies) {
      const result = await measure(name, run);
      results.push(result);
      console.log(JSON.stringify({ stage: 'measurement', ...result }));
    }

    const edgePlan = await explain(edgeTopKSql, [parentIds, relationLimit]);
    const joinedPlan = await explain(joinedTopKSql, [parentIds, relationLimit]);
    console.log(
      JSON.stringify({
        stage: 'explain',
        edgeTopK: edgePlan,
        joinedTopK: joinedPlan,
      }),
    );
    console.log(
      JSON.stringify({
        stage: 'summary',
        datasetRows: postCount + targetCount + postCount * fanout,
        results,
      }),
    );
  } finally {
    if (!keepSchema) {
      await db.raw(`DROP SCHEMA IF EXISTS ${quotedSchema} CASCADE`);
      console.log(JSON.stringify({ stage: 'cleanup', schema }));
    }
    await db.destroy();
  }
}

await main();
