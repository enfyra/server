/**
 * Integration test: verifies that the SQL patterns generated for MySQL
 * actually produce correctly-ordered JSON arrays on a real MySQL instance.
 *
 * Requires: MySQL running at localhost:3306 with root:1234 (or DB_URI env).
 * Skipped automatically when MySQL is unavailable.
 */

import knex, { Knex } from 'knex';

let db: Knex;
let skip = false;

beforeAll(async () => {
  const uri = process.env.DB_URI || 'mysql://root:1234@localhost:3306/enfyra';
  if (!uri.startsWith('mysql')) {
    skip = true;
    return;
  }

  try {
    db = knex({ client: 'mysql2', connection: uri });
    await db.raw('SELECT 1');
    await db.raw('SET SESSION group_concat_max_len = 16777216');
  } catch {
    skip = true;
  }
});

afterAll(async () => {
  if (db) await db.destroy();
});

function runIf(name: string, fn: () => Promise<void>) {
  it(name, async () => {
    if (skip) return;
    await fn();
  });
}

describe('MySQL real-DB ordering tests', () => {
  const PREFIX = '__test_ordering_';
  const parentTable = `${PREFIX}parent`;
  const childTable = `${PREFIX}child`;
  const targetTable = `${PREFIX}target`;
  const junctionTable = `${PREFIX}junction`;

  beforeAll(async () => {
    if (skip) return;
    // Clean up any leftover tables
    for (const t of [junctionTable, childTable, targetTable, parentTable]) {
      await db.raw(`DROP TABLE IF EXISTS \`${t}\``);
    }

    await db.schema.createTable(parentTable, (t) => {
      t.increments('id').primary();
      t.string('name');
    });
    await db.schema.createTable(childTable, (t) => {
      t.increments('id').primary();
      t.integer('parentId').unsigned().references('id').inTable(parentTable);
      t.string('val');
    });
    await db.schema.createTable(targetTable, (t) => {
      t.increments('id').primary();
      t.string('label');
    });
    await db.schema.createTable(junctionTable, (t) => {
      t.integer('parentId').unsigned().references('id').inTable(parentTable);
      t.integer('targetId').unsigned().references('id').inTable(targetTable);
      t.primary(['parentId', 'targetId']);
    });

    // Seed: insert children in REVERSE id order to stress-test ordering
    await db(parentTable).insert([
      { id: 1, name: 'p1' },
      { id: 2, name: 'p2' },
    ]);
    await db(childTable).insert([
      { id: 5, parentId: 1, val: 'e' },
      { id: 3, parentId: 1, val: 'c' },
      { id: 1, parentId: 1, val: 'a' },
      { id: 4, parentId: 2, val: 'd' },
      { id: 2, parentId: 2, val: 'b' },
    ]);
    await db(targetTable).insert([
      { id: 5, label: 'E' },
      { id: 3, label: 'C' },
      { id: 1, label: 'A' },
      { id: 4, label: 'D' },
      { id: 2, label: 'B' },
    ]);
    await db(junctionTable).insert([
      { parentId: 1, targetId: 5 },
      { parentId: 1, targetId: 3 },
      { parentId: 1, targetId: 1 },
      { parentId: 2, targetId: 4 },
      { parentId: 2, targetId: 2 },
    ]);
  });

  afterAll(async () => {
    if (skip) return;
    for (const t of [junctionTable, childTable, targetTable, parentTable]) {
      await db.raw(`DROP TABLE IF EXISTS \`${t}\``);
    }
  });

  // ── Pattern 1: correlated O2M subquery with derived table ──────────────

  runIf(
    'O2M correlated subquery: derived table produces ASC-ordered array',
    async () => {
      const rows = await db.raw(`
        SELECT p.id, p.name,
          (SELECT ifnull(JSON_ARRAYAGG(JSON_OBJECT('id', c.id, 'val', c.val)), JSON_ARRAY())
           FROM (SELECT c.* FROM \`${childTable}\` c
                 WHERE c.parentId = p.id
                 ORDER BY c.id ASC) c) as children
        FROM \`${parentTable}\` p ORDER BY p.id
      `);

      const data = rows[0];
      expect(data).toHaveLength(2);

      const parse = (v: any) =>
        typeof v === 'string' ? JSON.parse(v) : v;

      const p1Children = parse(data[0].children);
      expect(p1Children.map((c: any) => c.id)).toEqual([1, 3, 5]);
      expect(p1Children.map((c: any) => c.val)).toEqual(['a', 'c', 'e']);

      const p2Children = parse(data[1].children);
      expect(p2Children.map((c: any) => c.id)).toEqual([2, 4]);
    },
  );

  // ── Pattern 2: correlated M2M subquery with derived table ──────────────

  runIf(
    'M2M correlated subquery: derived table produces ASC-ordered array',
    async () => {
      const rows = await db.raw(`
        SELECT p.id, p.name,
          (SELECT ifnull(JSON_ARRAYAGG(JSON_OBJECT('id', c.id, 'label', c.label)), JSON_ARRAY())
           FROM (SELECT c.* FROM \`${junctionTable}\` j
                 JOIN \`${targetTable}\` c ON j.targetId = c.id
                 WHERE j.parentId = p.id
                 ORDER BY c.id ASC) c) as targets
        FROM \`${parentTable}\` p ORDER BY p.id
      `);

      const data = rows[0];
      const parse = (v: any) =>
        typeof v === 'string' ? JSON.parse(v) : v;

      const p1Targets = parse(data[0].targets);
      expect(p1Targets.map((t: any) => t.id)).toEqual([1, 3, 5]);

      const p2Targets = parse(data[1].targets);
      expect(p2Targets.map((t: any) => t.id)).toEqual([2, 4]);
    },
  );

  // ── Pattern 3: CTE O2M with GROUP_CONCAT ───────────────────────────────

  runIf(
    'CTE O2M: GROUP_CONCAT ORDER BY produces ASC-ordered array',
    async () => {
      const rows = await db.raw(`
        WITH limited AS (SELECT id FROM \`${parentTable}\`)
        SELECT
          r.parentId as parent_id,
          CAST(CONCAT('[', GROUP_CONCAT(
            JSON_OBJECT('id', r.id, 'val', r.val)
            ORDER BY r.id ASC SEPARATOR ','
          ), ']') AS JSON) as children
        FROM \`${childTable}\` r
        INNER JOIN limited l ON r.parentId = l.id
        GROUP BY r.parentId
      `);

      const data = rows[0];
      expect(data).toHaveLength(2);

      const g1 = data.find((r: any) => r.parent_id === 1);
      const children1 =
        typeof g1.children === 'string'
          ? JSON.parse(g1.children)
          : g1.children;
      expect(children1.map((c: any) => c.id)).toEqual([1, 3, 5]);

      const g2 = data.find((r: any) => r.parent_id === 2);
      const children2 =
        typeof g2.children === 'string'
          ? JSON.parse(g2.children)
          : g2.children;
      expect(children2.map((c: any) => c.id)).toEqual([2, 4]);
    },
  );

  // ── Pattern 4: CTE M2M with GROUP_CONCAT ───────────────────────────────

  runIf(
    'CTE M2M: GROUP_CONCAT ORDER BY produces ASC-ordered array',
    async () => {
      const rows = await db.raw(`
        WITH limited AS (SELECT id FROM \`${parentTable}\`)
        SELECT
          j.parentId as parent_id,
          CAST(CONCAT('[', GROUP_CONCAT(
            JSON_OBJECT('id', r.id, 'label', r.label)
            ORDER BY r.id ASC SEPARATOR ','
          ), ']') AS JSON) as targets
        FROM \`${junctionTable}\` j
        INNER JOIN \`${targetTable}\` r ON j.targetId = r.id
        INNER JOIN limited l ON j.parentId = l.id
        GROUP BY j.parentId
      `);

      const data = rows[0];
      const g1 = data.find((r: any) => r.parent_id === 1);
      const targets1 =
        typeof g1.targets === 'string'
          ? JSON.parse(g1.targets)
          : g1.targets;
      expect(targets1.map((t: any) => t.id)).toEqual([1, 3, 5]);

      const g2 = data.find((r: any) => r.parent_id === 2);
      const targets2 =
        typeof g2.targets === 'string'
          ? JSON.parse(g2.targets)
          : g2.targets;
      expect(targets2.map((t: any) => t.id)).toEqual([2, 4]);
    },
  );

  // ── Empty result handling ──────────────────────────────────────────────

  runIf(
    'O2M correlated: empty children returns JSON_ARRAY()',
    async () => {
      await db(parentTable).insert({ id: 99, name: 'empty' });
      const rows = await db.raw(`
        SELECT p.id,
          (SELECT ifnull(JSON_ARRAYAGG(JSON_OBJECT('id', c.id)), JSON_ARRAY())
           FROM (SELECT c.* FROM \`${childTable}\` c
                 WHERE c.parentId = p.id
                 ORDER BY c.id ASC) c) as children
        FROM \`${parentTable}\` p WHERE p.id = 99
      `);
      const data = rows[0];
      const children =
        typeof data[0].children === 'string'
          ? JSON.parse(data[0].children)
          : data[0].children;
      expect(children).toEqual([]);
      await db(parentTable).where({ id: 99 }).delete();
    },
  );

  // ── group_concat_max_len ──────────────────────────────────────────────

  runIf(
    'group_concat_max_len is set to 16MB for this session',
    async () => {
      const rows = await db.raw('SELECT @@session.group_concat_max_len as val');
      const val = rows[0][0].val;
      expect(Number(val)).toBe(16777216);
    },
  );
});
