import assert from 'node:assert/strict';
import { buildContainer } from '../../../src/container';
import { initBootstrap, shutdown } from '../../../src/init';
import { getEnfyraVersion } from '../../../src/shared/utils/enfyra-version.util';

async function main() {
  const uri = new URL(process.env.DB_URI!);
  assert.ok(['127.0.0.1', 'localhost'].includes(uri.hostname));
  assert.match(uri.pathname.slice(1), /^enfyra_tag_upgrade_[a-f0-9]{24}$/);
  assert.equal(process.env.NODE_ENV, 'test');
  const stage = process.env.TAG_UPGRADE_STAGE;
  assert.ok(stage === 'source' || stage === 'target' || stage === 'stable');
  const seed = stage === 'source';
  const container = buildContainer();
  const initializer = container.cradle.firstRunInitializer;
  const original = initializer.run.bind(initializer);
  let bootstrapRuns = 0;
  initializer.run = async () => {
    bootstrapRuns++;
    return original();
  };
  const table = 'temporal_tag_records';
  try {
    await initBootstrap(container);
    assert.equal(bootstrapRuns, stage === 'stable' ? 0 : 1);
    const query = container.cradle.queryBuilderService;
    const mongo = query.isMongoDb();
    const sql = mongo ? null : query.getKnex();
    const db = mongo ? query.getMongoDb() : null;
    const backend = query.getDatabaseType();
    const values = ['2040-05-20T12:34:56.123Z', '2040-05-20T12:34:56.456Z', '2040-05-20T12:34:56.789Z'];
    if (seed) {
      assert.equal(getEnfyraVersion(), '2.3.3');
      if (db) {
        await db.createCollection(table);
        await db.collection(table).insertOne({ legacyDay: new Date(values[0]), legacyStamp: new Date(values[1]), instant: new Date(values[2]) });
        const inserted = await db.collection('enfyra_table').insertOne({ name: table, isSystem: false, isSingleRecord: false, validateBody: true, uniques: [], indexes: [], metadata: {} });
        await db.collection('enfyra_column').insertMany([
          { table: inserted.insertedId, name: '_id', type: 'objectId', isPrimary: true, isGenerated: true, isNullable: false },
          ...['date', 'timestamp', 'datetime'].map((type, index) => ({ table: inserted.insertedId, name: ['legacyDay', 'legacyStamp', 'instant'][index], type, isNullable: false })),
        ]);
      } else {
        await sql!.schema.createTable(table, (builder) => {
          builder.increments('id').primary();
          for (const name of ['legacyDay', 'legacyStamp', 'instant']) builder.specificType(name, backend === 'postgres' ? 'TIMESTAMPTZ' : 'DATETIME(6)').notNullable();
          builder.timestamp('createdAt', { useTz: true, precision: 3 }).defaultTo(sql!.fn.now(3));
          builder.timestamp('updatedAt', { useTz: true, precision: 3 }).defaultTo(sql!.fn.now(3));
        });
        const precise = ['2040-05-20 12:34:56.123456', '2040-05-20 12:34:56.456789', '2040-05-20 12:34:56.789123'];
        await sql!.raw('INSERT INTO ?? (??, ??, ??) VALUES (?, ?, ?)', [table, 'legacyDay', 'legacyStamp', 'instant', ...precise]);
        await sql!('enfyra_table').insert({ name: table, isSystem: false, isSingleRecord: false, validateBody: true, uniques: '[]', indexes: '[]', metadata: '{}' });
        const owner = await sql!('enfyra_table').where({ name: table }).first('id');
        await sql!('enfyra_column').insert([
          { tableId: owner.id, name: 'id', type: 'int', isPrimary: true, isGenerated: true, isNullable: false },
          ...['date', 'timestamp', 'datetime'].map((type, index) => ({ tableId: owner.id, name: ['legacyDay', 'legacyStamp', 'instant'][index], type, isNullable: false })),
        ]);
      }
    } else {
      assert.equal(getEnfyraVersion(), '2.4.0');
      const owner = db ? await db.collection('enfyra_table').findOne({ name: table }) : await sql!('enfyra_table').where({ name: table }).first('id');
      assert.ok(owner);
      const columns = db ? await db.collection('enfyra_column').find({ table: owner._id }).toArray() : await sql!('enfyra_column').where({ tableId: owner.id }).select('*');
      assert.ok(columns.length >= 4);
      for (const name of ['legacyDay', 'legacyStamp', 'instant']) assert.equal(columns.find((column: { name: string }) => column.name === name)?.type, 'datetime');
      if (db) {
        const row = await db.collection(table).findOne({});
        for (const [index, name] of ['legacyDay', 'legacyStamp', 'instant'].entries()) {
          assert.ok(row![name] instanceof Date);
          assert.equal(row![name].toISOString(), values[index]);
        }
      } else {
        const rows = backend === 'postgres'
          ? (await sql!.raw(`SELECT to_char(?? AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS a, to_char(?? AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS b, to_char(?? AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS c FROM ??`, ['legacyDay', 'legacyStamp', 'instant', table])).rows
          : (await sql!.raw('SELECT CAST(?? AS CHAR) AS a, CAST(?? AS CHAR) AS b, CAST(?? AS CHAR) AS c FROM ??', ['legacyDay', 'legacyStamp', 'instant', table]))[0];
        assert.deepEqual([rows[0].a, rows[0].b, rows[0].c], ['2040-05-20 12:34:56.123456', '2040-05-20 12:34:56.456789', '2040-05-20 12:34:56.789123']);
      }
    }
    const setting = db ? await db.collection('enfyra_setting').findOne({}) : await sql!('enfyra_setting').first('enfyraVersion', 'isInit');
    assert.equal(setting!.enfyraVersion, seed ? '2.3.3' : '2.4.0');
    assert.ok(setting!.isInit);
    console.log('TAG_UPGRADE_RESULT', JSON.stringify({ backend, stage, version: setting!.enfyraVersion, bootstrapRuns, passed: true }));
  } finally {
    await shutdown(container);
  }
}
main().then(() => process.exit(0), (error) => { console.error(error); process.exit(1); });
