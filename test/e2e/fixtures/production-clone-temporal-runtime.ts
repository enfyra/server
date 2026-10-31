import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { buildContainer } from '../../../src/container';
import { initBootstrap, shutdown } from '../../../src/init';
import { phaseCoreCaches } from '../../../src/wiring/init/phases';
import type { TDynamicContext } from '../../../src/shared/types';

async function main() {
  const uri = new URL(process.env.DB_URI!);
  assert.equal(process.env.NODE_ENV, 'test');
  assert.equal(uri.hostname, '127.0.0.1');
  assert.match(uri.pathname.slice(1), /^enfyra_1endpoint_clone_[a-f0-9]{32}$/);
  const directory = process.env.CLONE_PRIVATE_DIRECTORY;
  assert.ok(directory);
  assert.equal(process.env.ENFYRA_TEST_KERNEL_DIR, undefined);
  const require = createRequire(import.meta.url);
  const entry = require.resolve('@enfyra/kernel');
  const artifact = {
    version: JSON.parse(readFileSync(join(entry, '../../package.json'), 'utf8')).version,
    sha256: createHash('sha256').update(readFileSync(entry)).digest('hex'),
  };
  assert.equal(artifact.version, '2.1.4');
  assert.equal(artifact.sha256, 'fadad5a5e58b1bf6f7675b30c84eb2cffa3e3b9050e99eec5df578c63ebf954d');
  const container = buildContainer();
  const checks: string[] = [];
  const initializer = container.cradle.firstRunInitializer;
  let bootstraps = 0;
  const original = initializer.run.bind(initializer);
  initializer.run = async () => { bootstraps++; await original(); };
  try {
    await initBootstrap(container);
    assert.equal(bootstraps, 0);
    const c = container.cradle;
    await phaseCoreCaches(c);
    await c.runtimeRegistryService.publishFromCache('metadata', c.metadataCacheService);
    for (const [identifier, service] of [
      ['field-permission', c.fieldPermissionCacheBuilder],
      ['column-rule', c.columnRuleCacheBuilder],
      ['setting', c.settingCacheService],
    ] as const) {
      await service.reload();
      await c.runtimeRegistryService.publishFromCache(identifier, service);
    }
    const query = c.queryBuilderService;
    const sql = query.getKnex();
    const context = { $user: { isRootAdmin: true, roles: [] }, $query: {}, $body: {} } as TDynamicContext;
    const usage = c.dynamicRepositoryFactory.create('ai_usage', context);
    const source = (await sql.raw(`SELECT id, to_char("occurredAt" AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS instant FROM ai_usage WHERE "occurredAt" IS NOT NULL ORDER BY "occurredAt" DESC, id DESC LIMIT 1`)).rows[0];
    assert.ok(source);
    const day = source.instant.slice(0, 10);
    const start = `${day}T00:00:00.000Z`;
    const end = new Date(new Date(start).getTime() + 86400_000).toISOString();
    const row = (await usage.find({ filter: { id: { _eq: source.id } }, fields: ['id', 'occurredAt'], limit: 1 })).data[0];
    assert.equal(new Date(row.occurredAt).toISOString(), source.instant);
    checks.push('real-ai-usage-temporal-read-matches-physical-milliseconds');
    const expected = Number((await sql('ai_usage').where('occurredAt', '>=', start).where('occurredAt', '<', end).count({ count: '*' }).first())!.count);
    const range = { occurredAt: { _gte: start, _lt: end } };
    const page = await usage.find({ filter: range, fields: ['id', 'occurredAt'], limit: 17, meta: 'filterCount' });
    assert.equal(Number(page.meta?.filterCount), expected);
    assert.equal(page.data.length, Math.min(17, expected));
    const variants = [
      { _and: [{ occurredAt: { _gte: start } }, { occurredAt: { _lt: end } }] },
      { _or: [range, { id: { _eq: -1 } }] },
      { occurredAt: { _between: [start, end] } },
      { id: { _eq: source.id }, occurredAt: { _in: [source.instant] } },
      { occurredAt: { _gte: start } },
      { occurredAt: { _lt: end } },
    ];
    for (const [index, filter] of variants.entries()) {
      const actual = await usage.find({ filter, fields: ['id'], limit: 1, meta: 'filterCount' });
      let raw = sql('ai_usage');
      if (index < 2) raw = raw.where('occurredAt', '>=', start).where('occurredAt', '<', end);
      else if (index === 2) raw = raw.whereBetween('occurredAt', [start, end]);
      else if (index === 3) raw = raw.where('id', source.id).whereIn('occurredAt', [source.instant]);
      else if (index === 4) raw = raw.where('occurredAt', '>=', start);
      else raw = raw.where('occurredAt', '<', end);
      const count = Number((await raw.count({ count: '*' }).first())!.count);
      assert.equal(Number(actual.meta?.filterCount), count);
    }
    checks.push('real-ai-usage-UTC-ranges-between-in-and-or-one-sided-match-SQL');
    for (const timezone of ['UTC', 'Asia/Ho_Chi_Minh', 'America/New_York']) {
      const actual = await usage.aggregate({ filter: range, dimensions: [{ field: 'occurredAt', bucket: 'day', timezone }], measures: { entries: { count: 'id' } } });
      const expectedBuckets = (await sql.raw(`SELECT date_trunc('day', "occurredAt" AT TIME ZONE ?) AT TIME ZONE ? AS bucket, count(*)::int AS entries FROM ai_usage WHERE "occurredAt" >= ? AND "occurredAt" < ? GROUP BY 1 ORDER BY 1`, [timezone, timezone, start, end])).rows;
      const normalize = (values: Array<{ bucket?: string | Date; occurredAt?: string | Date; entries: number }>) => values.map((value) => [new Date(value.bucket ?? value.occurredAt!).toISOString(), Number(value.entries)]).sort();
      assert.deepEqual(normalize(actual.data), normalize(expectedBuckets));
    }
    checks.push('real-ai-usage-UTC-Hanoi-NewYork-buckets-match-native-SQL');
    const marker = `clone-final-${randomUUID()}`;
    const repo = c.dynamicRepositoryFactory.create('announcements', context);
    let id: number | undefined;
    try {
      for (const publishedAt of ['2024-02-30T00:00:00.000Z', '2026-10-27T00:00:00+07:00', '2026-10-27', '0000-01-01T00:00:00.000Z', '2026-10-27T00:00:00.123456Z']) {
        await assert.rejects(c.mutationBodyValidationService.validate({ tableName: 'announcements', mode: 'create', body: { title: marker, publishedAt }, user: context.$user }));
      }
      const created = (await repo.create({ data: { title: marker, isEnabled: false, isPinned: false, priority: 7, publishedAt: '2040-05-20T12:34:56.789Z' } })).data[0];
      id = created.id;
      assert.ok(id);
      const saved = (await repo.find({ filter: { id: { _eq: id } }, fields: ['id', 'title', 'publishedAt'], limit: 1 })).data[0];
      assert.equal(saved.title, marker);
      assert.equal(new Date(saved.publishedAt).toISOString(), '2040-05-20T12:34:56.789Z');
      await repo.update({ id, data: { publishedAt: '2041-06-21T01:02:03.456Z', priority: 9 } });
      const updated = (await repo.find({ filter: { id: { _eq: id } }, fields: ['id', 'publishedAt', 'priority'], limit: 1 })).data[0];
      assert.equal(new Date(updated.publishedAt).toISOString(), '2041-06-21T01:02:03.456Z');
      assert.equal(updated.priority, 9);
      await repo.delete({ id });
      assert.equal((await repo.find({ filter: { id: { _eq: id } }, fields: ['id'], limit: 1 })).data.length, 0);
      id = undefined;
      checks.push('existing-announcements-create-read-update-delete-after-2038', 'existing-temporal-column-invalid-input-rejected');
    } finally {
      if (id !== undefined) await query.delete('announcements', id);
    }
    const result = { version: '2.4.0', artifact, checks, sampledDayRows: expected, bootstrapRuns: bootstraps, automationStarted: false, passed: true };
    writeFileSync(join(directory, 'final-clone-runtime.json'), JSON.stringify(result, null, 2), { mode: 0o600 });
    console.log('FINAL_CLONE_RUNTIME_RESULT', JSON.stringify(result));
  } finally {
    await shutdown(container);
  }
}
main().then(() => process.exit(0), (error) => { console.error(error); process.exit(1); });
