import assert from 'node:assert/strict';
import { asValue } from 'awilix';
import { buildContainer } from '../../../src/container';
import { init, shutdown } from '../../../src/init';
import { bootstrapSourceArtifacts } from '../../../src/data';
import { BootstrapDefinitionService } from '../../../src/engines/bootstrap/services/bootstrap-definition.service';
import { createOAuthRedirectValidator } from '../../../src/domain/auth/utils/oauth-redirect.util';
import { getEnfyraVersion } from '../../../src/shared/utils/enfyra-version.util';
import type { TDynamicContext } from '../../../src/shared/types';

async function main() {
  assert.equal(process.env.NODE_ENV, 'test');
  const uri = new URL(process.env.DB_URI!);
  assert.ok(['127.0.0.1', 'localhost'].includes(uri.hostname));
  assert.match(uri.pathname.slice(1), /^enfyra_fresh_matrix_[a-f0-9]{32}$/);
  const legacy = process.env.AUTH_ORIGIN_LEGACY_TARGET === '1';
  const container = buildContainer();
  if (legacy) {
    const sources = structuredClone(bootstrapSourceArtifacts);
    delete sources.snapshot.enfyra_auth_origin;
    sources.defaultData.enfyra_route = sources.defaultData.enfyra_route.filter((row: { path: string }) => row.path !== '/enfyra_auth_origin');
    sources.defaultData.enfyra_menu = sources.defaultData.enfyra_menu.filter((row: { path: string }) => row.path !== '/settings/oauth/origins');
    for (const menu of sources.defaultData.enfyra_menu) {
      if (menu.permission?.or) menu.permission.or = menu.permission.or.filter((entry: { route: string }) => entry.route !== '/enfyra_auth_origin');
    }
    sources.dataCorrections.enfyra_route = sources.dataCorrections.enfyra_route.filter((row: { _unique: { path: { _eq: string } } }) => row._unique.path._eq !== '/enfyra_auth_origin');
    sources.dataCorrections.enfyra_menu = sources.dataCorrections.enfyra_menu.filter((row: { _unique: { path: { _eq: string } } }) => row._unique.path._eq !== '/settings/oauth');
    container.register({ bootstrapDefinitionService: asValue(new BootstrapDefinitionService({ databaseConfigService: container.cradle.databaseConfigService }, sources)) });
  }
  const initializer = container.cradle.firstRunInitializer;
  const original = initializer.run.bind(initializer);
  let bootstrapRuns = 0;
  initializer.run = async () => { bootstrapRuns++; await original(); };
  try {
    await init(container);
    assert.equal(bootstrapRuns, Number(process.env.EXPECTED_BOOTSTRAP_RUNS));
    const c = container.cradle;
    const query = c.queryBuilderService;
    const pk = query.getPkField();
    const setting = await query.findOne({ table: 'enfyra_setting', fields: [pk, 'isInit', 'enfyraVersion'] });
    assert.equal(Boolean(setting.isInit), true);
    assert.equal(setting.enfyraVersion, getEnfyraVersion());
    const metadata = await query.findOne({ table: 'enfyra_table', filter: { name: { _eq: 'enfyra_auth_origin' } }, fields: [pk] });
    if (legacy) {
      assert.ok(!metadata);
    } else {
      assert.ok(metadata);
      const menu = await query.findOne({ table: 'enfyra_menu', filter: { path: { _eq: '/settings/oauth/origins' } }, fields: [pk, 'parent.label', 'isPublic'] });
      assert.equal(menu.parent?.label, 'OAuth');
      assert.equal(Boolean(menu.isPublic), false);
      const routes = await query.find({ table: 'enfyra_route', filter: { path: { _eq: '/enfyra_auth_origin' } }, fields: [pk] });
      assert.equal(routes.data.length, 1);
      const rootContext = { $user: { isRootAdmin: true, roles: [] }, $body: {}, $query: {} } as TDynamicContext;
      const repo = c.dynamicRepositoryFactory.create('enfyra_auth_origin', rootContext);
      const corsBefore = await query.find({ table: 'enfyra_cors_origin', fields: [pk, 'value', 'isEnabled'], limit: -1 });
      const origin = 'https://auth-origin-fixture.example.test';
      const row = (await repo.create({ data: { value: origin, isEnabled: true } })).data[0];
      assert.ok(row[pk]);
      let validate = await createOAuthRedirectValidator(query, ['https://admin.example.test']);
      assert.equal(validate(origin + '/return'), origin + '/return');
      await repo.update({ id: row[pk], data: { isEnabled: false } });
      validate = await createOAuthRedirectValidator(query, ['https://admin.example.test']);
      assert.throws(() => validate(origin + '/return'));
      await repo.update({ id: row[pk], data: { isEnabled: true } });
      await assert.rejects(() => repo.create({ data: { value: origin, isEnabled: true } }));
      await assert.rejects(() => repo.create({ data: { value: 'https://unsafe.example.test/path' } }));
      await repo.delete({ id: row[pk] });
      validate = await createOAuthRedirectValidator(query, ['https://admin.example.test']);
      assert.throws(() => validate(origin + '/return'));
      const corsAfter = await query.find({ table: 'enfyra_cors_origin', fields: [pk, 'value', 'isEnabled'], limit: -1 });
      assert.deepEqual(corsAfter, corsBefore);
    }
    const dashboard = await query.findOne({ table: 'enfyra_menu', filter: { path: { _eq: '/dashboard' } }, fields: [pk] });
    const linked = await query.findOne({ table: 'enfyra_setting', fields: [`defaultPage.${pk}`] });
    assert.equal(String(linked.defaultPage?.[pk] ?? linked.defaultPage), String(dashboard[pk]));
    const removedHeaderMetadata = await query.findOne({ table: 'enfyra_table', filter: { name: { _eq: 'enfyra_auth_header' } }, fields: [pk] });
    assert.equal(removedHeaderMetadata, null);
    if (query.isMongoDb()) assert.equal((await query.getMongoDb().listCollections({ name: 'enfyra_auth_header' }).toArray()).length, 0);
    else assert.equal(await query.getKnex().schema.hasTable('enfyra_auth_header'), false);
    console.log('AUTH_ORIGIN_BOOT_RESULT ' + JSON.stringify({ legacy, bootstrapRuns, version: setting.enfyraVersion, passed: true }));
    console.log('STORAGE_BOOT_RESULT ' + JSON.stringify({ bootstrapRuns, version: setting.enfyraVersion, dashboardLinked: true }));
  } finally {
    await shutdown(container);
  }
}
main().then(() => process.exit(0), error => { console.error(error); process.exit(1); });
