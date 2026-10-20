import assert from 'node:assert/strict';
import { buildContainer } from '../../../src/container';
import { init, shutdown } from '../../../src/init';
import { getEnfyraVersion } from '../../../src/shared/utils/enfyra-version.util';

async function main() {
  const container = buildContainer();
  const initializer = container.cradle.firstRunInitializer;
  const original = initializer.run.bind(initializer);
  let bootstrapRuns = 0;
  initializer.run = async () => {
    bootstrapRuns++;
    return original();
  };
  try {
    await init(container);
    assert.equal(bootstrapRuns, Number(process.env.EXPECTED_BOOTSTRAP_RUNS));
    const query = container.cradle.queryBuilderService;
    const pk = query.getPkField();
    const setting = await query.findOne({ table: 'enfyra_setting', fields: [pk, 'isInit', 'enfyraVersion', `defaultPage.${pk}`] });
    assert.equal(Boolean(setting.isInit), true);
    assert.equal(setting.enfyraVersion, getEnfyraVersion());
    const menu = await query.findOne({ table: 'enfyra_menu', filter: { path: { _eq: '/dashboard' } }, fields: [pk, 'isSystem'] });
    assert.ok(menu);
    assert.equal(Boolean(menu.isSystem), false);
    const extension = await query.findOne({ table: 'enfyra_extension', filter: { menu: { [pk]: { _eq: menu[pk] } } }, fields: [pk, 'code', 'compiledCode', 'isSystem'] });
    assert.ok(extension?.code?.includes('<template>'));
    assert.ok(extension.compiledCode.length > 0);
    assert.equal(Boolean(extension.isSystem), false);
    assert.equal(String(setting.defaultPage?.[pk] ?? setting.defaultPage), String(menu[pk]));
    console.log('STORAGE_BOOT_RESULT ' + JSON.stringify({ bootstrapRuns, version: setting.enfyraVersion, dashboardLinked: true }));
  } finally {
    await shutdown(container);
  }
}
main().then(() => process.exit(0), (error) => { console.error(error); process.exit(1); });
