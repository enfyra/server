import assert from 'node:assert/strict';
import { buildContainer } from '../../../src/container';
import { init, shutdown } from '../../../src/init';
import { getEnfyraVersion } from '../../../src/shared/utils/enfyra-version.util';
import { verifyRuntimeMigrationMatrix } from '../utils/runtime-migration-matrix.util';

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
    const setting = await query.findOne({
      table: 'enfyra_setting',
      fields: [pk, 'isInit', 'enfyraVersion', `defaultPage.${pk}`],
    });
    assert.equal(Boolean(setting.isInit), true);
    assert.equal(setting.enfyraVersion, getEnfyraVersion());

    let runtimeChecks: string[] = [];
    if (process.env.RUN_RUNTIME_MIGRATION_MATRIX === '1') {
      runtimeChecks = await verifyRuntimeMigrationMatrix(container.cradle);
    }

    console.log(
      'LIFECYCLE_BOOT_RESULT ' +
        JSON.stringify({
          bootstrapRuns,
          version: setting.enfyraVersion,
          runtimeChecks,
          passed: true,
        }),
    );
  } finally {
    await shutdown(container);
  }
}

main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
