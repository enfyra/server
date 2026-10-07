import assert from 'node:assert/strict';
import { buildContainer } from '../../../src/container';
import { init, shutdown } from '../../../src/init';
import { CACHE_EVENTS } from '../../../src/shared/utils/cache-events.constants';

async function main() {
  const container = buildContainer();
  const initializer = container.cradle.firstRunInitializer;
  const originalRun = initializer.run.bind(initializer);
  let bootstrapRuns = 0;
  let readyEvents = 0;
  initializer.run = async () => {
    bootstrapRuns++;
    await originalRun();
  };
  container.cradle.eventEmitter.on(
    CACHE_EVENTS.SYSTEM_READY,
    () => readyEvents++,
  );
  try {
    await init(container);
    assert.equal(bootstrapRuns, Number(process.env.EXPECTED_BOOTSTRAP_RUNS));
    assert.equal(readyEvents, 1);
    const setting = await container.cradle.queryBuilderService.findOne({
      table: 'enfyra_setting',
      fields: ['id', 'enfyraVersion', 'isInit', 'defaultPage.id'],
    });
    assert.equal(setting?.enfyraVersion, '2.3.2');
    assert.equal(setting?.isInit, true);
    assert.equal(setting?.defaultPage, null);
    const dashboard = await container.cradle.queryBuilderService.findOne({
      table: 'enfyra_menu',
      filter: { path: { _eq: '/dashboard' } },
      fields: ['id', 'isSystem'],
    });
    assert.equal(dashboard?.isSystem, false);
    console.log(
      JSON.stringify({
        bootstrapRuns,
        readyEvents,
        version: setting?.enfyraVersion,
        passed: true,
      }),
    );
  } finally {
    await shutdown(container);
  }
}

void main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
