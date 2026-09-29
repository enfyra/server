import assert from 'node:assert/strict';
import { buildContainer } from '../../../src/container';
import { init, shutdown } from '../../../src/init';

async function main() {
const container = buildContainer();
let failure: unknown;
try {
  await init(container);
  const query = container.cradle.queryBuilderService;
  const [routes, methods, configs, handlers, settings] = await Promise.all([
    query.find({ table: 'enfyra_route', fields: ['id'], limit: 0 }),
    query.find({ table: 'enfyra_method', fields: ['id'], limit: 0 }),
    query.find({ table: 'enfyra_route_method_config', fields: ['id', 'route.id', 'method.id', 'timeout', 'available'], limit: 0 }),
    query.find({ table: 'enfyra_route_handler', fields: ['id', 'routeMethodConfig.id', 'timeout'], limit: 0 }),
    query.find({ table: 'enfyra_setting', fields: ['enfyraVersion', 'isInit'], limit: 1 }),
  ]);
  assert.equal(configs.data.length, routes.data.length * methods.data.length);
  const expectedHandlers = Number(process.env.UPGRADE_EXPECTED_HANDLERS);
  assert.ok(Number.isSafeInteger(expectedHandlers) && expectedHandlers > 0, 'Set UPGRADE_EXPECTED_HANDLERS from the pre-upgrade clone count plus declared route seeds');
  assert.equal(handlers.data.length, expectedHandlers);
  assert.ok(handlers.data.every((handler: any) => handler.routeMethodConfig?.id));
  assert.equal(settings.data[0]?.enfyraVersion, '2.3.1');
  assert.equal(settings.data[0]?.isInit, true);
  process.stdout.write(`[prod-clone] routes=${routes.data.length} methods=${methods.data.length} configs=${configs.data.length} handlers=${handlers.data.length} version=${settings.data[0].enfyraVersion} passed\n`);
} catch (error) {
  failure = error;
} finally {
  try {
    await shutdown(container);
  } catch (error) {
    failure ??= error;
  }
}
if (failure) throw failure;
}

void main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
