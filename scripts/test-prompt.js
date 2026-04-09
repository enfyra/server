const { spawnSync } = require('child_process');
const path = require('path');
const prompts = require('prompts');

const root = path.join(__dirname, '..');

function mergeNodeOptions() {
  const cur = process.env.NODE_OPTIONS || '';
  return cur.includes('no-node-snapshot')
    ? process.env
    : { ...process.env, NODE_OPTIONS: `--no-node-snapshot ${cur}`.trim() };
}

function runNode(scriptRel) {
  const r = spawnSync(process.execPath, [path.join(root, scriptRel)], {
    cwd: root,
    stdio: 'inherit',
    env: process.env,
  });
  process.exit(r.status ?? 1);
}

function runYarnJest(jestArgs) {
  const r = spawnSync('yarn', ['jest', ...jestArgs, '--runInBand'], {
    cwd: root,
    stdio: 'inherit',
    env: mergeNodeOptions(),
  });
  process.exit(r.status ?? 1);
}

const JEST_CHOICES = [
  { title: 'All specs (*.spec.ts)', value: { kind: 'all' } },
  { title: 'core — policy, bcrypt, code-transformer, canonical route, logging', value: { kind: 'path', path: 'test/core' } },
  { title: 'shared — enfyra-route-engine', value: { kind: 'path', path: 'test/shared' } },
  { title: 'cache — guard-cache, pubsub, multi-instance redis', value: { kind: 'path', path: 'test/cache' } },
  { title: 'guards — guard-evaluator', value: { kind: 'path', path: 'test/guards' } },
  { title: 'security — client-ip, fetch-helper', value: { kind: 'path', path: 'test/security' } },
  { title: 'executor-engine — isolated-vm, bridge, tuning, batch…', value: { kind: 'path', path: 'test/executor-engine' } },
  { title: 'query-builder — filter DSL, engine core, unaccent', value: { kind: 'path', path: 'test/query-builder' } },
  { title: 'knex — sql pool, fk naming', value: { kind: 'path', path: 'test/knex' } },
  { title: 'package-management — CDN cache, multi-instance', value: { kind: 'path', path: 'test/package-management' } },
  { title: 'websocket — dx-debug (multi-instance needs Redis)', value: { kind: 'path', path: 'test/websocket' } },
  { title: 'flow — step-executor security', value: { kind: 'path', path: 'test/flow' } },
  { title: 'admin — admin-test-run', value: { kind: 'path', path: 'test/admin' } },
  { title: 'interceptors — dynamic post-hooks', value: { kind: 'path', path: 'test/interceptors' } },
  {
    title: 'integration — filter-dsl-real-db (FILTER_INTEGRATION=1 + DB)',
    value: { kind: 'path', path: 'test/integration' },
  },
  { title: 'benchmark — multi-process / spawn (slow)', value: { kind: 'path', path: 'test/benchmark' } },
  { title: '← Back', value: { kind: 'back' } },
];

const E2E_CHOICES = [
  { title: 'cache-load.e2e.js (mock)', value: { kind: 'file', file: 'test/cache-load.e2e.js' } },
  {
    title: 'metadata-reload.e2e.js (mock; run yarn build first)',
    value: { kind: 'file', file: 'test/metadata-reload.e2e.js' },
  },
  { title: 'flow-engine.e2e.js (mock)', value: { kind: 'file', file: 'test/flow-engine.e2e.js' } },
  { title: 'query-builder.e2e.js (.env + real DB)', value: { kind: 'file', file: 'test/query-builder.e2e.js' } },
  {
    title: 'Run mock chain: cache-load → metadata-reload → flow-engine',
    value: { kind: 'chain', chain: ['cache-load', 'metadata', 'flow'] },
  },
  { title: '← Back', value: { kind: 'back' } },
];

const CHAIN_FILES = {
  'cache-load': 'test/cache-load.e2e.js',
  metadata: 'test/metadata-reload.e2e.js',
  flow: 'test/flow-engine.e2e.js',
};

async function runE2eChain(names) {
  for (const name of names) {
    const file = CHAIN_FILES[name];
    const r = spawnSync(process.execPath, [path.join(root, file)], {
      cwd: root,
      stdio: 'inherit',
      env: process.env,
    });
    if (r.status !== 0) process.exit(r.status ?? 1);
  }
  process.exit(0);
}

async function main() {
  for (;;) {
    const { category } = await prompts(
      {
        type: 'select',
        name: 'category',
        message: 'Test runner — choose category (↑/↓, Enter)',
        choices: [
          { title: 'Jest — by module / folder', value: 'jest' },
          { title: 'E2E — Node .e2e.js scripts', value: 'e2e' },
          { title: 'bench:executor (worker RSS / memory, slow)', value: 'bench' },
          { title: 'Exit', value: 'exit' },
        ],
      },
      { onCancel: () => process.exit(0) },
    );

    if (!category || category === 'exit') {
      console.log('Goodbye.');
      process.exit(0);
    }

    if (category === 'bench') {
      const r = spawnSync('yarn', ['bench:executor'], {
        cwd: root,
        stdio: 'inherit',
        env: mergeNodeOptions(),
      });
      process.exit(r.status ?? 1);
    }

    if (category === 'jest') {
      const { pick } = await prompts(
        {
          type: 'select',
          name: 'pick',
          message: 'Jest — choose suite (↑/↓, Enter)',
          choices: JEST_CHOICES,
        },
        { onCancel: () => process.exit(0) },
      );
      if (!pick || pick.kind === 'back') continue;
      if (pick.kind === 'all') runYarnJest([]);
      runYarnJest([pick.path]);
    }

    if (category === 'e2e') {
      const { pick } = await prompts(
        {
          type: 'select',
          name: 'pick',
          message: 'E2E — choose script (↑/↓, Enter)',
          choices: E2E_CHOICES,
        },
        { onCancel: () => process.exit(0) },
      );
      if (!pick || pick.kind === 'back') continue;
      if (pick.kind === 'chain') await runE2eChain(pick.chain);
      runNode(pick.file);
    }
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
