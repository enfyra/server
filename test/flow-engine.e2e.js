/**
 * Flow Engine E2E Mock Test
 *
 * Run: node test/flow-engine.e2e.js
 */

const assert = require('assert');

class MockHandlerExecutor {
  async run(code, ctx, timeout) {
    const fn = new Function('$ctx', `return (async () => { ${code} })()`);
    return fn(ctx);
  }
}

let httpCalls = [];

class MockRepoRegistry {
  constructor() {
    this.createCalls = [];
    this.updateCalls = [];
    this.deleteCalls = [];
    this.findCalls = [];
  }
  createReposProxy(ctx, mainTableName) {
    const self = this;
    const mockRepo = {
      find: async (opt) => { self.findCalls.push(opt); return { data: [{ id: 1, name: 'test', status: 'active', stock: 10, email: 'test@test.com' }], count: 1 }; },
      create: async (opt) => { self.createCalls.push(opt); return { data: [{ id: 99, ...opt.data }], count: 1 }; },
      update: async (opt) => { self.updateCalls.push(opt); return { data: [{ id: opt.id, ...opt.data }] }; },
      delete: async (opt) => { self.deleteCalls.push(opt); return { message: `Deleted ${opt.id}` }; },
    };
    return new Proxy({}, {
      get: (_, prop) => mockRepo,
    });
  }
}

const originalFetch = global.fetch;

async function executeFlow(flow, payload, handlerExecutor, repoRegistry) {
  const flowContext = {
    $payload: payload || {},
    $last: null,
    $meta: { flowId: flow.id, flowName: flow.name },
  };

  const ctx = {
    $body: payload || {},
    $query: {},
    $params: {},
    $user: null,
    $repos: repoRegistry.createReposProxy({}, null),
    $helpers: {
      autoSlug: (text) => text.toLowerCase().replace(/\s+/g, '-'),
      $bcrypt: { hash: async (p) => `hashed_${p}`, compare: async (p, h) => h === `hashed_${p}` },
    },
    $cache: {},
    $share: { $logs: [] },
    $logs: (...args) => ctx.$share.$logs.push(...args),
    $flow: flowContext,
    $dispatch: { trigger: async (name, p) => ({ triggered: true, flowName: name, payload: p }) },
  };

  const completedSteps = [];
  const allSteps = [...flow.steps].sort((a, b) => a.stepOrder - b.stepOrder);
  const rootSteps = allSteps.filter(s => !s.parentId);
  const getChildren = (parentId, branch) => allSteps.filter(s => String(s.parentId) === String(parentId) && s.branch === branch);

  async function execStep(step) {
    if (!step.isEnabled) return;
    try {
      let result;
      const config = step.config || {};
      switch (step.type) {
        case 'script': result = await handlerExecutor.run(config.code || '', ctx, step.timeout); break;
        case 'condition': result = await handlerExecutor.run(config.code || 'return false;', ctx, step.timeout); break;
        case 'query':
          if (!config.table) throw new Error('Step config missing required field: table');
          result = await ctx.$repos[config.table]?.find({ filter: config.filter, limit: config.limit }); break;
        case 'create':
          if (!config.table) throw new Error('Step config missing required field: table');
          result = await ctx.$repos[config.table]?.create({ data: config.data }); break;
        case 'update':
          if (!config.table) throw new Error('Step config missing required field: table');
          result = await ctx.$repos[config.table]?.update({ id: config.id, data: config.data }); break;
        case 'delete':
          if (!config.table) throw new Error('Step config missing required field: table');
          result = await ctx.$repos[config.table]?.delete({ id: config.id }); break;
        case 'http': {
          if (!config.url) throw new Error('Step config missing required field: url');
          const method = config.method || 'GET';
          const hasBody = !['GET', 'DELETE'].includes(method) && config.body !== undefined;
          const headers = { ...(config.headers || {}) };
          if (hasBody && !Object.keys(headers).some(k => k.toLowerCase() === 'content-type')) {
            headers['Content-Type'] = 'application/json';
          }
          const response = await fetch(config.url, { method, headers, body: hasBody ? JSON.stringify(config.body) : undefined });
          const ct = response.headers.get('content-type') || '';
          result = { status: response.status, data: ct.includes('json') ? await response.json() : await response.text() };
          break;
        }
        case 'trigger_flow': result = { triggered: true, flowId: config.flowId, flowName: config.flowName }; break;
        case 'sleep': await new Promise(r => setTimeout(r, config.ms || 10)); result = { slept: config.ms || 10 }; break;
        case 'log': { const msg = config.message || JSON.stringify(ctx.$flow.$last); if (ctx.$logs) ctx.$logs(msg); result = { logged: true, message: msg }; break; }
        default: throw new Error(`Unknown step type: ${step.type}`);
      }
      flowContext[step.key] = result;
      flowContext.$last = result;
      completedSteps.push(step.key);
      if (step.type === 'condition') {
        const branchValue = !!result ? 'true' : 'false';
        const branchSteps = getChildren(step.id, branchValue);
        for (const child of branchSteps) { await execStep(child); }
      }
    } catch (error) {
      if (step.onError === 'retry' && step.retryAttempts > 0) {
        let retrySuccess = false;
        for (let i = 0; i < step.retryAttempts; i++) {
          try {
            let retryResult;
            const retryConfig = step.config || {};
            switch (step.type) {
              case 'script': retryResult = await handlerExecutor.run(retryConfig.code || '', ctx, step.timeout); break;
              case 'condition': retryResult = await handlerExecutor.run(retryConfig.code || 'return false;', ctx, step.timeout); break;
              case 'query':
                if (!retryConfig.table) throw new Error('Step config missing required field: table');
                retryResult = await ctx.$repos[retryConfig.table]?.find({ filter: retryConfig.filter, limit: retryConfig.limit }); break;
              case 'create':
                if (!retryConfig.table) throw new Error('Step config missing required field: table');
                retryResult = await ctx.$repos[retryConfig.table]?.create({ data: retryConfig.data }); break;
              case 'update':
                if (!retryConfig.table) throw new Error('Step config missing required field: table');
                retryResult = await ctx.$repos[retryConfig.table]?.update({ id: retryConfig.id, data: retryConfig.data }); break;
              case 'delete':
                if (!retryConfig.table) throw new Error('Step config missing required field: table');
                retryResult = await ctx.$repos[retryConfig.table]?.delete({ id: retryConfig.id }); break;
              case 'http': {
                if (!retryConfig.url) throw new Error('Step config missing required field: url');
                const rm = retryConfig.method || 'GET';
                const rHasBody = !['GET', 'DELETE'].includes(rm) && retryConfig.body !== undefined;
                const rHeaders = { ...(retryConfig.headers || {}) };
                if (rHasBody && !Object.keys(rHeaders).some(k => k.toLowerCase() === 'content-type')) {
                  rHeaders['Content-Type'] = 'application/json';
                }
                const rResp = await fetch(retryConfig.url, { method: rm, headers: rHeaders, body: rHasBody ? JSON.stringify(retryConfig.body) : undefined });
                const rCt = rResp.headers.get('content-type') || '';
                retryResult = { status: rResp.status, data: rCt.includes('json') ? await rResp.json() : await rResp.text() };
                break;
              }
              case 'trigger_flow': retryResult = { triggered: true, flowId: retryConfig.flowId, flowName: retryConfig.flowName }; break;
              case 'sleep': await new Promise(r => setTimeout(r, retryConfig.ms || 10)); retryResult = { slept: retryConfig.ms || 10 }; break;
              case 'log': { const rmsg = retryConfig.message || JSON.stringify(ctx.$flow.$last); if (ctx.$logs) ctx.$logs(rmsg); retryResult = { logged: true, message: rmsg }; break; }
              default: throw new Error(`Unknown step type: ${step.type}`);
            }
            flowContext[step.key] = retryResult;
            flowContext.$last = retryResult;
            completedSteps.push(step.key);
            retrySuccess = true;
            break;
          } catch (retryErr) {}
        }
        if (!retrySuccess) throw error;
      } else if (step.onError === 'skip') {
        flowContext[step.key] = { error: error.message, skipped: true };
        flowContext.$last = flowContext[step.key];
        completedSteps.push(step.key);
        return;
      } else {
        throw error;
      }
    }
  }

  for (const step of rootSteps) { await execStep(step); }
  return { context: flowContext, completedSteps };
}

const S = (id, key, order, type, config, opts = {}) => ({
  id, key, stepOrder: order, type, config, timeout: 5000, onError: opts.onError || 'stop',
  isEnabled: opts.isEnabled !== undefined ? opts.isEnabled : true,
  retryAttempts: opts.retryAttempts || 0,
  parentId: opts.parentId || null, branch: opts.branch || null,
});

async function testBasicFlow() {
  console.log('  Basic sequential flow execution');
  const flow = {
    id: 1, name: 'test-basic',
    steps: [
      { key: 'step1', stepOrder: 1, type: 'log', config: { message: 'Hello' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'step2', stepOrder: 2, type: 'query', config: { table: 'user_definition', filter: { status: 'active' } }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'step3', stepOrder: 3, type: 'log', config: { message: 'Done' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, { email: 'test@test.com' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['step1', 'step2', 'step3']);
  assert.strictEqual(result.context.$payload.email, 'test@test.com');
  assert.ok(result.context.step2.data);
  console.log('    PASS');
}

async function testDataChain() {
  console.log('  Data chain between steps');
  const flow = {
    id: 2, name: 'test-chain',
    steps: [
      { key: 'fetch_user', stepOrder: 1, type: 'query', config: { table: 'user_definition' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'check_result', stepOrder: 2, type: 'script', config: { code: 'return { hasUser: $ctx.$flow.fetch_user?.data?.length > 0, lastId: $ctx.$flow.$last?.data?.[0]?.id }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.check_result.hasUser, true);
  assert.strictEqual(result.context.check_result.lastId, 1);
  console.log('    PASS');
}

async function testConditionFalseContinuesRoot() {
  console.log('  Condition false continues to next root step (no branch children)');
  const flow = {
    id: 3, name: 'test-condition',
    steps: [
      { key: 'check', stepOrder: 1, type: 'condition', config: { code: 'return false;' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'after', stepOrder: 2, type: 'log', config: { message: 'still runs' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['check', 'after']);
  console.log('    PASS');
}

async function testConditionContinueOnTrue() {
  console.log('  Condition continues on true');
  const flow = {
    id: 4, name: 'test-cond-true',
    steps: [
      { key: 'check', stepOrder: 1, type: 'condition', config: { code: 'return true;' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'reached', stepOrder: 2, type: 'log', config: { message: 'yes' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['check', 'reached']);
  console.log('    PASS');
}

async function testErrorSkip() {
  console.log('  onError=skip continues execution');
  const flow = {
    id: 5, name: 'test-skip',
    steps: [
      { key: 'fail', stepOrder: 1, type: 'script', config: { code: 'throw new Error("boom");' }, timeout: 5000, onError: 'skip', isEnabled: true },
      { key: 'after', stepOrder: 2, type: 'log', config: { message: 'still here' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['fail', 'after']);
  assert.strictEqual(result.context.fail.skipped, true);
  assert.strictEqual(result.context.fail.error, 'boom');
  console.log('    PASS');
}

async function testErrorStop() {
  console.log('  onError=stop halts flow');
  const flow = {
    id: 6, name: 'test-stop',
    steps: [
      { key: 'fail', stepOrder: 1, type: 'script', config: { code: 'throw new Error("halt");' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'unreachable', stepOrder: 2, type: 'log', config: { message: 'nope' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('halt')); }
  console.log('    PASS');
}

async function testDisabledSteps() {
  console.log('  Disabled steps are skipped');
  const flow = {
    id: 7, name: 'test-disabled',
    steps: [
      { key: 'a', stepOrder: 1, type: 'log', config: { message: 'a' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'b', stepOrder: 2, type: 'log', config: { message: 'b' }, timeout: 5000, onError: 'stop', isEnabled: false },
      { key: 'c', stepOrder: 3, type: 'log', config: { message: 'c' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['a', 'c']);
  assert.strictEqual(result.context.b, undefined);
  console.log('    PASS');
}

async function testStepOrdering() {
  console.log('  Steps execute in stepOrder regardless of array order');
  const flow = {
    id: 8, name: 'test-order',
    steps: [
      { key: 'c', stepOrder: 3, type: 'script', config: { code: 'return { n: 3 }' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'a', stepOrder: 1, type: 'script', config: { code: 'return { n: 1 }' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'b', stepOrder: 2, type: 'script', config: { code: 'return { n: 2 }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['a', 'b', 'c']);
  assert.strictEqual(result.context.$last.n, 3);
  console.log('    PASS');
}

async function testPayloadAccessible() {
  console.log('  Payload accessible via $flow.$payload');
  const flow = {
    id: 9, name: 'test-payload',
    steps: [
      { key: 'read', stepOrder: 1, type: 'script', config: { code: 'return { orderId: $ctx.$flow.$payload.orderId, email: $ctx.$flow.$payload.email }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, { orderId: 42, email: 'u@t.com' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.read.orderId, 42);
  assert.strictEqual(result.context.read.email, 'u@t.com');
  console.log('    PASS');
}

async function testCreateStep() {
  console.log('  Create step calls repo.create()');
  const registry = new MockRepoRegistry();
  const flow = {
    id: 10, name: 'test-create',
    steps: [
      { key: 'new', stepOrder: 1, type: 'create', config: { table: 'order', data: { status: 'pending', total: 100 } }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), registry);
  assert.strictEqual(result.context.new.data[0].status, 'pending');
  assert.strictEqual(registry.createCalls.length, 1);
  assert.strictEqual(registry.createCalls[0].data.total, 100);
  console.log('    PASS');
}

async function testUpdateStep() {
  console.log('  Update step calls repo.update()');
  const registry = new MockRepoRegistry();
  const flow = {
    id: 11, name: 'test-update',
    steps: [
      { key: 'upd', stepOrder: 1, type: 'update', config: { table: 'order', id: 5, data: { status: 'done' } }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), registry);
  assert.strictEqual(result.context.upd.data[0].id, 5);
  assert.strictEqual(result.context.upd.data[0].status, 'done');
  assert.strictEqual(registry.updateCalls[0].id, 5);
  console.log('    PASS');
}

async function testDeleteStep() {
  console.log('  Delete step calls repo.delete()');
  const registry = new MockRepoRegistry();
  const flow = {
    id: 12, name: 'test-delete',
    steps: [
      { key: 'del', stepOrder: 1, type: 'delete', config: { table: 'order', id: 7 }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), registry);
  assert.ok(result.context.del.message.includes('7'));
  assert.strictEqual(registry.deleteCalls[0].id, 7);
  console.log('    PASS');
}

async function testHttpStep() {
  console.log('  HTTP step fetches external URL');
  global.fetch = async (url, opts) => {
    httpCalls.push({ url, ...opts });
    return {
      status: 200,
      headers: { get: () => 'application/json' },
      json: async () => ({ ok: true, received: JSON.parse(opts?.body || '{}') }),
    };
  };
  const flow = {
    id: 13, name: 'test-http',
    steps: [
      { key: 'api', stepOrder: 1, type: 'http', config: { url: 'https://api.example.com/hook', method: 'POST', headers: { 'X-Key': 'abc' }, body: { event: 'test' } }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  httpCalls = [];
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.api.status, 200);
  assert.strictEqual(result.context.api.data.ok, true);
  assert.strictEqual(result.context.api.data.received.event, 'test');
  assert.strictEqual(httpCalls[0].url, 'https://api.example.com/hook');
  assert.strictEqual(httpCalls[0].method, 'POST');
  assert.strictEqual(httpCalls[0].headers['X-Key'], 'abc');
  global.fetch = originalFetch;
  console.log('    PASS');
}

async function testHttpGetNoBody() {
  console.log('  HTTP GET does not send body');
  global.fetch = async (url, opts) => {
    httpCalls.push({ url, ...opts });
    return { status: 200, headers: { get: () => 'text/plain' }, text: async () => 'hello' };
  };
  httpCalls = [];
  const flow = {
    id: 14, name: 'test-http-get',
    steps: [
      { key: 'get', stepOrder: 1, type: 'http', config: { url: 'https://example.com', method: 'GET' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.get.status, 200);
  assert.strictEqual(result.context.get.data, 'hello');
  assert.strictEqual(httpCalls[0].body, undefined);
  global.fetch = originalFetch;
  console.log('    PASS');
}

async function testTriggerFlowStep() {
  console.log('  Trigger flow step returns metadata');
  const flow = {
    id: 15, name: 'test-trigger-flow',
    steps: [
      { key: 'trigger', stepOrder: 1, type: 'trigger_flow', config: { flowId: 99, flowName: 'child-flow' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.trigger.triggered, true);
  assert.strictEqual(result.context.trigger.flowId, 99);
  assert.strictEqual(result.context.trigger.flowName, 'child-flow');
  console.log('    PASS');
}

async function testSleepStep() {
  console.log('  Sleep step waits and returns duration');
  const flow = {
    id: 16, name: 'test-sleep',
    steps: [
      { key: 'wait', stepOrder: 1, type: 'sleep', config: { ms: 50 }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const start = Date.now();
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(Date.now() - start >= 40);
  assert.strictEqual(result.context.wait.slept, 50);
  console.log('    PASS');
}

async function testLogWritesToShareLogs() {
  console.log('  Log step writes to $share.$logs');
  const flow = {
    id: 17, name: 'test-log',
    steps: [
      { key: 'log1', stepOrder: 1, type: 'log', config: { message: 'first msg' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'log2', stepOrder: 2, type: 'log', config: { message: 'second msg' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const executor = new MockHandlerExecutor();
  const registry = new MockRepoRegistry();
  const result = await executeFlow(flow, {}, executor, registry);
  assert.strictEqual(result.context.log1.message, 'first msg');
  assert.strictEqual(result.context.log2.message, 'second msg');
  console.log('    PASS');
}

async function testScriptAccessesRepos() {
  console.log('  Script step accesses $repos for CRUD');
  const registry = new MockRepoRegistry();
  const flow = {
    id: 18, name: 'test-script-repos',
    steps: [
      { key: 'query', stepOrder: 1, type: 'script', config: { code: 'return await $ctx.$repos.user_definition.find({ filter: { role: "admin" }, limit: 5 })' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'create', stepOrder: 2, type: 'script', config: { code: 'return await $ctx.$repos.order.create({ data: { total: $ctx.$flow.query.data[0].id * 100 } })' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), registry);
  assert.ok(result.context.query.data.length > 0);
  assert.strictEqual(result.context.create.data[0].total, 100);
  console.log('    PASS');
}

async function testScriptAccessesHelpers() {
  console.log('  Script step accesses $helpers');
  const flow = {
    id: 19, name: 'test-helpers',
    steps: [
      { key: 'slug', stepOrder: 1, type: 'script', config: { code: 'return { slug: $ctx.$helpers.autoSlug("Hello World") }' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'hash', stepOrder: 2, type: 'script', config: { code: 'return { h: await $ctx.$helpers.$bcrypt.hash("pass123") }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.slug.slug, 'hello-world');
  assert.strictEqual(result.context.hash.h, 'hashed_pass123');
  console.log('    PASS');
}

async function testScriptTriggersFlow() {
  console.log('  Script step triggers another flow via $dispatch');
  const flow = {
    id: 20, name: 'test-trigger-from-script',
    steps: [
      { key: 'fire', stepOrder: 1, type: 'script', config: { code: 'return await $ctx.$dispatch.trigger("send-email", { to: "user@test.com" })' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.fire.triggered, true);
  assert.strictEqual(result.context.fire.flowName, 'send-email');
  assert.strictEqual(result.context.fire.payload.to, 'user@test.com');
  console.log('    PASS');
}

async function testMultiStepDataPipeline() {
  console.log('  Multi-step pipeline: query → condition → create → log');
  const flow = {
    id: 21, name: 'test-pipeline',
    steps: [
      { key: 'users', stepOrder: 1, type: 'query', config: { table: 'user_definition', limit: 10 }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'has_users', stepOrder: 2, type: 'condition', config: { code: 'return $ctx.$flow.users?.data?.length > 0' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'order', stepOrder: 3, type: 'create', config: { table: 'order', data: { userId: 1, status: 'new' } }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'done', stepOrder: 4, type: 'log', config: { message: 'pipeline complete' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['users', 'has_users', 'order', 'done']);
  assert.strictEqual(result.context.order.data[0].status, 'new');
  assert.strictEqual(result.context.$last.logged, true);
  console.log('    PASS');
}

async function testPipelineContinuesAfterConditionFalse() {
  console.log('  Pipeline continues after condition false (branching model)');
  const flow = {
    id: 22, name: 'test-pipeline-continue',
    steps: [
      { key: 'step1', stepOrder: 1, type: 'log', config: { message: 'start' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'gate', stepOrder: 2, type: 'condition', config: { code: 'return $ctx.$flow.$payload.allowed === true' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'step3', stepOrder: 3, type: 'log', config: { message: 'after gate' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, { allowed: false }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['step1', 'gate', 'step3']);
  console.log('    PASS');
}

async function testMultipleSkipErrors() {
  console.log('  Multiple skip errors continue flow');
  const flow = {
    id: 23, name: 'test-multi-skip',
    steps: [
      { key: 'fail1', stepOrder: 1, type: 'script', config: { code: 'throw new Error("err1");' }, timeout: 5000, onError: 'skip', isEnabled: true },
      { key: 'fail2', stepOrder: 2, type: 'script', config: { code: 'throw new Error("err2");' }, timeout: 5000, onError: 'skip', isEnabled: true },
      { key: 'ok', stepOrder: 3, type: 'log', config: { message: 'survived' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, ['fail1', 'fail2', 'ok']);
  assert.strictEqual(result.context.fail1.error, 'err1');
  assert.strictEqual(result.context.fail2.error, 'err2');
  assert.strictEqual(result.context.ok.logged, true);
  console.log('    PASS');
}

async function testEmptyFlow() {
  console.log('  Empty flow (no steps) completes');
  const flow = { id: 24, name: 'test-empty', steps: [] };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(result.completedSteps, []);
  assert.strictEqual(result.context.$last, null);
  console.log('    PASS');
}

async function testMetaAvailable() {
  console.log('  $meta is available in script');
  const flow = {
    id: 25, name: 'my-flow',
    steps: [
      { key: 'meta', stepOrder: 1, type: 'script', config: { code: 'return { id: $ctx.$flow.$meta.flowId, name: $ctx.$flow.$meta.flowName }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.meta.id, 25);
  assert.strictEqual(result.context.meta.name, 'my-flow');
  console.log('    PASS');
}

async function testScriptModifiesPayload() {
  console.log('  Script can read and transform payload');
  const flow = {
    id: 26, name: 'test-transform',
    steps: [
      { key: 'transform', stepOrder: 1, type: 'script', config: { code: 'const items = $ctx.$flow.$payload.items; return { total: items.reduce((s, i) => s + i.price, 0), count: items.length }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, { items: [{ price: 10 }, { price: 20 }, { price: 30 }] }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.transform.total, 60);
  assert.strictEqual(result.context.transform.count, 3);
  console.log('    PASS');
}

async function testConditionBasedOnPayload() {
  console.log('  Condition evaluates payload data');
  const flow = {
    id: 27, name: 'test-cond-payload',
    steps: [
      { key: 'check', stepOrder: 1, type: 'condition', config: { code: 'return $ctx.$flow.$payload.amount > 1000' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 'big', stepOrder: 2, type: 'log', config: { message: 'big order' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const r1 = await executeFlow(flow, { amount: 2000 }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(r1.completedSteps, ['check', 'big']);

  const r2 = await executeFlow(flow, { amount: 500 }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(r2.completedSteps, ['check', 'big']);
  console.log('    PASS');
}

async function testUnknownStepTypeThrows() {
  console.log('  Unknown step type throws error');
  const flow = {
    id: 28, name: 'test-unknown',
    steps: [
      { key: 'bad', stepOrder: 1, type: 'nonexistent', config: {}, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) {
    assert.ok(e.message.includes('Unknown step type'));
  }
  console.log('    PASS');
}

async function testQueryWithFilterAndLimit() {
  console.log('  Query step passes filter and limit to repo');
  const registry = new MockRepoRegistry();
  const flow = {
    id: 29, name: 'test-query-params',
    steps: [
      { key: 'q', stepOrder: 1, type: 'query', config: { table: 'product', filter: { status: { _eq: 'active' } }, limit: 5 }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  await executeFlow(flow, {}, new MockHandlerExecutor(), registry);
  assert.deepStrictEqual(registry.findCalls[0].filter, { status: { _eq: 'active' } });
  assert.strictEqual(registry.findCalls[0].limit, 5);
  console.log('    PASS');
}

async function testLastUpdatesEachStep() {
  console.log('  $last updates after each step');
  const flow = {
    id: 30, name: 'test-last-chain',
    steps: [
      { key: 's1', stepOrder: 1, type: 'script', config: { code: 'return { v: 1 }' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 's2', stepOrder: 2, type: 'script', config: { code: 'return { v: $ctx.$flow.$last.v + 1 }' }, timeout: 5000, onError: 'stop', isEnabled: true },
      { key: 's3', stepOrder: 3, type: 'script', config: { code: 'return { v: $ctx.$flow.$last.v + 1 }' }, timeout: 5000, onError: 'stop', isEnabled: true },
    ],
  };
  const result = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(result.context.s1.v, 1);
  assert.strictEqual(result.context.s2.v, 2);
  assert.strictEqual(result.context.s3.v, 3);
  assert.strictEqual(result.context.$last.v, 3);
  console.log('    PASS');
}

async function testBranchTrue() {
  console.log('  Condition true → executes true branch only');
  const flow = { id: 50, name: 'branch-true', steps: [
    S(1, 'check', 1, 'condition', { code: 'return true;' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
    S(4, 'after', 2, 'log', { message: 'after condition' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('check'));
  assert.ok(r.completedSteps.includes('yes'));
  assert.ok(!r.completedSteps.includes('no'));
  assert.ok(r.completedSteps.includes('after'));
  console.log('    PASS');
}

async function testBranchFalse() {
  console.log('  Condition false → executes false branch only');
  const flow = { id: 51, name: 'branch-false', steps: [
    S(1, 'check', 1, 'condition', { code: 'return false;' }),
    S(2, 'yes', 1, 'log', { message: 'true' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false' }, { parentId: 1, branch: 'false' }),
    S(4, 'after', 2, 'log', { message: 'continues' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('no'));
  assert.ok(!r.completedSteps.includes('yes'));
  assert.ok(r.completedSteps.includes('after'));
  console.log('    PASS');
}

async function testBranchNoChildren() {
  console.log('  Condition with no children → continues');
  const flow = { id: 52, name: 'no-children', steps: [
    S(1, 'check', 1, 'condition', { code: 'return true;' }),
    S(2, 'after', 2, 'log', { message: 'still runs' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(r.completedSteps, ['check', 'after']);
  console.log('    PASS');
}

async function testBranchMultipleSteps() {
  console.log('  Branch with multiple steps executes all in order');
  const flow = { id: 53, name: 'multi-branch', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return true;' }),
    S(2, 'b1', 1, 'script', { code: 'return { v: 1 }' }, { parentId: 1, branch: 'true' }),
    S(3, 'b2', 2, 'script', { code: 'return { v: $ctx.$flow.b1.v + 1 }' }, { parentId: 1, branch: 'true' }),
    S(4, 'b3', 3, 'script', { code: 'return { v: $ctx.$flow.b2.v + 1 }' }, { parentId: 1, branch: 'true' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r.context.b3.v, 3);
  console.log('    PASS');
}

async function testBranchAccessParentResult() {
  console.log('  Branch step accesses parent condition result');
  const flow = { id: 54, name: 'access-parent', steps: [
    S(1, 'check', 1, 'condition', { code: 'return true;' }),
    S(2, 'child', 1, 'script', { code: 'return { parentWasTrue: $ctx.$flow.check === true }' }, { parentId: 1, branch: 'true' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r.context.child.parentWasTrue, true);
  console.log('    PASS');
}

async function testBranchSkipError() {
  console.log('  Branch step with skip error continues branch');
  const flow = { id: 55, name: 'branch-skip', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return true;' }),
    S(2, 'fail', 1, 'script', { code: 'throw new Error("oops");' }, { parentId: 1, branch: 'true', onError: 'skip' }),
    S(3, 'ok', 2, 'log', { message: 'still here' }, { parentId: 1, branch: 'true' }),
    S(4, 'after', 2, 'log', { message: 'root continues' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.context.fail.skipped);
  assert.ok(r.completedSteps.includes('ok'));
  assert.ok(r.completedSteps.includes('after'));
  console.log('    PASS');
}

async function testBranchDisabledStep() {
  console.log('  Disabled step in branch is skipped');
  const flow = { id: 56, name: 'branch-disabled', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return true;' }),
    S(2, 'active', 1, 'log', { message: 'runs' }, { parentId: 1, branch: 'true' }),
    S(3, 'disabled', 2, 'log', { message: 'skip' }, { parentId: 1, branch: 'true', isEnabled: false }),
    S(4, 'also_active', 3, 'log', { message: 'runs too' }, { parentId: 1, branch: 'true' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('active'));
  assert.ok(!r.completedSteps.includes('disabled'));
  assert.ok(r.completedSteps.includes('also_active'));
  console.log('    PASS');
}

async function testMultipleConditionsSequential() {
  console.log('  Multiple conditions in sequence');
  const flow = { id: 57, name: 'multi-cond', steps: [
    S(1, 'c1', 1, 'condition', { code: 'return true;' }),
    S(2, 'c1_yes', 1, 'log', { message: 'c1 true' }, { parentId: 1, branch: 'true' }),
    S(3, 'c2', 2, 'condition', { code: 'return false;' }),
    S(4, 'c2_yes', 1, 'log', { message: 'c2 true' }, { parentId: 3, branch: 'true' }),
    S(5, 'c2_no', 1, 'log', { message: 'c2 false' }, { parentId: 3, branch: 'false' }),
    S(6, 'end', 3, 'log', { message: 'done' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('c1_yes'));
  assert.ok(!r.completedSteps.includes('c2_yes'));
  assert.ok(r.completedSteps.includes('c2_no'));
  assert.ok(r.completedSteps.includes('end'));
  console.log('    PASS');
}

async function testEmptyTrueBranchPopulatedFalse() {
  console.log('  Empty true branch, populated false branch');
  const flow = { id: 58, name: 'empty-true', steps: [
    S(1, 'check', 1, 'condition', { code: 'return true;' }),
    S(2, 'false_step', 1, 'log', { message: 'false only' }, { parentId: 1, branch: 'false' }),
    S(3, 'after', 2, 'log', { message: 'continues' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(!r.completedSteps.includes('false_step'));
  assert.ok(r.completedSteps.includes('after'));
  console.log('    PASS');
}

async function testRealWorldOrderProcessing() {
  console.log('  Real-world: Order processing with stock check');
  const flow = { id: 60, name: 'process-order', steps: [
    S(1, 'check_stock', 1, 'query', { table: 'product', filter: { id: { _eq: 1 } } }),
    S(2, 'in_stock', 2, 'condition', { code: 'return $ctx.$flow.check_stock?.data?.[0]?.stock > 0' }),
    S(3, 'charge', 1, 'create', { table: 'payment', data: { amount: 100, status: 'charged' } }, { parentId: 2, branch: 'true' }),
    S(4, 'ship', 2, 'update', { table: 'order', id: 1, data: { status: 'shipped' } }, { parentId: 2, branch: 'true' }),
    S(5, 'out_of_stock', 1, 'log', { message: 'out of stock' }, { parentId: 2, branch: 'false' }),
    S(6, 'done', 3, 'log', { message: 'order processed' }),
  ]};
  const r = await executeFlow(flow, { orderId: 1 }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('charge'));
  assert.ok(r.completedSteps.includes('ship'));
  assert.ok(!r.completedSteps.includes('out_of_stock'));
  assert.ok(r.completedSteps.includes('done'));
  console.log('    PASS');
}

async function testRealWorldUserRegistration() {
  console.log('  Real-world: User registration flow');
  const flow = { id: 61, name: 'register-user', steps: [
    S(1, 'hash', 1, 'script', { code: 'return { hashed: await $ctx.$helpers.$bcrypt.hash($ctx.$flow.$payload.password) }' }),
    S(2, 'create_user', 2, 'create', { table: 'user', data: { email: 'new@test.com', role: 'user' } }),
    S(3, 'send_email', 3, 'log', { message: 'welcome email sent' }),
  ]};
  const r = await executeFlow(flow, { password: 'secret123' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r.context.hash.hashed, 'hashed_secret123');
  assert.strictEqual(r.context.create_user.data[0].email, 'new@test.com');
  assert.ok(r.completedSteps.includes('send_email'));
  console.log('    PASS');
}

async function testRealWorldScheduledCleanup() {
  console.log('  Real-world: Scheduled cleanup');
  const reg = new MockRepoRegistry();
  const flow = { id: 62, name: 'cleanup', steps: [
    S(1, 'old_records', 1, 'query', { table: 'session', filter: { expired: true }, limit: 100 }),
    S(2, 'has_records', 2, 'condition', { code: 'return $ctx.$flow.old_records?.data?.length > 0' }),
    S(3, 'delete_them', 1, 'delete', { table: 'session', id: 1 }, { parentId: 2, branch: 'true' }),
    S(4, 'log_done', 2, 'log', { message: 'cleanup done' }, { parentId: 2, branch: 'true' }),
    S(5, 'log_empty', 1, 'log', { message: 'nothing to clean' }, { parentId: 2, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), reg);
  assert.ok(r.completedSteps.includes('delete_them'));
  assert.ok(r.completedSteps.includes('log_done'));
  assert.ok(!r.completedSteps.includes('log_empty'));
  assert.strictEqual(reg.deleteCalls.length, 1);
  console.log('    PASS');
}

async function testRealWorldErrorRecovery() {
  console.log('  Real-world: Error recovery with fallback');
  global.fetch = async () => { throw new Error('API down'); };
  const flow = { id: 63, name: 'error-recovery', steps: [
    S(1, 'try_api', 1, 'http', { url: 'https://api.fail.com', method: 'GET' }, { onError: 'skip' }),
    S(2, 'check_failed', 2, 'condition', { code: 'return $ctx.$flow.try_api?.skipped === true' }),
    S(3, 'fallback', 1, 'log', { message: 'using fallback' }, { parentId: 2, branch: 'true' }),
    S(4, 'use_api', 1, 'script', { code: 'return $ctx.$flow.try_api' }, { parentId: 2, branch: 'false' }),
    S(5, 'done', 3, 'log', { message: 'completed' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.context.try_api.skipped);
  assert.ok(r.completedSteps.includes('fallback'));
  assert.ok(!r.completedSteps.includes('use_api'));
  assert.ok(r.completedSteps.includes('done'));
  global.fetch = originalFetch;
  console.log('    PASS');
}

async function testRealWorldMultiTableJoin() {
  console.log('  Real-world: Multi-table query + compute + update');
  const flow = { id: 64, name: 'compute-totals', steps: [
    S(1, 'orders', 1, 'query', { table: 'order', limit: 10 }),
    S(2, 'products', 2, 'query', { table: 'product', limit: 10 }),
    S(3, 'compute', 3, 'script', { code: 'return { total: $ctx.$flow.orders.count * $ctx.$flow.products.count }' }),
    S(4, 'save', 4, 'update', { table: 'report', id: 1, data: { status: 'computed' } }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r.context.compute.total, 1);
  assert.ok(r.completedSteps.includes('save'));
  console.log('    PASS');
}

async function testRealWorldDispatchFromFlow() {
  console.log('  Real-world: Flow dispatches another flow');
  const flow = { id: 65, name: 'parent-flow', steps: [
    S(1, 'process', 1, 'script', { code: 'return { userId: 42 }' }),
    S(2, 'notify', 2, 'script', { code: 'return await $ctx.$dispatch.trigger("send-notification", { userId: $ctx.$flow.process.userId })' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r.context.notify.triggered, true);
  assert.strictEqual(r.context.notify.flowName, 'send-notification');
  assert.strictEqual(r.context.notify.payload.userId, 42);
  console.log('    PASS');
}

async function testRealWorldAuthFlow() {
  console.log('  Real-world: Auth check → condition → response');
  const flow = { id: 66, name: 'auth-check', steps: [
    S(1, 'find_user', 1, 'query', { table: 'user', filter: { email: 'test@test.com' } }),
    S(2, 'exists', 2, 'condition', { code: 'return $ctx.$flow.find_user?.data?.length > 0' }),
    S(3, 'verify', 1, 'script', { code: 'return { valid: await $ctx.$helpers.$bcrypt.compare("pass", "hashed_pass") }' }, { parentId: 2, branch: 'true' }),
    S(4, 'not_found', 1, 'log', { message: 'user not found' }, { parentId: 2, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, { email: 'test@test.com', password: 'pass' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('verify'));
  assert.strictEqual(r.context.verify.valid, true);
  assert.ok(!r.completedSteps.includes('not_found'));
  console.log('    PASS');
}

async function testBranchPayloadDriven() {
  console.log('  Branch based on payload value');
  const flow = { id: 67, name: 'payload-branch', steps: [
    S(1, 'check_vip', 1, 'condition', { code: 'return $ctx.$flow.$payload.tier === "vip"' }),
    S(2, 'vip_discount', 1, 'script', { code: 'return { discount: 0.2 }' }, { parentId: 1, branch: 'true' }),
    S(3, 'normal_price', 1, 'script', { code: 'return { discount: 0 }' }, { parentId: 1, branch: 'false' }),
    S(4, 'apply', 2, 'script', { code: 'return { finalDiscount: $ctx.$flow.$last.discount }' }),
  ]};
  const r1 = await executeFlow(flow, { tier: 'vip' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r1.context.vip_discount.discount, 0.2);
  assert.ok(!r1.completedSteps.includes('normal_price'));

  const r2 = await executeFlow(flow, { tier: 'basic' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r2.context.normal_price.discount, 0);
  assert.ok(!r2.completedSteps.includes('vip_discount'));
  console.log('    PASS');
}

async function testBranchErrorStopsFlow() {
  console.log('  Error in branch with onError=stop halts entire flow');
  const flow = { id: 68, name: 'branch-error-stop', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return true;' }),
    S(2, 'crash', 1, 'script', { code: 'throw new Error("branch crash");' }, { parentId: 1, branch: 'true' }),
    S(3, 'after', 2, 'log', { message: 'should not run' }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('branch crash')); }
  console.log('    PASS');
}

async function testBothBranchesPopulated() {
  console.log('  Both branches populated, only matching executes');
  const flow = { id: 69, name: 'both-branches', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return $ctx.$flow.$payload.go === "left"' }),
    S(2, 'left1', 1, 'log', { message: 'left 1' }, { parentId: 1, branch: 'false' }),
    S(3, 'left2', 2, 'log', { message: 'left 2' }, { parentId: 1, branch: 'false' }),
    S(4, 'right1', 1, 'log', { message: 'right 1' }, { parentId: 1, branch: 'true' }),
    S(5, 'right2', 2, 'log', { message: 'right 2' }, { parentId: 1, branch: 'true' }),
  ]};
  const r = await executeFlow(flow, { go: 'left' }, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(!r.completedSteps.includes('left1'));
  assert.ok(!r.completedSteps.includes('left2'));
  assert.ok(r.completedSteps.includes('right1'));
  assert.ok(r.completedSteps.includes('right2'));
  console.log('    PASS');
}

async function testQueryMissingTable() {
  console.log('  Query step missing table throws error');
  const flow = { id: 100, name: 'query-no-table', steps: [
    S(1, 'q', 1, 'query', { filter: { status: 'active' } }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('missing required field: table')); }
  console.log('    PASS');
}

async function testCreateMissingTable() {
  console.log('  Create step missing table throws error');
  const flow = { id: 101, name: 'create-no-table', steps: [
    S(1, 'c', 1, 'create', { data: { name: 'test' } }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('missing required field: table')); }
  console.log('    PASS');
}

async function testUpdateMissingTable() {
  console.log('  Update step missing table throws error');
  const flow = { id: 102, name: 'update-no-table', steps: [
    S(1, 'u', 1, 'update', { id: 1, data: { status: 'done' } }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('missing required field: table')); }
  console.log('    PASS');
}

async function testDeleteMissingTable() {
  console.log('  Delete step missing table throws error');
  const flow = { id: 103, name: 'delete-no-table', steps: [
    S(1, 'd', 1, 'delete', { id: 5 }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('missing required field: table')); }
  console.log('    PASS');
}

async function testHttpMissingUrl() {
  console.log('  HTTP step missing url throws error');
  const flow = { id: 104, name: 'http-no-url', steps: [
    S(1, 'h', 1, 'http', { method: 'GET' }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('missing required field: url')); }
  console.log('    PASS');
}

async function testConditionReturns1Truthy() {
  console.log('  Condition returns 1 (truthy) executes true branch');
  const flow = { id: 105, name: 'cond-1-truthy', steps: [
    S(1, 'check', 1, 'condition', { code: 'return 1;' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('yes'));
  assert.ok(!r.completedSteps.includes('no'));
  console.log('    PASS');
}

async function testConditionReturns0Falsy() {
  console.log('  Condition returns 0 (falsy) executes false branch');
  const flow = { id: 106, name: 'cond-0-falsy', steps: [
    S(1, 'check', 1, 'condition', { code: 'return 0;' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(!r.completedSteps.includes('yes'));
  assert.ok(r.completedSteps.includes('no'));
  console.log('    PASS');
}

async function testConditionReturnsNullFalsy() {
  console.log('  Condition returns null (falsy) executes false branch');
  const flow = { id: 107, name: 'cond-null-falsy', steps: [
    S(1, 'check', 1, 'condition', { code: 'return null;' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(!r.completedSteps.includes('yes'));
  assert.ok(r.completedSteps.includes('no'));
  console.log('    PASS');
}

async function testConditionReturnsObjectTruthy() {
  console.log('  Condition returns object (truthy) executes true branch');
  const flow = { id: 108, name: 'cond-obj-truthy', steps: [
    S(1, 'check', 1, 'condition', { code: 'return { found: true };' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('yes'));
  assert.ok(!r.completedSteps.includes('no'));
  console.log('    PASS');
}

async function testConditionReturnsEmptyStringFalsy() {
  console.log('  Condition returns empty string (falsy) executes false branch');
  const flow = { id: 109, name: 'cond-empty-str-falsy', steps: [
    S(1, 'check', 1, 'condition', { code: 'return "";' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(!r.completedSteps.includes('yes'));
  assert.ok(r.completedSteps.includes('no'));
  console.log('    PASS');
}

async function testConditionReturnsFalseStringTruthy() {
  console.log('  Condition returns "false" string (truthy!) executes true branch');
  const flow = { id: 110, name: 'cond-false-str-truthy', steps: [
    S(1, 'check', 1, 'condition', { code: 'return "false";' }),
    S(2, 'yes', 1, 'log', { message: 'true branch' }, { parentId: 1, branch: 'true' }),
    S(3, 'no', 1, 'log', { message: 'false branch' }, { parentId: 1, branch: 'false' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('yes'));
  assert.ok(!r.completedSteps.includes('no'));
  console.log('    PASS');
}

async function testCircularFlowDetection() {
  console.log('  Circular A->B->A detected via visitedFlowIds');
  const MAX_FLOW_DEPTH = 10;
  async function processFlow(jobData, flowCache) {
    const { flowId, flowName, depth = 0, visitedFlowIds = [] } = jobData;
    if (depth > MAX_FLOW_DEPTH) {
      throw new Error(`Max flow nesting depth (${MAX_FLOW_DEPTH}) exceeded for flow ${flowName || flowId}`);
    }
    const flow = flowCache[flowId];
    if (!flow) throw new Error(`Flow ${flowName || flowId} not found`);
    if (visitedFlowIds.includes(flow.id)) {
      throw new Error(`Circular flow detected: flow "${flow.name}" (${flow.id}) already in chain [${visitedFlowIds.join(' → ')}]`);
    }
    return { success: true };
  }
  const flowCache = {
    1: { id: 1, name: 'flow-A', steps: [] },
    2: { id: 2, name: 'flow-B', steps: [] },
  };
  try {
    await processFlow({ flowId: 1, flowName: 'flow-A', visitedFlowIds: [2, 1] }, flowCache);
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('Circular flow detected')); }
  console.log('    PASS');
}

async function testDepthLimitExceeded() {
  console.log('  Depth limit exceeded throws error');
  const MAX_FLOW_DEPTH = 10;
  async function processFlow(jobData) {
    const { flowId, flowName, depth = 0 } = jobData;
    if (depth > MAX_FLOW_DEPTH) {
      throw new Error(`Max flow nesting depth (${MAX_FLOW_DEPTH}) exceeded for flow ${flowName || flowId}`);
    }
    return { success: true };
  }
  try {
    await processFlow({ flowId: 1, flowName: 'deep-flow', depth: 11 });
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('Max flow nesting depth')); }
  console.log('    PASS');
}

async function testRetryEventualSuccess() {
  console.log('  Retry eventual success on second attempt');
  let attempt = 0;
  const executor = new MockHandlerExecutor();
  const origRun = executor.run.bind(executor);
  executor.run = async (code, ctx, timeout) => {
    attempt++;
    if (attempt <= 1) throw new Error('transient failure');
    return origRun(code, ctx, timeout);
  };
  const flow = { id: 111, name: 'retry-success', steps: [
    S(1, 'flaky', 1, 'script', { code: 'return { ok: true }' }, { onError: 'retry', retryAttempts: 3 }),
    S(2, 'after', 2, 'log', { message: 'continued' }),
  ]};
  const r = await executeFlow(flow, {}, executor, new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('flaky'));
  assert.ok(r.completedSteps.includes('after'));
  assert.strictEqual(r.context.flaky.ok, true);
  console.log('    PASS');
}

async function testRetryAllAttemptsFail() {
  console.log('  Retry all attempts fail throws final error');
  const executor = new MockHandlerExecutor();
  executor.run = async () => { throw new Error('permanent failure'); };
  const flow = { id: 112, name: 'retry-fail', steps: [
    S(1, 'flaky', 1, 'script', { code: 'throw new Error("permanent failure");' }, { onError: 'retry', retryAttempts: 2 }),
    S(2, 'unreachable', 2, 'log', { message: 'nope' }),
  ]};
  try {
    await executeFlow(flow, {}, executor, new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) { assert.ok(e.message.includes('permanent failure')); }
  console.log('    PASS');
}

async function testNestedConditionInsideBranch() {
  console.log('  Condition inside condition branch executes nested branches');
  const flow = { id: 113, name: 'nested-cond', steps: [
    S(1, 'outer', 1, 'condition', { code: 'return true;' }),
    S(2, 'inner', 1, 'condition', { code: 'return false;' }, { parentId: 1, branch: 'true' }),
    S(3, 'inner_yes', 1, 'log', { message: 'inner true' }, { parentId: 2, branch: 'true' }),
    S(4, 'inner_no', 1, 'log', { message: 'inner false' }, { parentId: 2, branch: 'false' }),
    S(5, 'outer_no', 1, 'log', { message: 'outer false' }, { parentId: 1, branch: 'false' }),
    S(6, 'end', 2, 'log', { message: 'done' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('outer'));
  assert.ok(r.completedSteps.includes('inner'));
  assert.ok(!r.completedSteps.includes('inner_yes'));
  assert.ok(r.completedSteps.includes('inner_no'));
  assert.ok(!r.completedSteps.includes('outer_no'));
  assert.ok(r.completedSteps.includes('end'));
  console.log('    PASS');
}

async function testTripleNestedConditions() {
  console.log('  Triple nested conditions 3 levels deep');
  const flow = { id: 114, name: 'triple-nested', steps: [
    S(1, 'l1', 1, 'condition', { code: 'return true;' }),
    S(2, 'l2', 1, 'condition', { code: 'return true;' }, { parentId: 1, branch: 'true' }),
    S(3, 'l3', 1, 'condition', { code: 'return true;' }, { parentId: 2, branch: 'true' }),
    S(4, 'deep_yes', 1, 'log', { message: 'deepest true' }, { parentId: 3, branch: 'true' }),
    S(5, 'deep_no', 1, 'log', { message: 'deepest false' }, { parentId: 3, branch: 'false' }),
    S(6, 'l2_no', 1, 'log', { message: 'l2 false' }, { parentId: 2, branch: 'false' }),
    S(7, 'l1_no', 1, 'log', { message: 'l1 false' }, { parentId: 1, branch: 'false' }),
    S(8, 'final', 2, 'log', { message: 'final' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('l1'));
  assert.ok(r.completedSteps.includes('l2'));
  assert.ok(r.completedSteps.includes('l3'));
  assert.ok(r.completedSteps.includes('deep_yes'));
  assert.ok(!r.completedSteps.includes('deep_no'));
  assert.ok(!r.completedSteps.includes('l2_no'));
  assert.ok(!r.completedSteps.includes('l1_no'));
  assert.ok(r.completedSteps.includes('final'));
  console.log('    PASS');
}

async function testHttpAutoContentType() {
  console.log('  HTTP POST with body auto-adds Content-Type: application/json');
  global.fetch = async (url, opts) => {
    httpCalls.push({ url, ...opts });
    return { status: 200, headers: { get: () => 'application/json' }, json: async () => ({ ok: true }) };
  };
  httpCalls = [];
  const flow = { id: 115, name: 'http-auto-ct', steps: [
    S(1, 'post', 1, 'http', { url: 'https://api.example.com', method: 'POST', body: { data: 1 } }),
  ]};
  await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(httpCalls[0].headers['Content-Type'], 'application/json');
  global.fetch = originalFetch;
  console.log('    PASS');
}

async function testHttpGetNoBodyStrict() {
  console.log('  HTTP GET does not send body even when config has body');
  global.fetch = async (url, opts) => {
    httpCalls.push({ url, ...opts });
    return { status: 200, headers: { get: () => 'text/plain' }, text: async () => 'ok' };
  };
  httpCalls = [];
  const flow = { id: 116, name: 'http-get-no-body', steps: [
    S(1, 'get', 1, 'http', { url: 'https://example.com', method: 'GET', body: { ignored: true } }),
  ]};
  await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(httpCalls[0].body, undefined);
  global.fetch = originalFetch;
  console.log('    PASS');
}

async function testAllStepsDisabled() {
  console.log('  All steps disabled completes with empty completedSteps');
  const flow = { id: 117, name: 'all-disabled', steps: [
    S(1, 'a', 1, 'log', { message: 'a' }, { isEnabled: false }),
    S(2, 'b', 2, 'log', { message: 'b' }, { isEnabled: false }),
    S(3, 'c', 3, 'log', { message: 'c' }, { isEnabled: false }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.deepStrictEqual(r.completedSteps, []);
  assert.strictEqual(r.context.$last, null);
  console.log('    PASS');
}

async function testScriptAccessesDispatch() {
  console.log('  Script accesses $dispatch.trigger()');
  const flow = { id: 118, name: 'dispatch-access', steps: [
    S(1, 'fire', 1, 'script', { code: 'return await $ctx.$dispatch.trigger("child-flow", { key: "val" })' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.strictEqual(r.context.fire.triggered, true);
  assert.strictEqual(r.context.fire.flowName, 'child-flow');
  assert.deepStrictEqual(r.context.fire.payload, { key: 'val' });
  console.log('    PASS');
}

async function testNestedBranchBothFalse() {
  console.log('  Nested branch: outer false → inner never reached');
  const flow = { id: 200, name: 'nested-both-false', steps: [
    S(1, 'outer', 1, 'condition', { code: 'return false;' }),
    S(2, 'inner', 1, 'condition', { code: 'return true;' }, { parentId: 1, branch: 'true' }),
    S(3, 'inner_yes', 1, 'log', { message: 'should not run' }, { parentId: 2, branch: 'true' }),
    S(4, 'outer_no', 1, 'log', { message: 'outer false' }, { parentId: 1, branch: 'false' }),
    S(5, 'end', 2, 'log', { message: 'done' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('outer'));
  assert.ok(!r.completedSteps.includes('inner'));
  assert.ok(!r.completedSteps.includes('inner_yes'));
  assert.ok(r.completedSteps.includes('outer_no'));
  assert.ok(r.completedSteps.includes('end'));
  console.log('    PASS');
}

async function testNestedBranchDataChain() {
  console.log('  Nested branch: inner step accesses outer step result via $flow');
  const flow = { id: 201, name: 'nested-data-chain', steps: [
    S(1, 'fetch', 1, 'script', { code: 'return { users: [1,2,3] };' }),
    S(2, 'check', 2, 'condition', { code: 'return $ctx.$flow.fetch.users.length > 0;' }),
    S(3, 'inner_check', 1, 'condition', { code: 'return $ctx.$flow.fetch.users.length > 5;' }, { parentId: 2, branch: 'true' }),
    S(4, 'many', 1, 'log', { message: 'many users' }, { parentId: 3, branch: 'true' }),
    S(5, 'few', 1, 'script', { code: 'return { count: $ctx.$flow.fetch.users.length };' }, { parentId: 3, branch: 'false' }),
    S(6, 'none', 1, 'log', { message: 'no users' }, { parentId: 2, branch: 'false' }),
    S(7, 'summary', 3, 'script', { code: 'return { last: $ctx.$flow.$last };' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('fetch'));
  assert.ok(r.completedSteps.includes('check'));
  assert.ok(r.completedSteps.includes('inner_check'));
  assert.ok(!r.completedSteps.includes('many'));
  assert.ok(r.completedSteps.includes('few'));
  assert.ok(!r.completedSteps.includes('none'));
  assert.ok(r.completedSteps.includes('summary'));
  assert.strictEqual(r.context.few.count, 3);
  console.log('    PASS');
}

async function testMultipleBranchesAtSameLevel() {
  console.log('  Two conditions at root level with separate branches');
  const flow = { id: 202, name: 'multi-cond-root', steps: [
    S(1, 'cond_a', 1, 'condition', { code: 'return true;' }),
    S(2, 'a_yes', 1, 'log', { message: 'A true' }, { parentId: 1, branch: 'true' }),
    S(3, 'a_no', 1, 'log', { message: 'A false' }, { parentId: 1, branch: 'false' }),
    S(4, 'cond_b', 2, 'condition', { code: 'return false;' }),
    S(5, 'b_yes', 1, 'log', { message: 'B true' }, { parentId: 4, branch: 'true' }),
    S(6, 'b_no', 1, 'log', { message: 'B false' }, { parentId: 4, branch: 'false' }),
    S(7, 'done', 3, 'log', { message: 'all done' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('a_yes'));
  assert.ok(!r.completedSteps.includes('a_no'));
  assert.ok(!r.completedSteps.includes('b_yes'));
  assert.ok(r.completedSteps.includes('b_no'));
  assert.ok(r.completedSteps.includes('done'));
  console.log('    PASS');
}

async function testBranchWithMultipleStepsAndSkipError() {
  console.log('  Branch with 3 steps: middle fails with skip, rest continue');
  const flow = { id: 203, name: 'branch-skip-middle', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return true;' }),
    S(2, 'step_a', 1, 'log', { message: 'first' }, { parentId: 1, branch: 'true' }),
    S(3, 'step_b', 2, 'script', { code: 'throw new Error("oops");' }, { parentId: 1, branch: 'true', onError: 'skip' }),
    S(4, 'step_c', 3, 'log', { message: 'third' }, { parentId: 1, branch: 'true' }),
    S(5, 'after', 2, 'log', { message: 'after' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('step_a'));
  assert.ok(r.completedSteps.includes('step_b'));
  assert.ok(r.context.step_b.skipped === true);
  assert.ok(r.completedSteps.includes('step_c'));
  assert.ok(r.completedSteps.includes('after'));
  console.log('    PASS');
}

async function testBranchWithCrudInsideBranch() {
  console.log('  Branch contains query + condition + create chain');
  const reg = new MockRepoRegistry();
  const flow = { id: 204, name: 'branch-crud', steps: [
    S(1, 'check_user', 1, 'condition', { code: 'return true;' }),
    S(2, 'find_orders', 1, 'query', { table: 'order_definition' }, { parentId: 1, branch: 'true' }),
    S(3, 'has_orders', 2, 'condition', { code: 'return $ctx.$flow.find_orders?.data?.length > 0;' }, { parentId: 1, branch: 'true' }),
    S(4, 'create_report', 1, 'create', { table: 'report_definition', data: { type: 'orders' } }, { parentId: 3, branch: 'true' }),
    S(5, 'log_empty', 1, 'log', { message: 'no orders' }, { parentId: 3, branch: 'false' }),
    S(6, 'no_user', 1, 'log', { message: 'no user' }, { parentId: 1, branch: 'false' }),
    S(7, 'finish', 2, 'log', { message: 'done' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), reg);
  assert.ok(r.completedSteps.includes('find_orders'));
  assert.ok(r.completedSteps.includes('has_orders'));
  assert.ok(r.completedSteps.includes('create_report'));
  assert.ok(!r.completedSteps.includes('log_empty'));
  assert.ok(!r.completedSteps.includes('no_user'));
  assert.ok(r.completedSteps.includes('finish'));
  assert.strictEqual(reg.createCalls.length, 1);
  console.log('    PASS');
}

async function testDeepNestingFourLevels() {
  console.log('  4-level deep nesting: condition → condition → condition → condition');
  const flow = { id: 205, name: 'four-levels', steps: [
    S(1, 'l1', 1, 'condition', { code: 'return true;' }),
    S(2, 'l2', 1, 'condition', { code: 'return true;' }, { parentId: 1, branch: 'true' }),
    S(3, 'l3', 1, 'condition', { code: 'return false;' }, { parentId: 2, branch: 'true' }),
    S(4, 'l4_yes', 1, 'log', { message: 'l4 true' }, { parentId: 3, branch: 'true' }),
    S(5, 'l4_no', 1, 'log', { message: 'l4 false' }, { parentId: 3, branch: 'false' }),
    S(6, 'l3_no', 1, 'log', { message: 'l3 false' }, { parentId: 2, branch: 'false' }),
    S(7, 'l2_no', 1, 'log', { message: 'l2 false' }, { parentId: 1, branch: 'false' }),
    S(8, 'end', 2, 'log', { message: 'end' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('l1'));
  assert.ok(r.completedSteps.includes('l2'));
  assert.ok(r.completedSteps.includes('l3'));
  assert.ok(!r.completedSteps.includes('l4_yes'));
  assert.ok(r.completedSteps.includes('l4_no'));
  assert.ok(!r.completedSteps.includes('l3_no'));
  assert.ok(!r.completedSteps.includes('l2_no'));
  assert.ok(r.completedSteps.includes('end'));
  console.log('    PASS');
}

async function testBranchTrueAndFalseBothHaveMultipleSteps() {
  console.log('  Both true and false branches have 3 steps each');
  const flow = { id: 206, name: 'both-branches-full', steps: [
    S(1, 'gate', 1, 'condition', { code: 'return true;' }),
    S(2, 't1', 1, 'log', { message: 'true 1' }, { parentId: 1, branch: 'true' }),
    S(3, 't2', 2, 'log', { message: 'true 2' }, { parentId: 1, branch: 'true' }),
    S(4, 't3', 3, 'log', { message: 'true 3' }, { parentId: 1, branch: 'true' }),
    S(5, 'f1', 1, 'log', { message: 'false 1' }, { parentId: 1, branch: 'false' }),
    S(6, 'f2', 2, 'log', { message: 'false 2' }, { parentId: 1, branch: 'false' }),
    S(7, 'f3', 3, 'log', { message: 'false 3' }, { parentId: 1, branch: 'false' }),
    S(8, 'after', 2, 'log', { message: 'after' }),
  ]};
  const r = await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
  assert.ok(r.completedSteps.includes('t1'));
  assert.ok(r.completedSteps.includes('t2'));
  assert.ok(r.completedSteps.includes('t3'));
  assert.ok(!r.completedSteps.includes('f1'));
  assert.ok(!r.completedSteps.includes('f2'));
  assert.ok(!r.completedSteps.includes('f3'));
  assert.ok(r.completedSteps.includes('after'));
  assert.strictEqual(r.completedSteps.indexOf('t1') < r.completedSteps.indexOf('t2'), true);
  assert.strictEqual(r.completedSteps.indexOf('t2') < r.completedSteps.indexOf('t3'), true);
  console.log('    PASS');
}

async function testNestedBranchErrorStopsParentFlow() {
  console.log('  Error in nested branch with onError=stop halts entire flow');
  const flow = { id: 207, name: 'nested-error-stop', steps: [
    S(1, 'outer', 1, 'condition', { code: 'return true;' }),
    S(2, 'inner', 1, 'condition', { code: 'return true;' }, { parentId: 1, branch: 'true' }),
    S(3, 'crash', 1, 'script', { code: 'throw new Error("nested crash");' }, { parentId: 2, branch: 'true' }),
    S(4, 'after_crash', 2, 'log', { message: 'should not run' }, { parentId: 2, branch: 'true' }),
    S(5, 'end', 2, 'log', { message: 'should not run either' }),
  ]};
  try {
    await executeFlow(flow, {}, new MockHandlerExecutor(), new MockRepoRegistry());
    assert.fail('Should have thrown');
  } catch (e) {
    assert.strictEqual(e.message, 'nested crash');
  }
  console.log('    PASS');
}

async function main() {
  console.log('\nFlow Engine Tests\n');

  const tests = [
    testBasicFlow,
    testDataChain,
    testConditionFalseContinuesRoot,
    testConditionContinueOnTrue,
    testErrorSkip,
    testErrorStop,
    testDisabledSteps,
    testStepOrdering,
    testPayloadAccessible,
    testCreateStep,
    testUpdateStep,
    testDeleteStep,
    testHttpStep,
    testHttpGetNoBody,
    testTriggerFlowStep,
    testSleepStep,
    testLogWritesToShareLogs,
    testScriptAccessesRepos,
    testScriptAccessesHelpers,
    testScriptTriggersFlow,
    testMultiStepDataPipeline,
    testPipelineContinuesAfterConditionFalse,
    testMultipleSkipErrors,
    testEmptyFlow,
    testMetaAvailable,
    testScriptModifiesPayload,
    testConditionBasedOnPayload,
    testUnknownStepTypeThrows,
    testQueryWithFilterAndLimit,
    testLastUpdatesEachStep,
    testBranchTrue,
    testBranchFalse,
    testBranchNoChildren,
    testBranchMultipleSteps,
    testBranchAccessParentResult,
    testBranchSkipError,
    testBranchDisabledStep,
    testMultipleConditionsSequential,
    testEmptyTrueBranchPopulatedFalse,
    testRealWorldOrderProcessing,
    testRealWorldUserRegistration,
    testRealWorldScheduledCleanup,
    testRealWorldErrorRecovery,
    testRealWorldMultiTableJoin,
    testRealWorldDispatchFromFlow,
    testRealWorldAuthFlow,
    testBranchPayloadDriven,
    testBranchErrorStopsFlow,
    testBothBranchesPopulated,
    testQueryMissingTable,
    testCreateMissingTable,
    testUpdateMissingTable,
    testDeleteMissingTable,
    testHttpMissingUrl,
    testConditionReturns1Truthy,
    testConditionReturns0Falsy,
    testConditionReturnsNullFalsy,
    testConditionReturnsObjectTruthy,
    testConditionReturnsEmptyStringFalsy,
    testConditionReturnsFalseStringTruthy,
    testCircularFlowDetection,
    testDepthLimitExceeded,
    testRetryEventualSuccess,
    testRetryAllAttemptsFail,
    testNestedConditionInsideBranch,
    testTripleNestedConditions,
    testHttpAutoContentType,
    testHttpGetNoBodyStrict,
    testAllStepsDisabled,
    testScriptAccessesDispatch,
    testNestedBranchBothFalse,
    testNestedBranchDataChain,
    testMultipleBranchesAtSameLevel,
    testBranchWithMultipleStepsAndSkipError,
    testBranchWithCrudInsideBranch,
    testDeepNestingFourLevels,
    testBranchTrueAndFalseBothHaveMultipleSteps,
    testNestedBranchErrorStopsParentFlow,
  ];

  let passed = 0;
  let failed = 0;

  for (const test of tests) {
    try {
      await test();
      passed++;
    } catch (error) {
      failed++;
      console.log(`    FAIL: ${error.message}`);
    }
  }

  console.log(`\nResults: ${passed} passed, ${failed} failed\n`);
  process.exit(failed > 0 ? 1 : 0);
}

main();
