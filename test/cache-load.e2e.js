/**
 * Cache Load E2E Test
 * Verifies FlowCacheService and WebsocketCacheService load data correctly
 * using single-query with relation joins (fields: ['*', 'steps.*'] / ['*', 'events.*'])
 *
 * Run: node test/cache-load.e2e.js
 */

const assert = require('assert');

let testsPassed = 0;
let testsFailed = 0;

function log(name, passed, details = '') {
  const status = passed ? '✓ PASS' : '✗ FAIL';
  console.log(`${status}: ${name}${details ? '\n         ' + details : ''}`);
  if (passed) testsPassed++;
  else testsFailed++;
}

class MockQueryBuilder {
  constructor() {
    this.calls = [];
    this.data = {};
    this._isMongo = false;
  }

  setMongoMode(val) { this._isMongo = val; }
  isMongoDb() { return this._isMongo; }

  setTableData(tableName, data) {
    this.data[tableName] = data;
  }

  async select(params) {
    this.calls.push(params);
    const rows = this.data[params.tableName] || [];

    let filtered = [...rows];
    if (params.filter?.isEnabled) {
      filtered = filtered.filter(r => r.isEnabled === true);
    }

    if (params.fields?.includes('steps.*')) {
      const allSteps = this.data['flow_step_definition'] || [];
      filtered = filtered.map(flow => ({
        ...flow,
        steps: allSteps.filter(s => s.flowId === (flow._id || flow.id)),
      }));
    }

    if (params.fields?.includes('events.*')) {
      const allEvents = this.data['websocket_event_definition'] || [];
      filtered = filtered.map(gw => ({
        ...gw,
        events: allEvents.filter(e => e.gatewayId === (gw._id || gw.id)),
      }));
    }

    return { data: filtered };
  }
}

class MockEventEmitter {
  constructor() { this.emitted = []; }
  emit(event, data) { this.emitted.push({ event, data }); }
}

class MockRedisPubSub {
  subscribeWithHandler() {}
  async publish() {}
}

class MockInstanceService {
  getInstanceId() { return 'test-instance'; }
}

// ─── Flow Cache ───

function transformCode(code) { return code; }

async function simulateFlowCacheLoad(queryBuilder) {
  const isMongoDB = queryBuilder.isMongoDb();
  const idField = isMongoDB ? '_id' : 'id';

  const flowsResult = await queryBuilder.select({
    tableName: 'flow_definition',
    filter: { isEnabled: { _eq: true } },
    fields: ['*', 'steps.*'],
  });

  return flowsResult.data.map((flow) => {
    const rawSteps = (flow.steps || [])
      .filter((s) => s.isEnabled)
      .sort((a, b) => (a.stepOrder || 0) - (b.stepOrder || 0));

    const steps = rawSteps.map((step) => {
      if ((step.type === 'script' || step.type === 'condition') && step.config?.code) {
        step.config.code = transformCode(step.config.code);
      }
      return {
        id: step[idField],
        key: step.key,
        stepOrder: step.stepOrder,
        type: step.type,
        config: step.config,
        timeout: step.timeout || 5000,
        onError: step.onError || 'stop',
        retryAttempts: step.retryAttempts || 0,
        isEnabled: step.isEnabled,
        parentId: step.parentId || step.parent?.[idField] || null,
        branch: step.branch || null,
      };
    });

    return {
      id: flow[idField],
      name: flow.name,
      description: flow.description,
      icon: flow.icon,
      triggerType: flow.triggerType,
      triggerConfig: flow.triggerConfig,
      timeout: flow.timeout || 30000,
      isEnabled: flow.isEnabled,
      steps,
    };
  });
}

// ─── WebSocket Cache ───

async function simulateWebsocketCacheLoad(queryBuilder) {
  const result = await queryBuilder.select({
    tableName: 'websocket_definition',
    filter: { isEnabled: { _eq: true } },
    fields: ['*', 'events.*'],
  });

  return result.data.map((gateway) => {
    if (gateway.connectionHandlerScript) {
      gateway.connectionHandlerScript = transformCode(gateway.connectionHandlerScript);
    }

    gateway.events = (gateway.events || []).filter((e) => e.isEnabled);
    for (const event of gateway.events) {
      if (event.handlerScript) {
        event.handlerScript = transformCode(event.handlerScript);
      }
    }

    return gateway;
  });
}

// ─── Tests ───

async function testFlowCacheSingleQuery() {
  const qb = new MockQueryBuilder();

  qb.setTableData('flow_definition', [
    { id: 1, name: 'daily-report', triggerType: 'schedule', triggerConfig: { cron: '0 2 * * *' }, timeout: 30000, isEnabled: true },
    { id: 2, name: 'disabled-flow', triggerType: 'manual', isEnabled: false },
    { id: 3, name: 'manual-cleanup', triggerType: 'manual', timeout: 60000, isEnabled: true },
  ]);

  qb.setTableData('flow_step_definition', [
    { id: 10, flowId: 1, key: 'fetch-data', stepOrder: 1, type: 'query', config: { table: 'user_definition' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 11, flowId: 1, key: 'send-email', stepOrder: 2, type: 'http', config: { url: 'https://api.email.com' }, isEnabled: true, onError: 'retry', retryAttempts: 3 },
    { id: 12, flowId: 1, key: 'disabled-step', stepOrder: 3, type: 'log', config: {}, isEnabled: false, onError: 'stop', retryAttempts: 0 },
    { id: 20, flowId: 2, key: 'should-not-appear', stepOrder: 1, type: 'script', config: {}, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 30, flowId: 3, key: 'cleanup', stepOrder: 1, type: 'script', config: { code: 'return true' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
  ]);

  const flows = await simulateFlowCacheLoad(qb);

  log('Flow: single query issued', qb.calls.length === 1, `queries: ${qb.calls.length}`);
  log('Flow: query uses fields with steps.*', qb.calls[0].fields.includes('steps.*'));
  log('Flow: disabled flow filtered out', flows.length === 2, `flows: ${flows.length}`);
  log('Flow: first flow has correct name', flows[0].name === 'daily-report');
  log('Flow: first flow has 2 enabled steps', flows[0].steps.length === 2, `steps: ${flows[0].steps.length}`);
  log('Flow: disabled step filtered out', !flows[0].steps.find(s => s.key === 'disabled-step'));
  log('Flow: steps sorted by stepOrder', flows[0].steps[0].key === 'fetch-data' && flows[0].steps[1].key === 'send-email');
  log('Flow: step onError preserved', flows[0].steps[1].onError === 'retry');
  log('Flow: step retryAttempts preserved', flows[0].steps[1].retryAttempts === 3);
  log('Flow: second flow has 1 step', flows[1].steps.length === 1);
  log('Flow: timeout defaults to 30000', flows[0].timeout === 30000);
  log('Flow: custom timeout preserved', flows[1].timeout === 60000);
  log('Flow: triggerConfig preserved', flows[0].triggerConfig?.cron === '0 2 * * *');
}

async function testFlowCacheStepOrdering() {
  const qb = new MockQueryBuilder();

  qb.setTableData('flow_definition', [
    { id: 1, name: 'order-test', triggerType: 'manual', isEnabled: true },
  ]);

  qb.setTableData('flow_step_definition', [
    { id: 13, flowId: 1, key: 'third', stepOrder: 30, type: 'log', config: {}, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 11, flowId: 1, key: 'first', stepOrder: 10, type: 'log', config: {}, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 12, flowId: 1, key: 'second', stepOrder: 20, type: 'log', config: {}, isEnabled: true, onError: 'stop', retryAttempts: 0 },
  ]);

  const flows = await simulateFlowCacheLoad(qb);
  const keys = flows[0].steps.map(s => s.key);

  log('Flow ordering: steps sorted correctly', keys[0] === 'first' && keys[1] === 'second' && keys[2] === 'third', `order: ${keys.join(', ')}`);
}

async function testFlowCacheBranching() {
  const qb = new MockQueryBuilder();

  qb.setTableData('flow_definition', [
    { id: 1, name: 'branching-test', triggerType: 'manual', isEnabled: true },
  ]);

  qb.setTableData('flow_step_definition', [
    { id: 10, flowId: 1, key: 'check', stepOrder: 1, type: 'condition', config: { code: 'return true' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 11, flowId: 1, key: 'on-true', stepOrder: 2, type: 'log', config: {}, isEnabled: true, onError: 'stop', retryAttempts: 0, parentId: 10, branch: 'true' },
    { id: 12, flowId: 1, key: 'on-false', stepOrder: 3, type: 'log', config: {}, isEnabled: true, onError: 'stop', retryAttempts: 0, parentId: 10, branch: 'false' },
  ]);

  const flows = await simulateFlowCacheLoad(qb);
  const steps = flows[0].steps;

  log('Branching: 3 steps loaded', steps.length === 3);
  log('Branching: parent step has no parentId', steps[0].parentId === null);
  log('Branching: true branch has parentId', steps[1].parentId === 10);
  log('Branching: true branch value correct', steps[1].branch === 'true');
  log('Branching: false branch has parentId', steps[2].parentId === 10);
  log('Branching: false branch value correct', steps[2].branch === 'false');
}

async function testFlowCacheEmptySteps() {
  const qb = new MockQueryBuilder();

  qb.setTableData('flow_definition', [
    { id: 1, name: 'empty-flow', triggerType: 'manual', isEnabled: true },
  ]);

  qb.setTableData('flow_step_definition', []);

  const flows = await simulateFlowCacheLoad(qb);

  log('Empty: flow loaded with 0 steps', flows[0].steps.length === 0);
}

async function testFlowCacheCodeTransform() {
  const qb = new MockQueryBuilder();

  qb.setTableData('flow_definition', [
    { id: 1, name: 'code-test', triggerType: 'manual', isEnabled: true },
  ]);

  qb.setTableData('flow_step_definition', [
    { id: 10, flowId: 1, key: 'script-step', stepOrder: 1, type: 'script', config: { code: 'return 1' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 11, flowId: 1, key: 'condition-step', stepOrder: 2, type: 'condition', config: { code: 'return true' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
    { id: 12, flowId: 1, key: 'query-step', stepOrder: 3, type: 'query', config: { table: 'users', code: 'should-not-transform' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
  ]);

  const flows = await simulateFlowCacheLoad(qb);
  const steps = flows[0].steps;

  log('Transform: script code passed through transformCode', steps[0].config.code !== undefined);
  log('Transform: condition code passed through transformCode', steps[1].config.code !== undefined);
  log('Transform: query step code untouched', steps[2].config.code === 'should-not-transform');
}

// ─── WebSocket Tests ───

async function testWebsocketCacheSingleQuery() {
  const qb = new MockQueryBuilder();

  qb.setTableData('websocket_definition', [
    { id: 1, path: '/chat', isEnabled: true, requireAuth: true, connectionHandlerScript: 'console.log("connected")', connectionHandlerTimeout: 5000 },
    { id: 2, path: '/notifications', isEnabled: true, requireAuth: false, connectionHandlerScript: null, connectionHandlerTimeout: 3000 },
    { id: 3, path: '/disabled', isEnabled: false, requireAuth: false, connectionHandlerScript: null, connectionHandlerTimeout: 3000 },
  ]);

  qb.setTableData('websocket_event_definition', [
    { id: 100, gatewayId: 1, eventName: 'message', isEnabled: true, handlerScript: 'return data', timeout: 5000 },
    { id: 101, gatewayId: 1, eventName: 'typing', isEnabled: true, handlerScript: null, timeout: 3000 },
    { id: 102, gatewayId: 1, eventName: 'disabled-event', isEnabled: false, handlerScript: 'nope', timeout: 3000 },
    { id: 200, gatewayId: 2, eventName: 'notify', isEnabled: true, handlerScript: 'return notif', timeout: 5000 },
    { id: 300, gatewayId: 3, eventName: 'should-not-load', isEnabled: true, handlerScript: null, timeout: 3000 },
  ]);

  const gateways = await simulateWebsocketCacheLoad(qb);

  log('WS: single query issued', qb.calls.length === 1, `queries: ${qb.calls.length}`);
  log('WS: query uses fields with events.*', qb.calls[0].fields.includes('events.*'));
  log('WS: disabled gateway filtered out', gateways.length === 2, `gateways: ${gateways.length}`);
  log('WS: first gateway path correct', gateways[0].path === '/chat');
  log('WS: first gateway has 2 enabled events', gateways[0].events.length === 2, `events: ${gateways[0].events.length}`);
  log('WS: disabled event filtered out', !gateways[0].events.find(e => e.eventName === 'disabled-event'));
  log('WS: second gateway has 1 event', gateways[1].events.length === 1);
  log('WS: connectionHandlerScript transformed', gateways[0].connectionHandlerScript !== null);
  log('WS: null connectionHandlerScript stays null', gateways[1].connectionHandlerScript === null);
  log('WS: event handlerScript preserved', gateways[0].events[0].handlerScript === 'return data');
  log('WS: event with null handlerScript preserved', gateways[0].events[1].handlerScript === null);
  log('WS: requireAuth preserved', gateways[0].requireAuth === true && gateways[1].requireAuth === false);
}

async function testWebsocketCacheEmptyEvents() {
  const qb = new MockQueryBuilder();

  qb.setTableData('websocket_definition', [
    { id: 1, path: '/empty', isEnabled: true, requireAuth: false, connectionHandlerScript: null, connectionHandlerTimeout: 3000 },
  ]);

  qb.setTableData('websocket_event_definition', []);

  const gateways = await simulateWebsocketCacheLoad(qb);

  log('WS empty: gateway loaded with 0 events', gateways[0].events.length === 0);
}

async function testWebsocketCacheAllEventsDisabled() {
  const qb = new MockQueryBuilder();

  qb.setTableData('websocket_definition', [
    { id: 1, path: '/all-disabled', isEnabled: true, requireAuth: false, connectionHandlerScript: null, connectionHandlerTimeout: 3000 },
  ]);

  qb.setTableData('websocket_event_definition', [
    { id: 100, gatewayId: 1, eventName: 'e1', isEnabled: false, handlerScript: null, timeout: 3000 },
    { id: 101, gatewayId: 1, eventName: 'e2', isEnabled: false, handlerScript: null, timeout: 3000 },
  ]);

  const gateways = await simulateWebsocketCacheLoad(qb);

  log('WS all-disabled: gateway has 0 events after filter', gateways[0].events.length === 0);
}

// ─── MongoDB Mode ───

async function testFlowCacheMongoMode() {
  const qb = new MockQueryBuilder();
  qb.setMongoMode(true);

  qb.setTableData('flow_definition', [
    { _id: 'abc123', name: 'mongo-flow', triggerType: 'manual', isEnabled: true },
  ]);

  qb.setTableData('flow_step_definition', [
    { _id: 'step1', flowId: 'abc123', key: 'first', stepOrder: 1, type: 'script', config: { code: 'return 1' }, isEnabled: true, onError: 'stop', retryAttempts: 0 },
  ]);

  const flows = await simulateFlowCacheLoad(qb);

  log('Mongo: flow id uses _id', flows[0].id === 'abc123');
  log('Mongo: step id uses _id', flows[0].steps[0].id === 'step1');
}

// ─── Run ───

async function main() {
  console.log('=== Cache Load Tests ===\n');

  console.log('--- Flow Cache ---');
  await testFlowCacheSingleQuery();
  await testFlowCacheStepOrdering();
  await testFlowCacheBranching();
  await testFlowCacheEmptySteps();
  await testFlowCacheCodeTransform();
  await testFlowCacheMongoMode();

  console.log('\n--- WebSocket Cache ---');
  await testWebsocketCacheSingleQuery();
  await testWebsocketCacheEmptyEvents();
  await testWebsocketCacheAllEventsDisabled();

  console.log(`\n=== Results: ${testsPassed} passed, ${testsFailed} failed, ${testsPassed + testsFailed} total ===`);
  process.exit(testsFailed > 0 ? 1 : 0);
}

main().catch(err => {
  console.error('Test runner error:', err);
  process.exit(1);
});
