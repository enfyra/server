import 'dotenv/config';
import assert from 'node:assert';
import http from 'node:http';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import { Server } from 'socket.io';
import { io as clientIo, Socket } from 'socket.io-client';
import { buildContainer } from '../src/container';
import { buildExpressApp } from '../src/express-app';
import { init, shutdown } from '../src/init';
import { CACHE_EVENTS } from '../src/shared/utils/cache-events.constants';

const USER_STEPS = (process.env.CHAT_LOAD_USERS || '100,250,500')
  .split(',')
  .map((v) => Number(v.trim()))
  .filter(Boolean);
const INSTANCE_STEPS = (process.env.CHAT_LOAD_INSTANCES || '1,2')
  .split(',')
  .map((v) => Number(v.trim()))
  .filter(Boolean);
const CHAT_LOAD_MODE = process.env.CHAT_LOAD_MODE || 'realistic';
const MESSAGES_PER_USER = Number(process.env.CHAT_LOAD_MESSAGES_PER_USER || 2);
const REALISTIC_DURATION_MS = Number(
  process.env.CHAT_LOAD_REALISTIC_DURATION_MS || 30000,
);
const ACTIVE_USER_RATIO = Number(
  process.env.CHAT_LOAD_ACTIVE_USER_RATIO || 0.12,
);
const BURST_CHANCE = Number(process.env.CHAT_LOAD_BURST_CHANCE || 0.25);
const BURST_MAX = Number(process.env.CHAT_LOAD_BURST_MAX || 4);
const GROUP_MESSAGE_RATIO = Number(
  process.env.CHAT_LOAD_GROUP_MESSAGE_RATIO || 0.65,
);
const READ_PAUSE_MIN_MS = Number(process.env.CHAT_LOAD_READ_PAUSE_MIN_MS || 900);
const READ_PAUSE_MAX_MS = Number(process.env.CHAT_LOAD_READ_PAUSE_MAX_MS || 4500);
const BURST_PAUSE_MIN_MS = Number(process.env.CHAT_LOAD_BURST_PAUSE_MIN_MS || 120);
const BURST_PAUSE_MAX_MS = Number(process.env.CHAT_LOAD_BURST_PAUSE_MAX_MS || 650);
const GROUP_COUNT = Number(process.env.CHAT_LOAD_GROUPS || 20);
const CONNECT_BATCH = Number(process.env.CHAT_LOAD_CONNECT_BATCH || 100);
const CONNECT_TIMEOUT_MS = Number(
  process.env.CHAT_LOAD_CONNECT_TIMEOUT_MS || 5000,
);
const ACK_TIMEOUT_MS = Number(process.env.CHAT_LOAD_ACK_TIMEOUT_MS || 5000);
const RESULT_TIMEOUT_MS = Number(
  process.env.CHAT_LOAD_RESULT_TIMEOUT_MS || 15000,
);
const DELIVERY_TIMEOUT_MS = Number(
  process.env.CHAT_LOAD_DELIVERY_TIMEOUT_MS || 20000,
);
const DB_TIMEOUT_MS = Number(process.env.CHAT_LOAD_DB_TIMEOUT_MS || 30000);
const DB_DRAIN_OBSERVE_MS = Number(
  process.env.CHAT_LOAD_DB_DRAIN_OBSERVE_MS || 5000,
);
const KEEP_ROWS = process.env.CHAT_LOAD_KEEP_ROWS === '1';
const TRIGGER_FLOW = process.env.CHAT_LOAD_TRIGGER_FLOW !== '0';
const EXTERNAL_URLS = (process.env.CHAT_LOAD_EXTERNAL_URLS || '')
  .split(',')
  .map((v) => v.trim().replace(/\/+$/, ''))
  .filter(Boolean);
const EXTERNAL_INSTANCE_COUNT = Number(
  process.env.CHAT_LOAD_EXTERNAL_INSTANCE_COUNT || EXTERNAL_URLS.length || 0,
);
const AUTH_BEARER = process.env.CHAT_LOAD_AUTH_BEARER || '';
const AUTH_COOKIE = process.env.CHAT_LOAD_AUTH_COOKIE || '';
const NAMESPACE = '/e2e-chat-load';
const CLIENT_NAMESPACE = EXTERNAL_URLS.length ? `/ws${NAMESPACE}` : NAMESPACE;
const TABLE_NAME = 'e2e_chat_message';
const FLOW_NAME = 'e2e_chat_message_persist';
const RUN_ID = `app_chat_${Date.now()}`;
const TRACE_DIR = `.tmp/realtime-chat-${RUN_ID}`;
const TRACE_FILE = `${TRACE_DIR}/ws-event.ndjson`;
const SUMMARY_FILE = `${TRACE_DIR}/summary.json`;

type AppInstance = {
  container?: ReturnType<typeof buildContainer>;
  httpServer?: http.Server;
  ioServer?: Server;
  port?: number;
  url: string;
};

type TrackedClient = {
  userId: number;
  socket: Socket;
  groups: string[];
  port: number;
};

function wait(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitUntil(
  fn: () => boolean | Promise<boolean>,
  timeoutMs: number,
  intervalMs = 100,
) {
  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    if (await fn()) return true;
    await wait(intervalMs);
  }
  return await fn();
}

function percentile(values: number[], p: number) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const idx = Math.min(
    sorted.length - 1,
    Math.ceil((p / 100) * sorted.length) - 1,
  );
  return sorted[idx];
}

function createRng(seed: number) {
  let value = seed >>> 0;
  return () => {
    value = (value * 1664525 + 1013904223) >>> 0;
    return value / 0x100000000;
  };
}

function randomInt(rng: () => number, min: number, max: number) {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function setupTraceFiles() {
  mkdirSync(TRACE_DIR, { recursive: true });
  writeFileSync(TRACE_FILE, '');
  process.env.WS_EVENT_TRACE_FILE = TRACE_FILE;
}

function summarizeTrace() {
  if (!existsSync(TRACE_FILE)) {
    return { file: TRACE_FILE, count: 0 };
  }
  const entries = readFileSync(TRACE_FILE, 'utf8')
    .split('\n')
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  const successful = entries.filter((entry) => entry.status === 'success');
  const values = (field: string) =>
    successful
      .map((entry) => entry[field])
      .filter((value) => typeof value === 'number');
  const stat = (field: string) => {
    const nums = values(field);
    return {
      count: nums.length,
      p50: percentile(nums, 50),
      p95: percentile(nums, 95),
      max: nums.length ? Math.max(...nums) : 0,
    };
  };
  const socketToEnqueue = successful
    .map((entry) =>
      typeof entry.enqueuedAt === 'number' &&
      typeof entry.socketReceivedAt === 'number'
        ? entry.enqueuedAt - entry.socketReceivedAt
        : null,
    )
    .filter((value): value is number => typeof value === 'number');
  const ackToEnqueue = successful
    .map((entry) =>
      typeof entry.enqueuedAt === 'number' &&
      typeof entry.ackSentAt === 'number'
        ? entry.enqueuedAt - entry.ackSentAt
        : null,
    )
    .filter((value): value is number => typeof value === 'number');
  return {
    file: TRACE_FILE,
    count: entries.length,
    success: successful.length,
    error: entries.length - successful.length,
    socketToEnqueueMs: {
      p50: percentile(socketToEnqueue, 50),
      p95: percentile(socketToEnqueue, 95),
      max: socketToEnqueue.length ? Math.max(...socketToEnqueue) : 0,
    },
    ackToEnqueueMs: {
      p50: percentile(ackToEnqueue, 50),
      p95: percentile(ackToEnqueue, 95),
      max: ackToEnqueue.length ? Math.max(...ackToEnqueue) : 0,
    },
    queueWaitMs: stat('queueWaitMs'),
    executorMs: stat('executorMs'),
    inlineExecutorMs: stat('inlineExecutorMs'),
    serverReceiveLagMs: stat('serverReceiveLagMs'),
    totalHandlerMs: stat('totalHandlerMs'),
  };
}

function groupsForUser(userId: number) {
  const primary = `e2e_group_${userId % GROUP_COUNT}`;
  const secondary = `e2e_group_${(userId + 7) % GROUP_COUNT}`;
  return primary === secondary ? [primary] : [primary, secondary];
}

function createRootContext(container: ReturnType<typeof buildContainer>) {
  const ctx = container.cradle.dynamicContextFactory.createBase({
    user: { id: 1, isRootAdmin: true },
  });
  ctx.$repos = container.cradle.repoRegistryService.createReposProxy(ctx);
  return ctx;
}

async function ensureMessageTable(
  container: ReturnType<typeof buildContainer>,
) {
  const c = container.cradle;
  const isMongo = c.databaseConfigService.isMongoDb();
  let existing = await c.queryBuilderService.findOne({
    table: 'enfyra_table',
    where: { name: TABLE_NAME },
  });
  if (!isMongo) {
    const knex = c.queryBuilderService.getKnex();
    const hasPhysicalTable = await knex.schema.hasTable(TABLE_NAME);
    if (existing && !hasPhysicalTable) {
      await knex('enfyra_table').where({ name: TABLE_NAME }).delete();
      existing = null;
      await reloadRuntime(container);
    }
  } else if (existing) {
    const exists = await c.mongoService
      .getDb()
      .listCollections({ name: TABLE_NAME })
      .hasNext();
    if (!exists) {
      await c.mongoService.getDb().createCollection(TABLE_NAME);
    }
  }
  if (!existing) {
    const ctx = createRootContext(container);
    await ctx.$repos.enfyra_table.create({
      data: {
        name: TABLE_NAME,
        description: 'E2E realtime chat load messages',
        columns: [
          isMongo
            ? {
                name: '_id',
                type: 'ObjectId',
                isPrimary: true,
                isGenerated: true,
                isNullable: false,
              }
            : {
                name: 'id',
                type: 'int',
                isPrimary: true,
                isGenerated: true,
                isNullable: false,
              },
          { name: 'run_id', type: 'varchar', isNullable: false, index: true },
          {
            name: 'message_id',
            type: 'varchar',
            isNullable: false,
            isUnique: true,
          },
          { name: 'kind', type: 'varchar', isNullable: false },
          { name: 'room', type: 'varchar', isNullable: false, index: true },
          { name: 'sender_id', type: 'int', isNullable: false },
          { name: 'target_id', type: 'varchar', isNullable: false },
          { name: 'text', type: 'text', isNullable: false },
          { name: 'sent_at', type: 'varchar', isNullable: false },
        ],
        indexes: [['run_id'], ['room']],
        uniques: [['message_id']],
      },
    });
  }
  await reloadRuntime(container);
}

async function ensureFlowStepScriptColumns(
  container: ReturnType<typeof buildContainer>,
) {
  const c = container.cradle;
  const table = await c.queryBuilderService.findOne({
    table: 'enfyra_table',
    where: { name: 'enfyra_flow_step' },
  });
  assert(table, 'enfyra_flow_step metadata not found');
  const tableId = table.id ?? table._id;
  const isMongo = c.databaseConfigService.isMongoDb();
  const columns = [
    {
      name: 'sourceCode',
      type: 'code',
      description: 'Flow step source script before compilation',
    },
    {
      name: 'scriptLanguage',
      type: 'enum',
      description: 'Flow step source script language',
      options: ['javascript', 'typescript'],
      defaultValue: 'typescript',
      isNullable: false,
    },
    {
      name: 'compiledCode',
      type: 'code',
      description: 'Compiled executable JavaScript for flow step scripts',
      isGenerated: true,
      isUpdatable: false,
    },
  ];

  for (const column of columns) {
    if (isMongo) {
      const collection = c.mongoService.getDb().collection('enfyra_column');
      const existing = await collection.findOne({
        table: tableId,
        name: column.name,
      });
      if (!existing) {
        await collection.insertOne({
          table: tableId,
          name: column.name,
          type: column.type,
          isNullable: column.isNullable ?? true,
          isSystem: true,
          isPrimary: false,
          isGenerated: column.isGenerated ?? false,
          isUpdatable: column.isUpdatable ?? true,
          isPublished: true,
          defaultValue: column.defaultValue,
          options: column.options,
          description: column.description,
        });
      }
      continue;
    }

    const knex = c.queryBuilderService.getKnex();
    const hasPhysicalColumn = await knex.schema.hasColumn(
      'enfyra_flow_step',
      column.name,
    );
    if (!hasPhysicalColumn) {
      await knex.schema.alterTable('enfyra_flow_step', (t) => {
        if (column.name === 'scriptLanguage') {
          t.string(column.name).notNullable().defaultTo('typescript');
        } else {
          t.text(column.name, 'longtext').nullable();
        }
      });
    }
    const existing = await knex('enfyra_column')
      .where({ tableId, name: column.name })
      .first();
    if (!existing) {
      await knex('enfyra_column').insert({
        tableId,
        name: column.name,
        type: column.type,
        isNullable: column.isNullable ?? true,
        isSystem: true,
        isPrimary: false,
        isGenerated: column.isGenerated ?? false,
        isUpdatable: column.isUpdatable ?? true,
        isPublished: true,
        defaultValue:
          column.defaultValue === undefined
            ? null
            : JSON.stringify(column.defaultValue),
        options:
          column.options === undefined ? null : JSON.stringify(column.options),
        description: column.description,
      });
    }
  }

  await reloadRuntime(container);
}

function connectionSource() {
  return `
    const auth = $ctx.$data?.auth || {};
    const userId = String(auth.chatUserId || $ctx.$data?.headers?.['x-chat-user-id'] || '');
    if (userId) {
      $ctx.$socket.join('user_' + userId);
    }
    const rawGroups = auth.chatGroups || $ctx.$data?.headers?.['x-chat-groups'];
    const groups = Array.isArray(rawGroups)
      ? rawGroups
      : String(rawGroups || '').split(',').filter(Boolean);
    for (const group of groups) {
      $ctx.$socket.join(group);
    }
    return { joined: groups.length };
  `;
}

function messageSource() {
  return `
    const data = $ctx.$flow.$payload || {};
    const senderId = Number(data.senderId || 0);
    const kind = data.kind === 'dm' ? 'dm' : 'group';
    const room = String(data.room || '');
    await $ctx.$repos.${TABLE_NAME}.create({
      data: {
        run_id: data.runId,
        message_id: data.id,
        kind,
        room,
        sender_id: senderId,
        target_id: String(data.targetId || data.group || ''),
        text: String(data.text || ''),
        sent_at: String(data.sentAt),
      },
      batch: true,
    });
    return { persisted: true, id: data.id, room };
  `;
}

function chatEventSource() {
  return `
    const data = $ctx.$data || {};
    const kind = data.kind === 'dm' ? 'dm' : 'group';
    if (!data.id || !data.runId || !data.room || !data.senderId || !data.text || !data.sentAt) {
      throw new Error('Invalid chat payload');
    }
    if (kind === 'group') {
      $ctx.$socket.emitToCurrentRoom(data.room, 'chat:message', data);
    } else {
      $ctx.$socket.emitToUser(data.senderId, 'chat:message', data);
      $ctx.$socket.emitToUser(data.targetId, 'chat:message', data);
    }
    ${TRIGGER_FLOW ? `await $ctx.$trigger('${FLOW_NAME}', data);` : ''}
    return { accepted: true, id: data.id, kind, room: data.room };
  `;
}

async function upsertPersistFlow(container: ReturnType<typeof buildContainer>) {
  const c = container.cradle;
  const ctx = createRootContext(container);
  const flowRepo = ctx.$repos.enfyra_flow;
  const stepRepo = ctx.$repos.enfyra_flow_step;
  let flow = await c.queryBuilderService.findOne({
    table: 'enfyra_flow',
    where: { name: FLOW_NAME },
  });
  if (flow) {
    await flowRepo.update({
      id: flow.id ?? flow._id,
      data: {
        isEnabled: true,
        triggerType: 'manual',
        timeout: 10000,
        maxExecutions: 100,
      },
    });
  } else {
    const created = await flowRepo.create({
      data: {
        name: FLOW_NAME,
        description: 'E2E realtime chat message persistence',
        triggerType: 'manual',
        isEnabled: true,
        timeout: 10000,
        maxExecutions: 100,
      },
    });
    flow = created.data?.[0];
  }
  const flowId = flow?.id ?? flow?._id;
  assert(flowId, 'persist flow id was not created');
  const existingStep = await c.queryBuilderService.findOne({
    table: 'enfyra_flow_step',
    where: { flow: flowId, key: 'persist_message' },
  });
  const stepData = {
    flow: flowId,
    key: 'persist_message',
    stepOrder: 0,
    type: 'script',
    sourceCode: messageSource(),
    scriptLanguage: 'typescript',
    config: {},
    timeout: 10000,
    onError: 'stop',
    retryAttempts: 0,
    isEnabled: true,
  };
  if (existingStep) {
    await stepRepo.update({
      id: existingStep.id ?? existingStep._id,
      data: stepData,
    });
  } else {
    await stepRepo.create({ data: stepData });
  }
  await reloadRuntime(container);
}

async function upsertGateway(container: ReturnType<typeof buildContainer>) {
  const c = container.cradle;
  const ctx = createRootContext(container);
  const gatewayRepo = ctx.$repos.enfyra_websocket;
  const eventRepo = ctx.$repos.enfyra_websocket_event;
  let gateway = await c.queryBuilderService.findOne({
    table: 'enfyra_websocket',
    where: { path: NAMESPACE },
  });
  if (gateway) {
    const id = gateway.id ?? gateway._id;
    await gatewayRepo.update({
      id,
      data: {
        isEnabled: true,
        requireAuth: false,
        sourceCode: connectionSource(),
        scriptLanguage: 'typescript',
        connectionHandlerTimeout: 10000,
      },
    });
  } else {
    const created = await gatewayRepo.create({
      data: {
        path: NAMESPACE,
        isEnabled: true,
        requireAuth: false,
        sourceCode: connectionSource(),
        scriptLanguage: 'typescript',
        connectionHandlerTimeout: 10000,
      },
    });
    gateway = created.data?.[0];
  }

  const gatewayId = gateway?.id ?? gateway?._id;
  assert(gatewayId, 'gateway id was not created');

  const existingEvent = await c.queryBuilderService.findOne({
    table: 'enfyra_websocket_event',
    where: { gatewayId, eventName: 'chat:send' },
  });
  const eventConfig = {
    isEnabled: true,
    sourceCode: chatEventSource(),
    compiledCode: null,
    scriptLanguage: 'typescript',
    timeout: 10000,
  };
  if (existingEvent) {
    await eventRepo.update({
      id: existingEvent.id ?? existingEvent._id,
      data: eventConfig,
    });
  } else {
    await eventRepo.create({
      data: {
        gateway: gatewayId,
        eventName: 'chat:send',
        ...eventConfig,
      },
    });
  }
  await reloadRuntime(container);
}

async function reloadRuntime(container: ReturnType<typeof buildContainer>) {
  const c = container.cradle;
  await c.metadataCacheService.reload();
  c.eventEmitter.emit(CACHE_EVENTS.METADATA_LOADED);
  await c.repoRegistryService.rebuildFromMetadata(c.metadataCacheService);
  await Promise.all([
    c.websocketCacheService.reload(),
    c.routeCacheService.reload(),
    c.fieldPermissionCacheService.reload(),
  ]);
  if (c.dynamicWebSocketGateway.server) {
    await c.dynamicWebSocketGateway.reloadGateways();
  }
}

async function prepareAppMetadata() {
  const container = buildContainer();
  await init(container);
  try {
    await ensureFlowStepScriptColumns(container);
    await ensureMessageTable(container);
    await upsertPersistFlow(container);
    await purgeE2eFlowJobs(container);
    await upsertGateway(container);
    if (container.cradle.databaseConfigService.isMongoDb()) {
      await container.cradle.mongoService
        .collection(TABLE_NAME)
        .deleteMany({ run_id: RUN_ID } as any);
    } else {
      const knex = container.cradle.queryBuilderService.getKnex();
      await knex(TABLE_NAME).where({ run_id: RUN_ID }).delete();
    }
  } finally {
    await shutdown(container);
  }
}

async function purgeE2eFlowJobs(container: ReturnType<typeof buildContainer>) {
  const flow = await container.cradle.queryBuilderService.findOne({
    table: 'enfyra_flow',
    where: { name: FLOW_NAME },
  });
  if (!flow) return;

  const result =
    await container.cradle.flowQueueMaintenanceService.removeFlowJobs(
      { id: flow.id ?? flow._id, name: FLOW_NAME },
      { includeCompleted: true },
    );
  writeFileSync(
    `${TRACE_DIR}/flow-queue-cleanup.json`,
    JSON.stringify(result, null, 2),
  );
}

async function getFlowQueueCounts(instance: AppInstance) {
  return await instance.container.cradle.flowQueue.getJobCounts(
    'waiting',
    'active',
    'completed',
    'failed',
    'delayed',
    'paused',
    'prioritized',
  );
}

async function startAppInstance(): Promise<AppInstance> {
  const container = buildContainer();
  await init(container);
  const app = buildExpressApp(container);
  const httpServer = http.createServer(app);
  const ioServer = new Server(httpServer, {
    cors: { origin: true, credentials: true },
  });
  const gateway = container.cradle.dynamicWebSocketGateway;
  gateway.server = ioServer;
  await gateway.afterInit(ioServer);
  const port = await new Promise<number>((resolve) => {
    httpServer.listen(0, '127.0.0.1', () =>
      resolve((httpServer.address() as any).port),
    );
  });
  await container.cradle.flowExecutionQueueService?.init?.();
  return { container, httpServer, ioServer, port, url: `http://127.0.0.1:${port}` };
}

async function stopAppInstance(instance: AppInstance) {
  instance.ioServer?.disconnectSockets(true);
  if (instance.ioServer) {
    await new Promise<void>((resolve) =>
      instance.ioServer!.close(() => resolve()),
    );
  }
  if (instance.httpServer) {
    await new Promise<void>((resolve) =>
      instance.httpServer!.close(() => resolve()),
    );
  }
  if (instance.container) {
    await shutdown(instance.container);
  }
}

async function connectClients(
  instances: AppInstance[],
  userCount: number,
  metrics: any,
  roomMembers: Map<string, Set<number>>,
) {
  const clients: TrackedClient[] = [];
  for (let base = 1; base <= userCount; base += CONNECT_BATCH) {
    const batch: Array<Promise<void>> = [];
    for (
      let userId = base;
      userId < base + CONNECT_BATCH && userId <= userCount;
      userId++
    ) {
      const groups = groupsForUser(userId);
      for (const group of groups) {
        if (!roomMembers.has(group)) roomMembers.set(group, new Set());
        roomMembers.get(group)!.add(userId);
      }
      const target = instances[(userId - 1) % instances.length];
      const socket = clientIo(`${target.url}${CLIENT_NAMESPACE}`, {
        ...(EXTERNAL_URLS.length ? {} : { transports: ['websocket'] }),
        forceNew: true,
        reconnection: false,
        auth: {
          chatUserId: String(userId),
          chatGroups: groups,
        },
        extraHeaders: {
          ...(AUTH_BEARER
            ? {
                authorization: `Bearer ${AUTH_BEARER}`,
              }
            : {}),
          ...(AUTH_COOKIE
            ? {
                cookie: AUTH_COOKIE,
              }
            : {}),
          'x-chat-user-id': String(userId),
          'x-chat-groups': groups.join(','),
        },
      });
      socket.on('chat:message', (msg: any) => {
        metrics.delivered++;
        metrics.deliveryLatencies.push(Date.now() - Number(msg.sentAt));
      });
      clients.push({ userId, socket, groups, port: target.port || 0 });
      batch.push(
        new Promise((resolve) => {
          let settled = false;
          let timer: NodeJS.Timeout;
          const finish = (connected: boolean) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            if (!connected) socket.disconnect();
            resolve();
          };
          timer = setTimeout(() => {
            metrics.connectFailed++;
            finish(false);
          }, CONNECT_TIMEOUT_MS);
          socket.once('connect', () => {
            metrics.connected++;
            finish(true);
          });
          socket.once('connect_error', () => {
            metrics.connectFailed++;
            finish(false);
          });
        }),
      );
    }
    await Promise.all(batch);
  }
  return clients.filter((client) => client.socket.connected);
}

function emitWithAck(socket: Socket, payload: any) {
  const started = Date.now();
  return new Promise<{ ok: boolean; latency: number; requestId?: string }>(
    (resolve) => {
      socket
        .timeout(ACK_TIMEOUT_MS)
        .emit('chat:send', payload, (error: any, response: any) => {
          resolve({
            ok:
              !error &&
              (response?.queued === true || response?.accepted === true),
            latency: Date.now() - started,
            requestId: response?.requestId,
          });
        });
    },
  );
}

async function countRows(
  instance: AppInstance,
  instanceCount: number,
  userCount: number,
) {
  assert(instance.container, 'row count requires a local probe container');
  if (instance.container.cradle.databaseConfigService.isMongoDb()) {
    return await instance.container.cradle.mongoService
      .collection(TABLE_NAME)
      .countDocuments({
        run_id: RUN_ID,
        message_id: {
          $regex: `^${RUN_ID}-${instanceCount}-${userCount}-`,
        },
      } as any);
  }
  const knex = instance.container.cradle.queryBuilderService.getKnex();
  const row = await knex(TABLE_NAME)
    .where({ run_id: RUN_ID })
    .andWhere('message_id', 'like', `${RUN_ID}-${instanceCount}-${userCount}-%`)
    .count('* as count')
    .first();
  return Number(row?.count || 0);
}

async function sendChatMessage(options: {
  instances: AppInstance[];
  userCount: number;
  client: TrackedClient;
  sequence: string | number;
  roomMembers: Map<string, Set<number>>;
  metrics: any;
  rng?: () => number;
}) {
  const { instances, userCount, client, sequence, roomMembers, metrics } =
    options;
  const { userId, socket, groups } = client;
  const rng = options.rng ?? Math.random;
  const isGroup = rng() < GROUP_MESSAGE_RATIO;
  const sentAt = Date.now();
  const id = `${RUN_ID}-${instances.length}-${userCount}-${sequence}-${userId}`;
  metrics.attempted++;
  const payload: any = {
    id,
    runId: RUN_ID,
    senderId: userId,
    text: `real app chat ${sequence} from ${userId}`,
    sentAt,
  };
  if (isGroup) {
    const group = groups[randomInt(rng, 0, groups.length - 1)];
    payload.kind = 'group';
    payload.group = group;
    payload.room = group;
    metrics.expectedDeliveries += roomMembers.get(group)?.size || 0;
  } else {
    const offset = randomInt(rng, 1, Math.min(17, Math.max(1, userCount - 1)));
    const targetId = ((userId + offset - 1) % userCount) + 1;
    payload.kind = 'dm';
    payload.targetId = targetId;
    payload.room = `dm:${[userId, targetId].sort((a, b) => a - b).join(':')}`;
    metrics.expectedDeliveries += 2;
  }
  const ack = await emitWithAck(socket, payload);
  if (ack.ok) {
    metrics.acked++;
    metrics.ackLatencies.push(ack.latency);
  } else {
    metrics.ackFailed++;
  }
}

async function runRoundTraffic(options: {
  instances: AppInstance[];
  userCount: number;
  clients: TrackedClient[];
  roomMembers: Map<string, Set<number>>;
  metrics: any;
}) {
  const { instances, userCount, clients, roomMembers, metrics } = options;
  metrics.activeUsers = clients.length;
  for (let round = 0; round < MESSAGES_PER_USER; round++) {
    await Promise.all(
      clients.map((client) =>
        sendChatMessage({
          instances,
          userCount,
          client,
          sequence: round,
          roomMembers,
          metrics,
          rng: createRng(userCount * 100000 + round * 1000 + client.userId),
        }),
      ),
    );
  }
}

async function runRealisticTraffic(options: {
  instances: AppInstance[];
  userCount: number;
  clients: TrackedClient[];
  roomMembers: Map<string, Set<number>>;
  metrics: any;
}) {
  const { instances, userCount, clients, roomMembers, metrics } = options;
  const activeCount = Math.max(
    1,
    Math.min(clients.length, Math.round(clients.length * ACTIVE_USER_RATIO)),
  );
  const activeClients = clients
    .map((client) => ({
      client,
      score: (client.userId * 2654435761) >>> 0,
    }))
    .sort((a, b) => a.score - b.score)
    .slice(0, activeCount)
    .map((item) => item.client);
  metrics.activeUsers = activeClients.length;
  const deadline = Date.now() + REALISTIC_DURATION_MS;
  await Promise.all(
    activeClients.map(async (client, index) => {
      const rng = createRng(userCount * 1000003 + client.userId * 97 + index);
      let sequence = 0;
      await wait(randomInt(rng, 0, Math.min(2000, REALISTIC_DURATION_MS)));
      while (Date.now() < deadline) {
        const burst =
          rng() < BURST_CHANCE ? randomInt(rng, 2, Math.max(2, BURST_MAX)) : 1;
        for (let i = 0; i < burst && Date.now() < deadline; i++) {
          await sendChatMessage({
            instances,
            userCount,
            client,
            sequence: `${sequence}.${i}`,
            roomMembers,
            metrics,
            rng,
          });
          await wait(randomInt(rng, BURST_PAUSE_MIN_MS, BURST_PAUSE_MAX_MS));
        }
        sequence++;
        await wait(randomInt(rng, READ_PAUSE_MIN_MS, READ_PAUSE_MAX_MS));
      }
    }),
  );
}

async function runScenario(instances: AppInstance[], userCount: number) {
  const metrics = {
    connected: 0,
    connectFailed: 0,
    attempted: 0,
    acked: 0,
    ackFailed: 0,
    results: 0,
    delivered: 0,
    expectedDeliveries: 0,
    ackLatencies: [] as number[],
    deliveryLatencies: [] as number[],
    activeUsers: 0,
  };
  const roomMembers = new Map<string, Set<number>>();
  const h = monitorEventLoopDelay({ resolution: 20 });
  h.enable();
  const clients = await connectClients(
    instances,
    userCount,
    metrics,
    roomMembers,
  );
  assert(clients.length > 0, 'no clients connected to real app gateway');
  let lastRoomProbe: any[] = [];
  const roomsReady = await waitUntil(async () => {
    const probe: any[] = [];
    for (const [room, members] of roomMembers) {
      const size =
        instances[0].container && instances[0].ioServer
          ? await instances[0].container.cradle.dynamicWebSocketGateway.namespaceRoomSize(
              NAMESPACE,
              room,
            )
          : members.size;
      if (size < members.size) {
        const localSizes = instances.map((instance) => {
          const namespace = instance.ioServer?.of(NAMESPACE);
          return namespace?.adapter.rooms.get(room)?.size || 0;
        });
        probe.push({
          room,
          expected: members.size,
          observed: size,
          localSizes,
        });
      }
    }
    lastRoomProbe = probe.slice(0, 20);
    return probe.length === 0;
  }, RESULT_TIMEOUT_MS);
  if (!roomsReady) {
    writeFileSync(
      `${TRACE_DIR}/room-ready-failure.json`,
      JSON.stringify(lastRoomProbe, null, 2),
    );
  }
  assert(roomsReady, 'group room joins did not finish before chat send phase');

  const started = Date.now();
  if (CHAT_LOAD_MODE === 'round') {
    await runRoundTraffic({
      instances,
      userCount,
      clients,
      roomMembers,
      metrics,
    });
  } else {
    await runRealisticTraffic({
      instances,
      userCount,
      clients,
      roomMembers,
      metrics,
    });
  }

  const allDelivered = await waitUntil(
    () => metrics.delivered >= metrics.expectedDeliveries,
    DELIVERY_TIMEOUT_MS,
  );
  const dbDone = await waitUntil(
    async () =>
      (await countRows(instances[0], instances.length, userCount)) >=
      metrics.attempted,
    DB_DRAIN_OBSERVE_MS,
  );
  h.disable();
  const dbRows = await countRows(instances[0], instances.length, userCount);
  const flowQueueCounts = await getFlowQueueCounts(instances[0]);
  const durationSec = (Date.now() - started) / 1000;

  for (const client of clients) client.socket.disconnect();
  await wait(300);

  return {
    instances: instances.length,
    users: userCount,
    mode: CHAT_LOAD_MODE,
    eventHandlerMode: 'script',
    activeUsers: metrics.activeUsers,
    connected: metrics.connected,
    connectFailed: metrics.connectFailed,
    messages: metrics.attempted,
    acked: metrics.acked,
    ackFailed: metrics.ackFailed,
    delivered: metrics.delivered,
    expectedDeliveries: metrics.expectedDeliveries,
    allDelivered,
    dbRows,
    dbDone,
    flowQueueCounts,
    persistenceRatio: metrics.attempted
      ? Number((dbRows / metrics.attempted).toFixed(3))
      : 1,
    durationSec: Number(durationSec.toFixed(2)),
    messagesPerSec: Number((metrics.attempted / durationSec).toFixed(1)),
    deliveryEventsPerSec: Number((metrics.delivered / durationSec).toFixed(1)),
    ackP95Ms: percentile(metrics.ackLatencies, 95),
    deliveryP95Ms: percentile(metrics.deliveryLatencies, 95),
    eventLoopP95Ms: Number((h.percentile(95) / 1e6).toFixed(1)),
    rssMb: Number((process.memoryUsage().rss / 1024 / 1024).toFixed(1)),
  };
}

function passed(result: any) {
  return (
    result.connected / result.users >= 0.98 &&
    result.acked / result.messages >= 0.99 &&
    result.delivered / result.expectedDeliveries >= 0.99
  );
}

async function main() {
  setupTraceFiles();
  console.log(
    `Real app realtime chat E2E run=${RUN_ID} namespace=${CLIENT_NAMESPACE} instances=${EXTERNAL_URLS.length ? EXTERNAL_INSTANCE_COUNT : INSTANCE_STEPS.join(',')} users=${USER_STEPS.join(',')}`,
  );
  console.log(
    `mode=${CHAT_LOAD_MODE} activeRatio=${ACTIVE_USER_RATIO} durationMs=${REALISTIC_DURATION_MS}`,
  );
  console.log('eventHandlerMode=script');
  console.log(`script flow trigger enabled=${TRIGGER_FLOW}`);
  if (EXTERNAL_URLS.length) {
    console.log(`external websocket targets=${EXTERNAL_URLS.join(',')}`);
  }
  await prepareAppMetadata();
  const allResults: any[] = [];
  const instanceSteps = EXTERNAL_URLS.length
    ? [EXTERNAL_INSTANCE_COUNT]
    : INSTANCE_STEPS;
  for (const instanceCount of instanceSteps) {
    const instances: AppInstance[] = [];
    try {
      if (EXTERNAL_URLS.length) {
        const probeContainer = buildContainer();
        await init(probeContainer);
        instances.push({
          container: probeContainer,
          url: EXTERNAL_URLS[0],
        });
        for (let i = 1; i < Math.max(1, instanceCount); i++) {
          instances.push({
            url: EXTERNAL_URLS[i % EXTERNAL_URLS.length],
          });
        }
      } else {
        for (let i = 0; i < instanceCount; i++) {
          instances.push(await startAppInstance());
        }
      }
      for (const users of USER_STEPS) {
        const result = await runScenario(instances, users);
        allResults.push(result);
        console.log(JSON.stringify(result));
        if (!passed(result)) {
          console.log(
            `Stopped ${instanceCount} instance run at ${users} users due to threshold miss.`,
          );
          break;
        }
      }
    } finally {
      await Promise.all(instances.map((instance) => stopAppInstance(instance)));
    }
  }
  for (const instanceCount of INSTANCE_STEPS) {
    const capacity =
      allResults
        .filter(
          (result) => result.instances === instanceCount && passed(result),
        )
        .at(-1)?.users || 0;
    console.log(
      `Estimated real-app capacity: ${instanceCount} instance(s), ${capacity} concurrent users`,
    );
  }
  const traceSummary = summarizeTrace();
  writeFileSync(
    SUMMARY_FILE,
    JSON.stringify(
      {
        runId: RUN_ID,
        namespace: NAMESPACE,
        results: allResults,
        trace: traceSummary,
      },
      null,
      2,
    ),
  );
  console.log(`Trace summary written to ${SUMMARY_FILE}`);
  if (!KEEP_ROWS) {
    const cleanup = buildContainer();
    await init(cleanup);
    try {
      if (cleanup.cradle.databaseConfigService.isMongoDb()) {
        await cleanup.cradle.mongoService
          .collection(TABLE_NAME)
          .deleteMany({ run_id: RUN_ID } as any);
      } else {
        await cleanup.cradle.queryBuilderService
          .getKnex()(TABLE_NAME)
          .where({ run_id: RUN_ID })
          .delete();
      }
    } finally {
      await shutdown(cleanup);
    }
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
