import { Logger } from '../../../shared/logger';
import { Job, Worker } from 'bullmq';
import { ExecutorEngineService } from '../../../engine/executor-engine/services/executor-engine.service';
import { TDynamicContext } from '../../../shared/types';
import type { Cradle } from '../../../container';
import { RepoRegistryService } from '../../../engine/cache/services/repo-registry.service';
import { FlowService } from '../../flow/services/flow.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { createFetchHelper } from '../../../shared/helpers/fetch.helper';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { EnvService } from '../../../shared/services/env.service';

export interface EventJobData {
  requestId: string;
  socketId: string;
  userId: number | string | null;
  eventName: string;
  payload: any;
  gatewayId: number | string;
  gatewayPath: string;
  eventId: number | string;
  script: string;
  timeout: number;
}

export class EventQueueService {
  private readonly logger = new Logger(EventQueueService.name);

  private readonly executorEngineService: ExecutorEngineService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly flowService: FlowService;
  private readonly envService: EnvService;
  private readonly lazyRef: Cradle;
  private worker?: Worker;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    flowService: FlowService;
    envService: EnvService;
    lazyRef: Cradle;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.lazyRef = deps.lazyRef;
    this.repoRegistryService = deps.repoRegistryService;
    this.flowService = deps.flowService;
    this.envService = deps.envService;
  }

  async init() {
    const nodeName = this.envService.get('NODE_NAME') || 'enfyra';
    this.worker = new Worker(
      SYSTEM_QUEUES.WS_EVENT,
      async (job: Job<EventJobData>) => {
        return await this.process(job);
      },
      {
        prefix: `${nodeName}:`,
        connection: {
          url: this.envService.get('REDIS_URI'),
          maxRetriesPerRequest: null,
        },
        concurrency: 5,
      },
    );
    this.worker.on('failed', (job, err) => {
      this.logger.error(`Event job ${job?.id} failed: ${err.message}`);
    });
    this.worker.on('completed', (job) => {
      this.logger.debug(`Event job ${job?.id} completed`);
    });
    this.logger.log(`Event queue worker started on ${SYSTEM_QUEUES.WS_EVENT}`);
  }

  async onDestroy() {
    if (this.worker) {
      await this.worker.close();
    }
  }

  async process(job: Job<EventJobData>): Promise<any> {
    const {
      requestId,
      socketId,
      userId,
      eventName,
      payload,
      gatewayPath,
      script,
      timeout,
    } = job.data;

    this.logger.debug(
      `Processing event ${eventName} for socket ${socketId} on ${gatewayPath}`,
    );

    const socketProxy = this.createSocketProxy(gatewayPath, socketId);

    const ctx: TDynamicContext = {
      $body: payload || {},
      $data: payload || {},
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $logs: (...args: any[]) => {
        const logsArray = (ctx.$share?.$logs as any[]) || [];
        logsArray.push(...args);
      },
      $helpers: {},
      $cache: {},
      $params: {},
      $query: {},
      $user: userId ? { id: userId } : null,
      $repos: {},
      $req: {
        method: 'WS_EVENT',
        url: gatewayPath,
        ip: null,
        headers: {},
        user: userId ? { id: userId } : null,
      } as any,
      $share: { $logs: [] },
      $api: {
        request: {
          method: 'WS_EVENT',
          url: `${gatewayPath}/${eventName}`,
          timestamp: new Date().toISOString(),
        },
      },
      $socket: socketProxy,
    };

    ctx.$helpers.$fetch = createFetchHelper();
    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    ctx.$trigger = (flowIdOrName: string | number, payload?: any) =>
      this.flowService.trigger(
        flowIdOrName,
        payload,
        userId ? { id: userId } : null,
      );

    try {
      const result = await this.executorEngineService.run(script, ctx, timeout);
      socketProxy.reply('ws:result', {
        requestId,
        eventName,
        success: true,
        result,
        logs: ctx.$share?.$logs || [],
      });
      return { success: true, requestId, eventName };
    } catch (error: any) {
      socketProxy.reply('ws:error', {
        requestId,
        eventName,
        success: false,
        code: error?.errorCode || error?.code || 'WS_HANDLER_ERROR',
        message: error?.message || 'Websocket handler failed',
        logs: ctx.$share?.$logs || [],
        details: error?.details,
      });
      return { success: false, requestId, eventName };
    }
  }

  onCompleted(job: Job) {
    this.logger.debug(`Event job ${job.id} completed`);
  }

  onFailed(job: Job | undefined, error: Error) {
    this.logger.error(`Event job ${job?.id} failed:`, error);
  }

  onError(error: Error) {
    this.logger.error(`Event queue error:`, error);
  }

  private createSocketProxy(gatewayPath: string, socketId: string) {
    return {
      join: (room: string) => {
        this.lazyRef.dynamicWebSocketGateway?.joinRoom(
          gatewayPath,
          socketId,
          room,
        );
      },
      leave: (room: string) => {
        this.lazyRef.dynamicWebSocketGateway?.leaveRoom(
          gatewayPath,
          socketId,
          room,
        );
      },
      reply: (event: string, data: any) => {
        this.lazyRef.dynamicWebSocketGateway?.emitToSocket(
          gatewayPath,
          socketId,
          event,
          data,
        );
      },
      emitToUser: (userId: number | string, event: string, data: any) => {
        this.lazyRef.dynamicWebSocketGateway?.emitToUser(userId, event, data);
      },
      emitToRoom: (room: string, event: string, data: any) => {
        this.lazyRef.dynamicWebSocketGateway?.emitToRoom(room, event, data);
      },
      emitToGateway: (path: string, event: string, data: any) => {
        this.lazyRef.dynamicWebSocketGateway?.emitToNamespace(
          path,
          event,
          data,
        );
      },
      broadcast: (event: string, data: any) => {
        this.lazyRef.dynamicWebSocketGateway?.emitToAll(event, data);
      },
      roomSize: async (room: string): Promise<number> => {
        const gateway = this.lazyRef.dynamicWebSocketGateway;
        return gateway ? gateway.roomSize(room) : 0;
      },
    };
  }
}
