import { Logger } from '../../../shared/logger';
import { Job, Worker } from 'bullmq';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
import { TDynamicContext } from '../../../shared/types';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { FlowService } from '../../flow/services/flow.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { createFetchHelper } from '../../../shared/helpers/fetch.helper';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { EnvService } from '../../../shared/services/env.service';

export interface ConnectionJobData {
  socketId: string;
  userId: number | string | null;
  clientInfo: any;
  gatewayId: number | string;
  gatewayPath: string;
  script: string;
  timeout: number;
}

export class ConnectionQueueService {
  private readonly logger = new Logger(ConnectionQueueService.name);

  private readonly executorEngineService: ExecutorEngineService;
  private _dynamicWebSocketGateway?: DynamicWebSocketGateway;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly flowService: FlowService;
  private readonly envService: EnvService;
  private _container?: any;
  private worker?: Worker;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    flowService: FlowService;
    envService: EnvService;
    _container?: any;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this._container = deps._container;
    this.repoRegistryService = deps.repoRegistryService;
    this.flowService = deps.flowService;
    this.envService = deps.envService;
  }

  async onInit() {
    const nodeName = this.envService.get('NODE_NAME') || 'enfyra';
    this.worker = new Worker(
      SYSTEM_QUEUES.WS_CONNECTION,
      async (job: Job<ConnectionJobData>) => {
        return await this.process(job);
      },
      {
        prefix: `${nodeName}:`,
        connection: { url: this.envService.get('REDIS_URI'), maxRetriesPerRequest: null },
        concurrency: 5,
      },
    );
  }

  async onDestroy() {
    if (this.worker) {
      await this.worker.close();
    }
  }

  private get dynamicWebSocketGateway(): DynamicWebSocketGateway | undefined {
    if (!this._dynamicWebSocketGateway && this._container) {
      this._dynamicWebSocketGateway = this._container.cradle?.dynamicWebSocketGateway;
    }
    return this._dynamicWebSocketGateway;
  }

  async process(job: Job<ConnectionJobData>): Promise<any> {
    const { socketId, userId, clientInfo, gatewayPath, script, timeout } =
      job.data;

    this.logger.debug(
      `Processing connection script for socket ${socketId} on ${gatewayPath}`,
    );

    const socketProxy = this.createSocketProxy(gatewayPath, socketId);

    const ctx: TDynamicContext = {
      $body: clientInfo || {},
      $data: clientInfo || {},
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
      $repos: {} as any,
      $req: {
        method: 'WS_CONNECT',
        url: gatewayPath,
        ip: clientInfo?.ip,
        headers: clientInfo?.headers,
        user: userId ? { id: userId } : null,
      } as any,
      $share: { $logs: [] },
      $api: {
        request: {
          method: 'WS_CONNECT',
          url: gatewayPath,
          timestamp: new Date().toISOString(),
          ip: clientInfo?.ip,
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

    const result = await this.executorEngineService.run(script, ctx, timeout);

    return { success: true, result };
  }

  onCompleted(job: Job) {
    this.logger.debug(`Connection job ${job.id} completed`);
  }

  onFailed(job: Job | undefined, error: Error) {
    this.logger.error(
      `Connection job ${job?.id} failed:`,
      error,
    );
  }

  onError(error: Error) {
    this.logger.error(`Connection queue error:`, error);
  }

  private createSocketProxy(gatewayPath: string, socketId: string) {
    const self = this;
    return {
      join: (room: string) => {
        self.dynamicWebSocketGateway?.joinRoom(gatewayPath, socketId, room);
      },
      leave: (room: string) => {
        self.dynamicWebSocketGateway?.leaveRoom(gatewayPath, socketId, room);
      },
      reply: (event: string, data: any) => {
        self.dynamicWebSocketGateway?.emitToSocket(gatewayPath, socketId, event, data);
      },
      emitToUser: (userId: number | string, event: string, data: any) => {
        self.dynamicWebSocketGateway?.emitToUser(userId, event, data);
      },
      emitToRoom: (room: string, event: string, data: any) => {
        self.dynamicWebSocketGateway?.emitToRoom(room, event, data);
      },
      emitToGateway: (path: string, event: string, data: any) => {
        self.dynamicWebSocketGateway?.emitToNamespace(path, event, data);
      },
      broadcast: (event: string, data: any) => {
        self.dynamicWebSocketGateway?.emitToAll(event, data);
      },
      disconnect: () => {
        self.dynamicWebSocketGateway?.disconnectSocket(gatewayPath, socketId);
      },
    };
  }
}
