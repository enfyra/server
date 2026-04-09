import { Logger } from '@nestjs/common';
import { Processor, OnWorkerEvent, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
import { TDynamicContext } from '../../../shared/types';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { FlowService } from '../../flow/services/flow.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { createFetchHelper } from '../../../shared/helpers/fetch.helper';

export interface ConnectionJobData {
  socketId: string;
  userId: number | string | null;
  clientInfo: any;
  gatewayId: number | string;
  gatewayPath: string;
  script: string;
  timeout: number;
}

@Processor(SYSTEM_QUEUES.WS_CONNECTION, { concurrency: 50 })
export class ConnectionQueueService extends WorkerHost {
  private readonly logger = new Logger(ConnectionQueueService.name);

  constructor(
    private readonly handlerExecutor: ExecutorEngineService,
    private readonly websocketGateway: DynamicWebSocketGateway,
    private readonly repoRegistryService: RepoRegistryService,
    private readonly flowService: FlowService,
  ) {
    super();
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
    ctx.$dispatch = {
      trigger: (flowIdOrName: string | number, payload?: any) =>
        this.flowService.trigger(
          flowIdOrName,
          payload,
          userId ? { id: userId } : null,
        ),
    };

    const result = await this.handlerExecutor.run(script, ctx, timeout);

    return { success: true, result };
  }

  @OnWorkerEvent('completed')
  onCompleted(job: Job) {
    this.logger.debug(`Connection job ${job.id} completed`);
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job, error: Error) {
    this.logger.error(`Connection job ${job.id} failed:`, error);
  }

  @OnWorkerEvent('error')
  onError(error: Error) {
    this.logger.error(`Connection queue error:`, error);
  }

  private createSocketProxy(gatewayPath: string, socketId: string) {
    const self = this;
    return {
      join: (room: string) => {
        self.websocketGateway.joinRoom(gatewayPath, socketId, room);
      },
      leave: (room: string) => {
        self.websocketGateway.leaveRoom(gatewayPath, socketId, room);
      },
      reply: (event: string, data: any) => {
        self.websocketGateway.emitToSocket(gatewayPath, socketId, event, data);
      },
      emitToUser: (userId: number | string, event: string, data: any) => {
        self.websocketGateway.emitToUser(userId, event, data);
      },
      emitToRoom: (room: string, event: string, data: any) => {
        self.websocketGateway.emitToRoom(room, event, data);
      },
      emitToGateway: (path: string, event: string, data: any) => {
        self.websocketGateway.emitToNamespace(path, event, data);
      },
      broadcast: (event: string, data: any) => {
        self.websocketGateway.emitToAll(event, data);
      },
      disconnect: () => {
        self.websocketGateway.disconnectSocket(gatewayPath, socketId);
      },
    };
  }
}
