import { Logger } from '@nestjs/common';
import { Processor, OnWorkerEvent, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { TDynamicContext } from '../../../shared/types';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { FlowService } from '../../flow/services/flow.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { createFetchHelper } from '../../../shared/helpers/fetch.helper';

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

@Processor(SYSTEM_QUEUES.WS_EVENT, { concurrency: 100 })
export class EventQueueService extends WorkerHost {
  private readonly logger = new Logger(EventQueueService.name);

  constructor(
    private readonly handlerExecutor: HandlerExecutorService,
    private readonly websocketGateway: DynamicWebSocketGateway,
    private readonly repoRegistryService: RepoRegistryService,
    private readonly flowService: FlowService,
  ) {
    super();
  }

  async process(job: Job<EventJobData>): Promise<any> {
    const { requestId, socketId, userId, eventName, payload, gatewayPath, script, timeout } = job.data;

    this.logger.debug(`Processing event ${eventName} for socket ${socketId} on ${gatewayPath}`);

    const socketProxy = this.createSocketProxy(gatewayPath, socketId);

    const ctx: TDynamicContext = {
      $body: payload || {},
      $data: payload || {},
      $statusCode: undefined,
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
    ctx.$dispatch = {
      trigger: (flowIdOrName: string | number, payload?: any) =>
        this.flowService.trigger(flowIdOrName, payload, userId ? { id: userId } : null),
    };

    try {
      const result = await this.handlerExecutor.run(script, ctx, timeout);
      socketProxy.send('ws:result', {
        requestId,
        eventName,
        success: true,
        result,
        logs: ctx.$share?.$logs || [],
      });
      return { success: true, requestId, eventName };
    } catch (error: any) {
      socketProxy.send('ws:error', {
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

  @OnWorkerEvent('completed')
  onCompleted(job: Job) {
    this.logger.debug(`Event job ${job.id} completed`);
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job, error: Error) {
    this.logger.error(`Event job ${job.id} failed:`, error);
  }

  @OnWorkerEvent('error')
  onError(error: Error) {
    this.logger.error(`Event queue error:`, error);
  }

  private createSocketProxy(gatewayPath: string, socketId: string) {
    const self = this;
    return {
      emit: (event: string, data: any) => {
        self.websocketGateway.emitToNamespace(gatewayPath, event, data);
      },
      send: (event: string, data: any) => {
        self.websocketGateway.emitToSocket(gatewayPath, socketId, event, data);
      },
      join: () => {},
      leave: () => {},
      to: (room: string) => ({
        emit: (event: string, data: any) => {
          self.websocketGateway.emitToRoom(room, event, data);
        },
      }),
      close: () => {},
      rooms: new Set<string>(),
    };
  }
}
