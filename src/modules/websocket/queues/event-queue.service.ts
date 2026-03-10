import { Logger } from '@nestjs/common';
import { Processor, OnWorkerEvent, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';

export interface EventJobData {
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

@Processor('ws-event', { concurrency: 100 })
export class EventQueueService extends WorkerHost {
  private readonly logger = new Logger(EventQueueService.name);

  constructor(
    private readonly handlerExecutor: HandlerExecutorService,
    private readonly websocketGateway: DynamicWebSocketGateway,
  ) {
    super();
  }

  async process(job: Job<EventJobData>): Promise<any> {
    const { socketId, userId, eventName, payload, gatewayPath, script, timeout } = job.data;

    this.logger.debug(`Processing event ${eventName} for socket ${socketId} on ${gatewayPath}`);

    const socketProxy = this.createSocketProxy(gatewayPath, socketId);

    const ctx: TDynamicContext = {
      $body: payload || {},
      $data: payload || {},
      $statusCode: undefined,
      $throw: this.createThrowHandlers(),
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

    const result = await this.handlerExecutor.run(script, ctx, timeout);

    return { success: true, result };
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

  private createThrowHandlers() {
    return {
      '400': (msg: string) => { throw new Error(`Bad Request: ${msg}`); },
      '401': (msg: string) => { throw new Error(`Unauthorized: ${msg}`); },
      '403': (msg: string) => { throw new Error(`Forbidden: ${msg}`); },
      '404': (msg: string) => { throw new Error(`Not Found: ${msg}`); },
      '409': (msg: string) => { throw new Error(`Conflict: ${msg}`); },
      '422': (msg: string) => { throw new Error(`Unprocessable Entity: ${msg}`); },
      '429': (msg: string) => { throw new Error(`Too Many Requests: ${msg}`); },
      '500': (msg: string) => { throw new Error(`Internal Server Error: ${msg}`); },
      '503': (msg: string) => { throw new Error(`Service Unavailable: ${msg}`); },
    };
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
