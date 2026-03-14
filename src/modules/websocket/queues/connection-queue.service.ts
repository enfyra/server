import { Injectable, Logger } from '@nestjs/common';
import { Processor, OnWorkerEvent, WorkerHost } from '@nestjs/bullmq';
import { Job } from 'bullmq';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { TDynamicContext } from '../../../shared/types';
import { DynamicWebSocketGateway } from '../gateway/dynamic-websocket.gateway';

export interface ConnectionJobData {
  socketId: string;
  userId: number | string | null;
  clientInfo: any;
  gatewayId: number | string;
  gatewayPath: string;
  script: string;
  timeout: number;
}

@Processor('ws-connection', { concurrency: 50 })
export class ConnectionQueueService extends WorkerHost {
  private readonly logger = new Logger(ConnectionQueueService.name);

  constructor(
    private readonly handlerExecutor: HandlerExecutorService,
    private readonly websocketGateway: DynamicWebSocketGateway,
  ) {
    super();
  }

  async process(job: Job<ConnectionJobData>): Promise<any> {
    const { socketId, userId, clientInfo, gatewayPath, script, timeout } = job.data;

    this.logger.debug(`Processing connection script for socket ${socketId} on ${gatewayPath}`);

    const socketProxy = this.createSocketProxy(gatewayPath, socketId);

    const ctx: TDynamicContext = {
      $body: clientInfo || {},
      $data: clientInfo || {},
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
    const emitToClient = (event: string, data: any) => {
      self.websocketGateway.emitToSocket(gatewayPath, socketId, event, data);
    };
    return {
      emit: emitToClient,
      send: emitToClient,
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
