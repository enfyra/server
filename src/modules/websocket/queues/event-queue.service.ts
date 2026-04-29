import { Logger } from '../../../shared/logger';
import { Job, Worker } from 'bullmq';
import { ExecutorEngineService } from '../../../kernel/execution';
import { RepoRegistryService } from '../../../engines/cache';
import { FlowService } from '../../flow';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { EnvService, DynamicContextFactory } from '../../../shared/services';

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
  private readonly dynamicContextFactory: DynamicContextFactory;
  private worker?: Worker;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    flowService: FlowService;
    envService: EnvService;
    dynamicContextFactory: DynamicContextFactory;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.repoRegistryService = deps.repoRegistryService;
    this.flowService = deps.flowService;
    this.envService = deps.envService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
  }

  async init() {
    const nodeName = this.envService.get('NODE_NAME') || 'enfyra';
    this.worker = new Worker(
      SYSTEM_QUEUES.WS_EVENT,
      async (job: Job<EventJobData>) => {
        return await this.process(job);
      },
      {
        prefix: nodeName,
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

    const ctx = this.dynamicContextFactory.createWebsocketEvent({
      gatewayPath,
      socketId,
      eventName,
      payload,
      user: userId ? { id: userId } : null,
    });
    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    ctx.$trigger = (flowIdOrName: string | number, payload?: any) =>
      this.flowService.trigger(
        flowIdOrName,
        payload,
        userId ? { id: userId } : null,
      );

    try {
      const result = await this.executorEngineService.run(script, ctx, timeout);
      ctx.$socket?.reply?.('ws:result', {
        requestId,
        eventName,
        success: true,
        result,
        logs: ctx.$share?.$logs || [],
      });
      return { success: true, requestId, eventName };
    } catch (error: any) {
      ctx.$socket?.reply?.('ws:error', {
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
}
