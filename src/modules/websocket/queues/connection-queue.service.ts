import { Logger } from '../../../shared/logger';
import { Job, Worker } from 'bullmq';
import { ExecutorEngineService } from '../../../kernel/execution';
import { RepoRegistryService } from '../../../engine/cache';
import { FlowService } from '../../flow';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';
import { EnvService } from '../../../shared/services';
import { DynamicContextFactory } from '../../../shared/services';

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
      SYSTEM_QUEUES.WS_CONNECTION,
      async (job: Job<ConnectionJobData>) => {
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
  }

  async onDestroy() {
    if (this.worker) {
      await this.worker.close();
    }
  }

  async process(job: Job<ConnectionJobData>): Promise<any> {
    const { socketId, userId, clientInfo, gatewayPath, script, timeout } =
      job.data;

    this.logger.debug(
      `Processing connection script for socket ${socketId} on ${gatewayPath}`,
    );

    const ctx = this.dynamicContextFactory.createWebsocketConnection({
      gatewayPath,
      socketId,
      clientInfo,
      user: userId ? { id: userId } : null,
    });
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
    this.logger.error(`Connection job ${job?.id} failed:`, error);
  }

  onError(error: Error) {
    this.logger.error(`Connection queue error:`, error);
  }
}
