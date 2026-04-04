import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bullmq';
import { Queue } from 'bullmq';
import { FlowCacheService } from '../../../infrastructure/cache/services/flow-cache.service';
import { FlowJobData } from '../../../shared/types/flow.types';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { TDynamicContext } from '../../../shared/types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { executeStepCore } from '../utils/step-executor.util';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

@Injectable()
export class FlowService {
  private readonly logger = new Logger(FlowService.name);

  constructor(
    @InjectQueue(SYSTEM_QUEUES.FLOW_EXECUTION) private readonly flowQueue: Queue,
    private readonly flowCacheService: FlowCacheService,
    private readonly handlerExecutor: ExecutorEngineService,
    private readonly repoRegistryService: RepoRegistryService,
  ) {}

  async trigger(flowIdOrName: string | number, payload?: any, triggeredBy?: any): Promise<{ jobId: string; flowId: number | string }> {
    const flow = typeof flowIdOrName === 'number' || /^\d+$/.test(String(flowIdOrName))
      ? await this.flowCacheService.getFlowById(flowIdOrName)
      : await this.flowCacheService.getFlowByName(String(flowIdOrName));

    if (!flow) {
      throw new Error(`Flow "${flowIdOrName}" not found`);
    }

    if (!flow.isEnabled) {
      throw new Error(`Flow "${flow.name}" is disabled`);
    }

    const jobData: FlowJobData = {
      flowId: flow.id,
      flowName: flow.name,
      payload: payload,
      triggeredBy,
    };

    const job = await this.flowQueue.add(`flow:${flow.name}`, jobData, {
      attempts: 1,
      removeOnComplete: { count: 200, age: 3600 * 24 },
      removeOnFail: { count: 500, age: 3600 * 24 * 7 },
    });

    this.logger.log(`Flow "${flow.name}" triggered, job ${job.id}`);
    return { jobId: job.id, flowId: flow.id };
  }

  async testStep(step: { type: string; config: any; timeout?: number }, mockFlow?: any): Promise<{ success: boolean; result?: any; error?: string; duration: number }> {
    const startTime = Date.now();
    const logs: any[] = [];

    const flowContext = {
      $payload: mockFlow?.$payload || {},
      $last: mockFlow?.$last || null,
      $meta: { flowId: 'test', flowName: 'test', executionId: 'test' },
      ...(mockFlow || {}),
    };

    const ctx: TDynamicContext = {
      $body: {},
      $query: {},
      $params: {},
      $user: null,
      $repos: {},
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $helpers: {},
      $cache: {},
      $share: { $logs: logs },
      $logs: (...args: any[]) => logs.push(...args),
    };

    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    (ctx as any).$flow = flowContext;
    (ctx as any).$dispatch = {
      trigger: async (flowIdOrName: string | number, triggerPayload?: any) =>
        ({ triggered: true, flowIdOrName, payload: triggerPayload, note: 'test mode' }),
    };

    const MAX_TEST_TIMEOUT = 5000;
    const config = step.config || {};
    const rawTimeout = Number(step.timeout);
    const timeout = Number.isFinite(rawTimeout) && rawTimeout > 0 ? Math.min(rawTimeout, MAX_TEST_TIMEOUT) : MAX_TEST_TIMEOUT;

    try {
      let result: any;

      if (step.type === 'trigger_flow') {
        result = { triggered: true, flowId: config.flowId, flowName: config.flowName, note: 'test mode - not actually triggered' };
      } else if (step.type === 'sleep') {
        result = { slept: config.ms || 1000, note: 'test mode - not actually sleeping' };
      } else {
        result = await executeStepCore({ type: step.type, config, timeout, ctx, handlerExecutor: this.handlerExecutor, shouldTransformCode: true });
      }

      return { success: true, result, duration: Date.now() - startTime };
    } catch (error) {
      return { success: false, error: error.message, duration: Date.now() - startTime };
    }
  }
}
