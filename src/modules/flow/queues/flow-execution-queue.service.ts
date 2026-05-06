import { Logger } from '../../../shared/logger';
import { Job, Queue, Worker } from 'bullmq';
import { appendFileSync, existsSync, mkdirSync, statSync } from 'node:fs';
import { dirname } from 'node:path';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import {
  ExecutorEngineService,
  type IsolatedExecutorService,
} from '@enfyra/kernel';
import { RepoRegistryService, FlowCacheService } from '../../../engines/cache';
import {
  getErrorMessage,
  getErrorStack,
} from '../../../shared/utils/error.util';
import { QueryBuilderService } from '@enfyra/kernel';
import { WebsocketEmitService } from '../../websocket';
import { TDynamicContext } from '../../../shared/types';
import {
  FlowDefinition,
  FlowStep,
  FlowJobData,
} from '../../../shared/types/flow.types';
import {
  executeStepCore,
  getExecutableStepConfig,
} from '../utils/step-executor.util';
import {
  DynamicContextFactory,
  EnvService,
  RuntimeMetricsCollectorService,
} from '../../../shared/services';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

export type { FlowJobData } from '../../../shared/types/flow.types';

const MAX_FLOW_DEPTH = 10;
const MAX_STEP_TIMEOUT = 300000;
const MAX_PAYLOAD_SIZE = 1024 * 1024;
const DEFAULT_MAX_CONCURRENCY_MULTIPLIER = 2;

type FlowWorkerConcurrencyTuning = {
  mode: 'adaptive' | 'fixed';
  initial: number;
  min: number;
  max: number;
  intervalMs: number;
  maxEventLoopLagMs: number;
};

export class FlowExecutionQueueService {
  private readonly logger = new Logger(FlowExecutionQueueService.name);
  private readonly executorEngineService: ExecutorEngineService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly flowCacheService: FlowCacheService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly websocketEmitService: WebsocketEmitService;
  private readonly dynamicContextFactory: DynamicContextFactory;
  private readonly envService: EnvService;
  private readonly isolatedExecutorService: IsolatedExecutorService;
  private readonly runtimeMetricsCollectorService?: RuntimeMetricsCollectorService;
  private readonly flowQueue: Queue;
  private readonly traceFile?: string;
  private readonly flowWorkerEventLoopDelay = monitorEventLoopDelay({
    resolution: 20,
  });
  private worker?: Worker;
  private tuneInterval?: NodeJS.Timeout;
  private tuning?: FlowWorkerConcurrencyTuning;
  private currentConcurrency = 0;
  private idleTuneTicks = 0;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    flowCacheService: FlowCacheService;
    queryBuilderService: QueryBuilderService;
    websocketEmitService: WebsocketEmitService;
    dynamicContextFactory: DynamicContextFactory;
    envService: EnvService;
    isolatedExecutorService: IsolatedExecutorService;
    runtimeMetricsCollectorService?: RuntimeMetricsCollectorService;
    flowQueue: Queue;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.repoRegistryService = deps.repoRegistryService;
    this.flowCacheService = deps.flowCacheService;
    this.queryBuilderService = deps.queryBuilderService;
    this.websocketEmitService = deps.websocketEmitService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
    this.envService = deps.envService;
    this.isolatedExecutorService = deps.isolatedExecutorService;
    this.runtimeMetricsCollectorService = deps.runtimeMetricsCollectorService;
    this.flowQueue = deps.flowQueue;
    const traceFile = this.envService.get('FLOW_WORKER_TRACE_FILE');
    this.traceFile =
      traceFile && (!existsSync(traceFile) || !statSync(traceFile).isDirectory())
        ? traceFile
        : undefined;
    if (this.traceFile) {
      mkdirSync(dirname(this.traceFile), { recursive: true });
    }
  }

  async init() {
    if (this.worker) return;

    const nodeName = this.envService.get('NODE_NAME') || 'enfyra';
    const tuning = this.resolveConcurrencyTuning();
    this.tuning = tuning;
    this.currentConcurrency = tuning.initial;
    this.worker = new Worker(
      SYSTEM_QUEUES.FLOW_EXECUTION,
      async (job: Job<FlowJobData>) => {
        if (!this.runtimeMetricsCollectorService) {
          return await this.process(job);
        }
        return await this.runtimeMetricsCollectorService.runWithQueryContext(
          'flow',
          () => this.process(job),
        );
      },
      {
        prefix: nodeName,
        connection: {
          url: this.envService.get('REDIS_URI'),
          maxRetriesPerRequest: null,
        },
        concurrency: tuning.initial,
      },
    );

    this.worker.on('failed', (job, err) => {
      this.logger.error(`Flow job ${job?.id} failed: ${err.message}`);
    });
    this.worker.on('completed', (job) => {
      this.logger.debug(`Flow job ${job.id} completed`);
    });
    this.worker.on('error', (err) => {
      this.logger.error(`Flow queue worker error: ${err.message}`);
    });
    this.logger.log(
      `Flow queue worker started on ${SYSTEM_QUEUES.FLOW_EXECUTION} with concurrency ${tuning.initial}`,
    );
    this.startAdaptiveConcurrencyTuning();
  }

  private resolveConcurrencyTuning(): FlowWorkerConcurrencyTuning {
    const configured = this.envService.get('FLOW_WORKER_CONCURRENCY');
    const executorWorkers =
      this.isolatedExecutorService.getMetrics().tuning.maxConcurrentWorkers;
    const sqlPoolMax = this.envService.get('SQL_POOL_MAX') || 4;
    const computed = Math.max(
      1,
      Math.min(sqlPoolMax + 1, executorWorkers * 2 + 1),
    );
    const initial = configured || computed;
    const min = Math.min(
      initial,
      this.envService.get('FLOW_WORKER_CONCURRENCY_MIN') || initial,
    );
    const configuredMax = this.envService.get('FLOW_WORKER_CONCURRENCY_MAX');
    const defaultMax = Math.max(
      initial,
      Math.min(32, initial * DEFAULT_MAX_CONCURRENCY_MULTIPLIER),
    );
    const max = Math.max(initial, configuredMax || defaultMax);
    return {
      mode: this.envService.get('FLOW_WORKER_CONCURRENCY_MODE'),
      initial,
      min,
      max,
      intervalMs: this.envService.get('FLOW_WORKER_TUNE_INTERVAL_MS'),
      maxEventLoopLagMs: this.envService.get(
        'FLOW_WORKER_TUNE_MAX_EVENT_LOOP_LAG_MS',
      ),
    };
  }

  private startAdaptiveConcurrencyTuning(): void {
    const tuning = this.tuning;
    if (!this.worker || !tuning || tuning.mode === 'fixed') return;
    if (tuning.max <= tuning.min) return;

    this.flowWorkerEventLoopDelay.enable();
    this.tuneInterval = setInterval(() => {
      void this.tuneWorkerConcurrency().catch((error) => {
        this.logger.warn(
          `Flow worker concurrency tuning failed: ${getErrorMessage(error)}`,
        );
      });
    }, tuning.intervalMs);
    this.tuneInterval.unref?.();
  }

  private async tuneWorkerConcurrency(): Promise<void> {
    const worker = this.worker;
    const tuning = this.tuning;
    if (!worker || !tuning || tuning.mode !== 'adaptive') return;

    const counts = await this.flowQueue.getJobCounts(
      'waiting',
      'active',
      'delayed',
    );
    const waiting = counts.waiting ?? 0;
    const active = counts.active ?? 0;
    const delayed = counts.delayed ?? 0;
    const eventLoopLagMs = this.readAndResetFlowWorkerEventLoopLag();
    const executorMetrics = this.isolatedExecutorService.getMetrics();
    const executorWaiting = executorMetrics.pool.waitingTasks;
    const current = this.currentConcurrency;
    let next = current;

    const backlogPressure =
      waiting > current * 8 || (waiting > current && active >= current);
    const hasHeadroom =
      eventLoopLagMs < tuning.maxEventLoopLagMs && executorWaiting === 0;
    const eventLoopPressure =
      eventLoopLagMs > tuning.maxEventLoopLagMs * 1.6;

    if (eventLoopPressure && current > tuning.min) {
      next = Math.max(tuning.min, current - 1);
      this.idleTuneTicks = 0;
    } else if (backlogPressure && hasHeadroom && current < tuning.max) {
      next = Math.min(tuning.max, current + 1);
      this.idleTuneTicks = 0;
    } else if (waiting === 0 && delayed === 0 && active < current / 2) {
      this.idleTuneTicks += 1;
      if (this.idleTuneTicks >= 3 && current > tuning.min) {
        next = Math.max(tuning.min, current - 1);
        this.idleTuneTicks = 0;
      }
    } else {
      this.idleTuneTicks = 0;
    }

    if (next === current) return;

    worker.concurrency = next;
    this.currentConcurrency = next;
    this.trace('flow-worker-concurrency', {
      previous: current,
      next,
      waiting,
      active,
      delayed,
      eventLoopLagMs,
      executorWaiting,
    });
    this.logger.log(
      `Flow worker concurrency tuned ${current} -> ${next} (waiting=${waiting}, active=${active}, eventLoopLagMs=${eventLoopLagMs.toFixed(1)})`,
    );
  }

  private readAndResetFlowWorkerEventLoopLag(): number {
    const mean = this.flowWorkerEventLoopDelay.mean / 1e6;
    this.flowWorkerEventLoopDelay.reset();
    return Number.isFinite(mean) ? mean : 0;
  }

  private trace(event: string, data: Record<string, any>): void {
    if (!this.traceFile) return;
    appendFileSync(
      this.traceFile,
      JSON.stringify({ event, ts: Date.now(), ...data }) + '\n',
    );
  }

  async onDestroy() {
    if (this.worker) {
      await this.worker.close();
      this.worker = undefined;
    }
    if (this.tuneInterval) {
      clearInterval(this.tuneInterval);
      this.tuneInterval = undefined;
    }
    this.flowWorkerEventLoopDelay.disable();
  }

  async process(job: Job<FlowJobData>): Promise<any> {
    const processStarted = Date.now();
    const {
      flowId,
      flowName,
      payload,
      triggeredBy,
      depth = 0,
      visitedFlowIds = [],
    } = job.data;

    if (depth > MAX_FLOW_DEPTH) {
      throw new Error(
        `Max flow nesting depth (${MAX_FLOW_DEPTH}) exceeded for flow ${flowName || flowId}`,
      );
    }

    if (payload && JSON.stringify(payload).length > MAX_PAYLOAD_SIZE) {
      throw new Error(
        `Flow payload exceeds maximum size of ${MAX_PAYLOAD_SIZE} bytes`,
      );
    }

    const resolveFlow = async (): Promise<FlowDefinition | null> => {
      return flowName
        ? await this.flowCacheService.getFlowByName(flowName)
        : await this.flowCacheService.getFlowById(flowId);
    };

    const resolveStarted = Date.now();
    let flow = await resolveFlow();

    if (!flow) {
      await this.flowCacheService.reload();
      flow = await resolveFlow();
    }
    const resolveFlowMs = Date.now() - resolveStarted;

    if (!flow) {
      throw new Error(`Flow ${flowName || flowId} not found`);
    }

    if (visitedFlowIds.includes(flow.id)) {
      throw new Error(
        `Circular flow detected: flow "${flow.name}" (${flow.id}) already visited`,
      );
    }

    const currentVisited = [...visitedFlowIds, flow.id];
    const executionId = String(job.id ?? `${flow.id}:${Date.now()}`);

    this.emitFlowEvent(triggeredBy, {
      executionId,
      flowId: flow.id,
      flowName: flow.name,
      status: 'pending',
    });

    const startTime = Date.now();
    this.runtimeMetricsCollectorService?.startFlow(flow.id, flow.name);

    try {
      this.emitFlowEvent(triggeredBy, {
        executionId,
        flowId: flow.id,
        flowName: flow.name,
        status: 'running',
      });

      const executeFlowStarted = Date.now();
      const result = await this.executeFlow(
        flow,
        payload,
        triggeredBy,
        executionId,
        job,
        depth,
        currentVisited,
      );
      const executeFlowMs = Date.now() - executeFlowStarted;

      const historyEnqueueStarted = Date.now();
      this.enqueueExecutionHistory(flow, payload, triggeredBy, {
        status: 'completed',
        startedAt: new Date(startTime),
        completedAt: new Date(),
        duration: Date.now() - startTime,
        context: result.context,
        completedSteps: result.completedSteps,
        currentStep: result.context?.$meta?.currentStep || null,
      });
      const historyEnqueueMs = Date.now() - historyEnqueueStarted;

      this.emitFlowEvent(triggeredBy, {
        executionId,
        flowId: flow.id,
        flowName: flow.name,
        status: 'completed',
        duration: Date.now() - startTime,
      });

      this.cleanupOldExecutions(flow).catch((err) =>
        this.logger.warn(
          `Cleanup failed for flow ${flow.name}: ${err.message}`,
        ),
      );
      this.runtimeMetricsCollectorService?.completeFlow({
        flowId: flow.id,
        flowName: flow.name,
        durationMs: Date.now() - startTime,
        status: 'completed',
      });
      this.trace('flow_job', {
        jobId: job.id,
        flowId: flow.id,
        flowName: flow.name,
        status: 'completed',
        resolveFlowMs,
        executeFlowMs,
        historyEnqueueMs,
        totalMs: Date.now() - processStarted,
      });
      return { success: true, executionId, context: result.context };
    } catch (error) {
      const historyEnqueueStarted = Date.now();
      this.enqueueExecutionHistory(flow, payload, triggeredBy, {
        status: 'failed',
        startedAt: new Date(startTime),
        completedAt: new Date(),
        duration: Date.now() - startTime,
        error: { message: getErrorMessage(error), stack: getErrorStack(error) },
      });
      const historyEnqueueMs = Date.now() - historyEnqueueStarted;

      this.emitFlowEvent(triggeredBy, {
        executionId,
        flowId: flow.id,
        flowName: flow.name,
        status: 'failed',
        duration: Date.now() - startTime,
        error: getErrorMessage(error),
      });

      this.cleanupOldExecutions(flow).catch((err) =>
        this.logger.warn(
          `Cleanup failed for flow ${flow.name}: ${getErrorMessage(err)}`,
        ),
      );
      this.runtimeMetricsCollectorService?.completeFlow({
        flowId: flow.id,
        flowName: flow.name,
        durationMs: Date.now() - startTime,
        status: 'failed',
      });
      this.trace('flow_job', {
        jobId: job.id,
        flowId: flow.id,
        flowName: flow.name,
        status: 'failed',
        resolveFlowMs,
        historyEnqueueMs,
        totalMs: Date.now() - processStarted,
        error: getErrorMessage(error),
      });
      return { success: false, executionId, error: getErrorMessage(error) };
    }
  }

  private emitFlowEvent(triggeredBy: any, data: any) {
    if (!triggeredBy?.id) return;
    this.websocketEmitService.emitToUser(
      triggeredBy.id,
      'flow:execution',
      data,
    );
  }

  private async cleanupOldExecutions(flow: FlowDefinition): Promise<void> {
    const maxExecutions = flow.maxExecutions || 100;
    try {
      const countResult = await this.queryBuilderService.find({
        table: 'flow_execution_definition',
        filter: { flow: { _eq: flow.id } },
        fields: ['id'],
        limit: 1,
        meta: 'total_count',
      });
      const total =
        countResult.meta?.total_count ?? countResult.meta?.totalCount ?? 0;
      if (total <= maxExecutions) return;

      const deleteCount = Math.min(total - maxExecutions, 200);
      const oldResult = await this.queryBuilderService.find({
        table: 'flow_execution_definition',
        filter: { flow: { _eq: flow.id } },
        sort: ['startedAt'],
        fields: ['id'],
        limit: deleteCount,
      });

      for (const record of oldResult.data || []) {
        await this.queryBuilderService.delete(
          'flow_execution_definition',
          record.id,
        );
      }
    } catch (err) {
      this.logger.warn(
        `Failed to cleanup old executions for flow ${flow.name}: ${(err as Error).message}`,
      );
    }
  }

  private enqueueExecutionHistory(
    flow: FlowDefinition,
    payload: any,
    triggeredBy: any,
    finalState: Record<string, any>,
  ): void {
    void (this.queryBuilderService as any)
      .insert(
        'flow_execution_definition',
        {
          flow: flow.id,
          status: finalState.status,
          triggeredBy: triggeredBy?.id || null,
          payload: payload || {},
          ...finalState,
        },
        { batch: true },
      )
      .catch((error: any) =>
        this.logger.error(
          `Flow execution history enqueue failed for ${flow.name}: ${getErrorMessage(error)}`,
        ),
      );
  }

  private async executeFlow(
    flow: FlowDefinition,
    payload: any,
    triggeredBy: any,
    executionId: number | string,
    job: Job<FlowJobData>,
    depth: number,
    visitedFlowIds: (number | string)[],
  ): Promise<{ context: any; completedSteps: any[] }> {
    const flowContext: any = {
      $payload: payload || {},
      $last: null,
      $meta: {
        flowId: flow.id,
        flowName: flow.name,
        executionId,
        depth,
        startedAt: new Date().toISOString(),
      },
    };

    const ctx = this.dynamicContextFactory.createFlow({
      payload,
      user: triggeredBy || null,
    }) as TDynamicContext;

    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    (ctx as any).$flow = flowContext;
    (ctx as any).$trigger = async (
      flowIdOrName: string | number,
      triggerPayload?: any,
    ) => {
      const targetFlow =
        typeof flowIdOrName === 'number' || /^\d+$/.test(String(flowIdOrName))
          ? await this.flowCacheService.getFlowById(flowIdOrName)
          : await this.flowCacheService.getFlowByName(String(flowIdOrName));
      if (!targetFlow) throw new Error(`Flow "${flowIdOrName}" not found`);
      const sourceStepKey = (ctx as any).$flow?.$meta?.currentStep;
      await this.flowQueue.add(`flow:${targetFlow.name}`, {
        flowId: targetFlow.id,
        flowName: targetFlow.name,
        payload: triggerPayload,
        depth: depth + 1,
        visitedFlowIds,
        sourceFlowId: flow.id,
        sourceFlowName: flow.name,
        sourceStepKey,
      });
      return {
        triggered: true,
        flowId: targetFlow.id,
        flowName: targetFlow.name,
      };
    };

    const completedSteps: any[] = [];
    const allSteps = [...(flow.steps || [])].sort(
      (a, b) => (a as any).stepOrder - (b as any).stepOrder,
    );
    const rootSteps = allSteps.filter((s) => !(s as any).parentId);

    const getChildren = (parentId: number | string, branch: string) =>
      allSteps.filter(
        (s) =>
          (s as any).parentId &&
          String((s as any).parentId) === String(parentId) &&
          (s as any).branch === branch,
      );

    const runStep = async (step: FlowStep): Promise<void> => {
      if (!step.isEnabled) return;

      flowContext.$meta.currentStep = step.key;
      const stepStart = Date.now();

      try {
        const executeStepStarted = Date.now();
        const result = await this.executeStep(
          step,
          ctx,
          (flow as any).timeout,
          visitedFlowIds,
        );
        const executeStepMs = Date.now() - executeStepStarted;
        flowContext[step.key] = result;
        flowContext.$last = result;
        this.trace('flow_step', {
          jobId: job.id,
          executionId,
          flowId: flow.id,
          flowName: flow.name,
          stepKey: step.key,
          stepType: step.type,
          status: 'completed',
          updateCurrentStepMs: 0,
          executeStepMs,
          totalMs: Date.now() - stepStart,
        });

        const entry: any = {
          key: step.key,
          type: step.type,
          duration: Date.now() - stepStart,
        };

        if (step.type === 'condition') {
          const branchValue = !!result ? 'true' : 'false';
          entry.branch = branchValue;
          completedSteps.push(entry);

          const branchSteps = getChildren((step as any).id, branchValue);
          for (const child of branchSteps) {
            await runStep(child);
          }
        } else {
          completedSteps.push(entry);
        }

        job
          .updateProgress({
            completedSteps,
            currentStep: step.key,
            totalSteps: allSteps.length,
          })
          .catch(() => {});
        this.runtimeMetricsCollectorService?.recordFlowStep({
          flowId: flow.id,
          flowName: flow.name,
          stepKey: step.key,
          durationMs: Date.now() - stepStart,
        });
      } catch (error: any) {
        this.trace('flow_step', {
          jobId: job.id,
          executionId,
          flowId: flow.id,
          flowName: flow.name,
          stepKey: step.key,
          stepType: step.type,
          status: 'failed',
          updateCurrentStepMs: 0,
          totalMs: Date.now() - stepStart,
          error: getErrorMessage(error),
        });
        try {
          job
            .updateProgress({
              completedSteps,
              currentStep: step.key,
              failedStep: step.key,
              totalSteps: allSteps.length,
            })
            .catch(() => {});
        } catch {}
        this.runtimeMetricsCollectorService?.recordFlowStep({
          flowId: flow.id,
          flowName: flow.name,
          stepKey: step.key,
          durationMs: Date.now() - stepStart,
          failed: true,
        });
        if (step.onError === 'retry' && (step as any).retryAttempts > 0) {
          const retryAttempts = (step as any).retryAttempts as number;
          let retrySuccess = false;
          for (let i = 0; i < retryAttempts; i++) {
            try {
              const backoffMs = Math.min(1000 * Math.pow(2, i), 30000);
              await new Promise((r) => setTimeout(r, backoffMs));
              const result = await this.executeStep(
                step,
                ctx,
                (flow as any).timeout,
                visitedFlowIds,
              );
              flowContext[step.key] = result;
              flowContext.$last = result;
              completedSteps.push({
                key: step.key,
                type: step.type,
                duration: Date.now() - stepStart,
                retries: i + 1,
              });
              retrySuccess = true;
              break;
            } catch (retryErr: any) {
              if (i === retryAttempts - 1) {
                this.logger.warn(
                  `Step "${step.key}" failed after ${retryAttempts} retries: ${retryErr.message}`,
                );
              }
            }
          }
          if (!retrySuccess) throw error;
        } else if (step.onError === 'skip') {
          flowContext[step.key] = {
            error: getErrorMessage(error),
            skipped: true,
          };
          flowContext.$last = flowContext[step.key];
          completedSteps.push({
            key: step.key,
            type: step.type,
            status: 'skipped',
            error: getErrorMessage(error),
            duration: Date.now() - stepStart,
          });
          return;
        } else {
          throw error;
        }
      }
    };

    for (const step of rootSteps) {
      await runStep(step);
    }

    return { context: flowContext, completedSteps };
  }

  private async executeStep(
    step: FlowStep,
    ctx: TDynamicContext,
    flowTimeout: number,
    visitedFlowIds: (number | string)[] = [],
  ): Promise<any> {
    const raw = (step as any).timeout || flowTimeout || 5000;
    const timeout = Math.min(Math.max(raw, 1), MAX_STEP_TIMEOUT);
    const config = getExecutableStepConfig(step);

    if (step.type === 'trigger_flow') {
      const targetFlow =
        (await this.flowCacheService.getFlowById((config as any).flowId)) ||
        (await this.flowCacheService.getFlowByName((config as any).flowName));
      if (!targetFlow)
        throw new Error(
          `Target flow ${(config as any).flowId || (config as any).flowName} not found`,
        );
      const childPayload =
        (config as any).payload || (ctx as any).$flow?.$last || {};
      const currentDepth = (ctx as any).$flow?.$meta?.depth || 0;
      await this.flowQueue.add(`flow:${targetFlow.name}`, {
        flowId: targetFlow.id,
        flowName: targetFlow.name,
        payload: childPayload,
        depth: currentDepth + 1,
        visitedFlowIds,
        sourceFlowId: (ctx as any).$flow?.$meta?.flowId,
        sourceFlowName: (ctx as any).$flow?.$meta?.flowName,
        sourceStepKey: step.key,
      });
      return {
        triggered: true,
        flowId: targetFlow.id,
        flowName: targetFlow.name,
      };
    }

    return executeStepCore({
      type: step.type,
      config,
      timeout,
      ctx,
      executorEngineService: this.executorEngineService,
    });
  }
}
