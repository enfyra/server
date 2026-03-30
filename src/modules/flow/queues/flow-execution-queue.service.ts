import { Logger } from '@nestjs/common';
import { Processor, OnWorkerEvent, WorkerHost, InjectQueue } from '@nestjs/bullmq';
import { Job, Queue } from 'bullmq';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { FlowCacheService } from '../../../infrastructure/cache/services/flow-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { TDynamicContext } from '../../../shared/types';
import { FlowDefinition, FlowStep, FlowJobData } from '../../../shared/types/flow.types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { executeStepCore } from '../utils/step-executor.util';

export type { FlowJobData } from '../../../shared/types/flow.types';

const MAX_FLOW_DEPTH = 10;
const MAX_STEP_TIMEOUT = 300000;
const MAX_PAYLOAD_SIZE = 1024 * 1024;

@Processor('flow-execution', { concurrency: 20 })
export class FlowExecutionQueueService extends WorkerHost {
  private readonly logger = new Logger(FlowExecutionQueueService.name);

  constructor(
    private readonly handlerExecutor: HandlerExecutorService,
    private readonly repoRegistryService: RepoRegistryService,
    private readonly flowCacheService: FlowCacheService,
    private readonly queryBuilder: QueryBuilderService,
    @InjectQueue('flow-execution') private readonly flowQueue: Queue,
  ) {
    super();
  }

  async process(job: Job<FlowJobData>): Promise<any> {
    const { flowId, flowName, payload, triggeredBy, depth = 0, visitedFlowIds = [] } = job.data;

    if (depth > MAX_FLOW_DEPTH) {
      throw new Error(`Max flow nesting depth (${MAX_FLOW_DEPTH}) exceeded for flow ${flowName || flowId}`);
    }

    if (payload && JSON.stringify(payload).length > MAX_PAYLOAD_SIZE) {
      throw new Error(`Flow payload exceeds maximum size of ${MAX_PAYLOAD_SIZE} bytes`);
    }

    const flow = flowName
      ? await this.flowCacheService.getFlowByName(flowName)
      : await this.flowCacheService.getFlowById(flowId);

    if (!flow) {
      throw new Error(`Flow ${flowName || flowId} not found`);
    }

    if (visitedFlowIds.includes(flow.id)) {
      throw new Error(`Circular flow detected: flow "${flow.name}" (${flow.id}) already visited`);
    }

    const currentVisited = [...visitedFlowIds, flow.id];

    const executionId = await this.createExecution(flow, payload, triggeredBy);

    const startTime = Date.now();

    try {
      await this.updateExecution(executionId, { status: 'running', startedAt: new Date() });

      const result = await this.executeFlow(flow, payload, triggeredBy, executionId, job, depth, currentVisited);

      await this.updateExecution(executionId, {
        status: 'completed',
        completedAt: new Date(),
        duration: Date.now() - startTime,
        context: result.context,
        completedSteps: result.completedSteps,
      });

      this.cleanupOldExecutions(flow).catch((err) => this.logger.warn(`Cleanup failed for flow ${flow.name}: ${err.message}`));
      return { success: true, executionId, context: result.context };
    } catch (error) {
      await this.updateExecution(executionId, {
        status: 'failed',
        completedAt: new Date(),
        duration: Date.now() - startTime,
        error: { message: error.message, stack: error.stack },
      });

      this.cleanupOldExecutions(flow).catch((err) => this.logger.warn(`Cleanup failed for flow ${flow.name}: ${err.message}`));
      throw error;
    }
  }

  private async cleanupOldExecutions(flow: FlowDefinition): Promise<void> {
    const maxExecutions = flow.maxExecutions || 100;
    try {
      const countResult = await this.queryBuilder.select({
        tableName: 'flow_execution_definition',
        filter: { flow: { _eq: flow.id } },
        fields: ['id'],
        limit: 1,
        meta: 'total_count',
      });
      const total = countResult.meta?.total_count || 0;
      if (total <= maxExecutions) return;

      const deleteCount = Math.min(total - maxExecutions, 200);
      const oldResult = await this.queryBuilder.select({
        tableName: 'flow_execution_definition',
        filter: { flow: { _eq: flow.id } },
        sort: ['startedAt'],
        fields: ['id'],
        limit: deleteCount,
      });
      const toDelete = oldResult.data || [];
      for (const row of toDelete) {
        await this.queryBuilder.deleteById('flow_execution_definition', row.id || row._id);
      }
    } catch (err) {
      this.logger.warn(`Failed to cleanup old executions for flow ${flow.name}: ${err.message}`);
    }
  }

  private async executeFlow(
    flow: FlowDefinition,
    payload: any,
    triggeredBy: any,
    executionId: number | string,
    job: Job,
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

    const ctx: TDynamicContext = {
      $body: payload || {},
      $query: {},
      $params: {},
      $user: triggeredBy || null,
      $repos: {},
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $helpers: {},
      $cache: {},
      $share: { $logs: [] },
      $logs: (...args: any[]) => {
        (ctx.$share.$logs as any[]).push(...args);
      },
    };

    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    (ctx as any).$flow = flowContext;
    (ctx as any).$dispatch = {
      trigger: async (flowIdOrName: string | number, triggerPayload?: any) => {
        const targetFlow = typeof flowIdOrName === 'number' || /^\d+$/.test(String(flowIdOrName))
          ? await this.flowCacheService.getFlowById(flowIdOrName)
          : await this.flowCacheService.getFlowByName(String(flowIdOrName));
        if (!targetFlow) throw new Error(`Flow "${flowIdOrName}" not found`);
        await this.flowQueue.add(`flow:${targetFlow.name}`, {
          flowId: targetFlow.id,
          flowName: targetFlow.name,
          payload: triggerPayload,
          depth: depth + 1,
          visitedFlowIds,
        });
        return { triggered: true, flowId: targetFlow.id, flowName: targetFlow.name };
      },
    };

    const completedSteps: any[] = [];
    const allSteps = [...flow.steps].sort((a, b) => a.stepOrder - b.stepOrder);
    const rootSteps = allSteps.filter((s) => !s.parentId);

    const getChildren = (parentId: number | string, branch: string) =>
      allSteps.filter((s) => s.parentId && String(s.parentId) === String(parentId) && s.branch === branch);

    const runStep = async (step: FlowStep): Promise<void> => {
      if (!step.isEnabled) return;

      await this.updateExecution(executionId, { currentStep: step.key });
      const stepStart = Date.now();

      try {
        const result = await this.executeStep(step, ctx, flow.timeout, visitedFlowIds);
        flowContext[step.key] = result;
        flowContext.$last = result;

        const entry: any = { key: step.key, type: step.type, duration: Date.now() - stepStart };

        if (step.type === 'condition') {
          const branchValue = !!result ? 'true' : 'false';
          entry.branch = branchValue;
          completedSteps.push(entry);

          const branchSteps = getChildren(step.id, branchValue);
          for (const child of branchSteps) {
            await runStep(child);
          }
        } else {
          completedSteps.push(entry);
        }

        await job.updateProgress({
          completedSteps,
          currentStep: step.key,
          totalSteps: allSteps.length,
        });
      } catch (error) {
        if (step.onError === 'retry' && step.retryAttempts > 0) {
          let retrySuccess = false;
          for (let i = 0; i < step.retryAttempts; i++) {
            try {
              const backoffMs = Math.min(1000 * Math.pow(2, i), 30000);
              await new Promise((r) => setTimeout(r, backoffMs));
              const result = await this.executeStep(step, ctx, flow.timeout, visitedFlowIds);
              flowContext[step.key] = result;
              flowContext.$last = result;
              completedSteps.push({ key: step.key, type: step.type, duration: Date.now() - stepStart, retries: i + 1 });
              retrySuccess = true;
              break;
            } catch (retryErr) {
              if (i === step.retryAttempts - 1) {
                this.logger.warn(`Step "${step.key}" failed after ${step.retryAttempts} retries: ${retryErr.message}`);
              }
            }
          }
          if (!retrySuccess) throw error;
        } else if (step.onError === 'skip') {
          flowContext[step.key] = { error: error.message, skipped: true };
          flowContext.$last = flowContext[step.key];
          completedSteps.push({ key: step.key, type: step.type, status: 'skipped', error: error.message, duration: Date.now() - stepStart });
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

  private async executeStep(step: FlowStep, ctx: TDynamicContext, flowTimeout: number, visitedFlowIds: (number | string)[] = []): Promise<any> {
    const raw = step.timeout || flowTimeout || 5000;
    const timeout = Math.min(Math.max(raw, 1), MAX_STEP_TIMEOUT);
    const config = step.config || {};

    if (step.type === 'trigger_flow') {
      const targetFlow = await this.flowCacheService.getFlowById(config.flowId)
        || await this.flowCacheService.getFlowByName(config.flowName);
      if (!targetFlow) throw new Error(`Target flow ${config.flowId || config.flowName} not found`);
      const childPayload = config.payload || (ctx as any).$flow?.$last || {};
      const currentDepth = (ctx as any).$flow?.$meta?.depth || 0;
      await this.flowQueue.add(`flow:${targetFlow.name}`, {
        flowId: targetFlow.id,
        flowName: targetFlow.name,
        payload: childPayload,
        depth: currentDepth + 1,
        visitedFlowIds,
      });
      return { triggered: true, flowId: targetFlow.id, flowName: targetFlow.name };
    }

    return executeStepCore({ type: step.type, config, timeout, ctx, handlerExecutor: this.handlerExecutor });
  }

  private async createExecution(flow: FlowDefinition, payload: any, triggeredBy: any): Promise<number | string> {
    const record = await this.queryBuilder.insertAndGet('flow_execution_definition', {
      flowId: flow.id,
      status: 'pending',
      payload: payload || null,
      ...(triggeredBy?.id ? { triggeredById: triggeredBy.id } : {}),
    });
    return record.id || record._id;
  }

  private async updateExecution(executionId: number | string, data: any): Promise<void> {
    await this.queryBuilder.updateById('flow_execution_definition', executionId, data);
  }

  @OnWorkerEvent('failed')
  onFailed(job: Job, error: Error) {
    this.logger.error(`Flow job ${job.id} failed: ${error.message}`);
  }

  @OnWorkerEvent('error')
  onError(error: Error) {
    this.logger.error(`Flow queue error: ${error.message}`);
  }
}
