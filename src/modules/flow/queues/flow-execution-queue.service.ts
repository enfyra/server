import { Logger } from '../../../shared/logger';
import { Job, Queue } from 'bullmq';
import { ExecutorEngineService } from '../../../infrastructure/executor-engine/services/executor-engine.service';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { FlowCacheService } from '../../../infrastructure/cache/services/flow-cache.service';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { WebsocketEmitService } from '../../websocket/services/websocket-emit.service';
import { TDynamicContext } from '../../../shared/types';
import {
  FlowDefinition,
  FlowStep,
  FlowJobData,
} from '../../../shared/types/flow.types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { executeStepCore } from '../utils/step-executor.util';

export type { FlowJobData } from '../../../shared/types/flow.types';

const MAX_FLOW_DEPTH = 10;
const MAX_STEP_TIMEOUT = 300000;
const MAX_PAYLOAD_SIZE = 1024 * 1024;

export class FlowExecutionQueueService {
  private readonly logger = new Logger(FlowExecutionQueueService.name);
  private readonly executorEngineService: ExecutorEngineService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly flowCacheService: FlowCacheService;
  private readonly queryBuilderService: QueryBuilderService;
  private readonly websocketEmitService: WebsocketEmitService;
  private readonly flowQueue: Queue;

  constructor(deps: {
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    flowCacheService: FlowCacheService;
    queryBuilderService: QueryBuilderService;
    websocketEmitService: WebsocketEmitService;
    flowQueue: Queue;
  }) {
    this.executorEngineService = deps.executorEngineService;
    this.repoRegistryService = deps.repoRegistryService;
    this.flowCacheService = deps.flowCacheService;
    this.queryBuilderService = deps.queryBuilderService;
    this.websocketEmitService = deps.websocketEmitService;
    this.flowQueue = deps.flowQueue;
  }

  async process(job: Job<FlowJobData>): Promise<any> {
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

    let flow = await resolveFlow();

    if (!flow) {
      await this.flowCacheService.reload();
      flow = await resolveFlow();
    }

    if (!flow) {
      throw new Error(`Flow ${flowName || flowId} not found`);
    }

    if (visitedFlowIds.includes(flow.id)) {
      throw new Error(
        `Circular flow detected: flow "${flow.name}" (${flow.id}) already visited`,
      );
    }

    const currentVisited = [...visitedFlowIds, flow.id];

    const executionId = await this.createExecution(flow, payload, triggeredBy);

    this.emitFlowEvent(triggeredBy, {
      executionId,
      flowId: flow.id,
      flowName: flow.name,
      status: 'pending',
    });

    const startTime = Date.now();

    try {
      await this.updateExecution(executionId, {
        status: 'running',
        startedAt: new Date(),
      });

      this.emitFlowEvent(triggeredBy, {
        executionId,
        flowId: flow.id,
        flowName: flow.name,
        status: 'running',
      });

      const result = await this.executeFlow(
        flow,
        payload,
        triggeredBy,
        executionId,
        job,
        depth,
        currentVisited,
      );

      await this.updateExecution(executionId, {
        status: 'completed',
        completedAt: new Date(),
        duration: Date.now() - startTime,
        context: result.context,
        completedSteps: result.completedSteps,
      });

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
      return { success: true, executionId, context: result.context };
    } catch (error) {
      await this.updateExecution(executionId, {
        status: 'failed',
        completedAt: new Date(),
        duration: Date.now() - startTime,
        error: { message: error.message, stack: error.stack },
      });

      this.emitFlowEvent(triggeredBy, {
        executionId,
        flowId: flow.id,
        flowName: flow.name,
        status: 'failed',
        duration: Date.now() - startTime,
        error: error.message,
      });

      this.cleanupOldExecutions(flow).catch((err) =>
        this.logger.warn(
          `Cleanup failed for flow ${flow.name}: ${err.message}`,
        ),
      );
      return { success: false, executionId, error: error.message };
    }
  }

  private emitFlowEvent(triggeredBy: any, data: any) {
    if (!triggeredBy?.id) return;
    this.websocketEmitService.emitToUser(triggeredBy.id, 'flow:execution', data);
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
        await this.queryBuilderService.delete('flow_execution_definition', record.id);
      }
    } catch (err) {
      this.logger.warn(
        `Failed to cleanup old executions for flow ${flow.name}: ${(err as Error).message}`,
      );
    }
  }

  private async createExecution(
    flow: FlowDefinition,
    payload: any,
    triggeredBy: any,
  ): Promise<string> {
    const execution = await this.queryBuilderService.insert('flow_execution_definition', {
      flow: flow.id,
      status: 'pending',
      triggeredBy: triggeredBy?.id || null,
      payload: payload || {},
      startedAt: new Date(),
    });
    return execution.id || execution._id;
  }

  private async updateExecution(
    executionId: number | string,
    updates: any,
  ): Promise<void> {
    await this.queryBuilderService.update('flow_execution_definition', executionId as any, updates);
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
    } as any;

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
      await this.flowQueue.add(`flow:${targetFlow.name}`, {
        flowId: targetFlow.id,
        flowName: targetFlow.name,
        payload: triggerPayload,
        depth: depth + 1,
        visitedFlowIds,
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

      await this.updateExecution(executionId, { currentStep: step.key });
      const stepStart = Date.now();

      try {
        const result = await this.executeStep(
          step,
          ctx,
          (flow as any).timeout,
          visitedFlowIds,
        );
        flowContext[step.key] = result;
        flowContext.$last = result;

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

        await job.updateProgress({
          completedSteps,
          currentStep: step.key,
          totalSteps: allSteps.length,
        });
      } catch (error: any) {
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
          flowContext[step.key] = { error: error.message, skipped: true };
          flowContext.$last = flowContext[step.key];
          completedSteps.push({
            key: step.key,
            type: step.type,
            status: 'skipped',
            error: error.message,
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
    const config = step.config || {};

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
