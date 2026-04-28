import { Logger } from '../../../shared/logger';
import { Queue } from 'bullmq';
import { FlowCacheService, RepoRegistryService } from '../../../engines/cache';
import { FlowJobData } from '../../../shared/types/flow.types';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { ExecutorEngineService } from '../../../kernel/execution';
import { executeStepCore } from '../utils/step-executor.util';
import { SocketEmitCapture } from '../../websocket';
import { DynamicContextFactory } from '../../../shared/services';
import type { FlowDefinition, FlowStep } from '../../../shared/types/flow.types';

interface FlowStepTestInput {
  id?: number | string;
  stepId?: number | string;
  flowId?: number | string;
  flowName?: string;
  type: string;
  config: any;
  timeout?: number;
  key?: string;
}

export class FlowService {
  private readonly logger = new Logger(FlowService.name);
  private readonly flowQueue: Queue;
  private readonly flowCacheService: FlowCacheService;
  private readonly executorEngineService: ExecutorEngineService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly dynamicContextFactory: DynamicContextFactory;

  constructor(deps: {
    flowQueue: Queue;
    flowCacheService: FlowCacheService;
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    dynamicContextFactory: DynamicContextFactory;
  }) {
    this.flowQueue = deps.flowQueue;
    this.flowCacheService = deps.flowCacheService;
    this.executorEngineService = deps.executorEngineService;
    this.repoRegistryService = deps.repoRegistryService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
  }

  async trigger(
    flowIdOrName: string | number,
    payload?: any,
    triggeredBy?: any,
  ): Promise<{ jobId: string; flowId: number | string }> {
    const asString = String(flowIdOrName);
    const looksLikeId =
      typeof flowIdOrName === 'number' ||
      /^\d+$/.test(asString) ||
      /^[a-f0-9]{24}$/i.test(asString);
    let flow = looksLikeId
      ? await this.flowCacheService.getFlowById(flowIdOrName)
      : null;
    if (!flow) {
      flow = await this.flowCacheService.getFlowByName(asString);
    }

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
    return { jobId: String(job.id), flowId: flow.id };
  }

  async testStep(
    step: FlowStepTestInput,
    mockFlow?: any,
  ): Promise<{
    success: boolean;
    result?: any;
    error?: string;
    duration: number;
    flowContext?: any;
    logs?: any[];
    emitted?: SocketEmitCapture;
  }> {
    const startTime = Date.now();
    const logs: any[] = [];
    const emitted: SocketEmitCapture = [];

    const flowContext: any = {
      $payload: mockFlow?.$payload || {},
      $last: mockFlow?.$last || null,
      $meta: { flowId: 'test', flowName: 'test', executionId: 'test' },
      ...(mockFlow || {}),
    };

    const ctx = this.dynamicContextFactory.createFlowTest({
      payload: {},
      user: null,
      share: { $logs: logs },
      emitted,
    });

    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    (ctx as any).$flow = flowContext;
    (ctx as any).$trigger = async (
      flowIdOrName: string | number,
      triggerPayload?: any,
    ) => ({
      triggered: true,
      flowIdOrName,
      payload: triggerPayload,
      note: 'test mode',
    });

    const MAX_TEST_TIMEOUT = 5000;
    const rawTimeout = Number(step.timeout);
    const timeout =
      Number.isFinite(rawTimeout) && rawTimeout > 0
        ? Math.min(rawTimeout, MAX_TEST_TIMEOUT)
        : MAX_TEST_TIMEOUT;

    try {
      const resolved = await this.resolveTestFlowStep(step);
      const targetStep = resolved?.step ?? step;
      const targetConfig = targetStep.config || {};

      if (resolved) {
        await this.primeFlowTestContext({
          flow: resolved.flow,
          targetStep,
          flowContext,
          ctx,
          timeout,
        });
      }

      let result: any;

      if (targetStep.type === 'trigger_flow') {
        result = {
          triggered: true,
          flowId: targetConfig.flowId,
          flowName: targetConfig.flowName,
          note: 'test mode - not actually triggered',
        };
      } else if (targetStep.type === 'sleep') {
        result = {
          slept: targetConfig.ms || 1000,
          note: 'test mode - not actually sleeping',
        };
      } else {
        result = await executeStepCore({
          type: targetStep.type,
          config: targetStep.config || {},
          timeout,
          ctx,
          executorEngineService: this.executorEngineService,
          shouldTransformCode: true,
        });
      }

      if (targetStep.key) {
        flowContext[targetStep.key] = result;
      }
      flowContext.$last = result;

      return {
        success: true,
        result,
        duration: Date.now() - startTime,
        flowContext,
        logs: ctx.$share?.$logs ?? logs,
        emitted,
      };
    } catch (error) {
      return {
        success: false,
        error: getErrorMessage(error),
        duration: Date.now() - startTime,
        logs: ctx.$share?.$logs ?? logs,
        emitted,
      };
    }
  }

  private async resolveTestFlowStep(
    step: FlowStepTestInput,
  ): Promise<{ flow: FlowDefinition; step: FlowStep } | null> {
    const stepId = step.stepId ?? step.id;
    const flowId = step.flowId;
    const flowName = step.flowName;

    let flows: FlowDefinition[] = [];
    if (flowId !== undefined && flowId !== null) {
      const flow = await this.flowCacheService.getFlowById(flowId);
      if (flow) flows = [flow];
    } else if (flowName) {
      const flow = await this.flowCacheService.getFlowByName(flowName);
      if (flow) flows = [flow];
    } else if (
      stepId !== undefined ||
      step.key ||
      this.getStepSourceCode(step.config)
    ) {
      if (typeof this.flowCacheService.getFlows !== 'function') return null;
      flows = await this.flowCacheService.getFlows();
    }

    for (const flow of flows) {
      const liveStep = (flow.steps || []).find((candidate) =>
        this.isMatchingTestStep(candidate, step, stepId),
      );
      if (!liveStep) continue;

      const mergedStep = {
        ...liveStep,
        type: step.type || liveStep.type,
        config: step.config ?? liveStep.config,
        timeout: step.timeout ?? liveStep.timeout,
        key: liveStep.key,
      } as FlowStep;

      return {
        flow: {
          ...flow,
          steps: flow.steps.map((candidate) =>
            String(candidate.id) === String(liveStep.id)
              ? mergedStep
              : candidate,
          ),
        },
        step: mergedStep,
      };
    }

    return null;
  }

  private isMatchingTestStep(
    candidate: FlowStep,
    step: FlowStepTestInput,
    stepId: number | string | undefined,
  ): boolean {
    if (stepId !== undefined && String(candidate.id) === String(stepId)) {
      return true;
    }

    const candidateSource = this.getStepSourceCode(candidate.config);
    const testSource = this.getStepSourceCode(step.config);
    if (candidateSource && testSource && candidateSource === testSource) {
      return true;
    }

    return !!step.key && candidate.key === step.key;
  }

  private getStepSourceCode(config: any): string {
    if (!config || typeof config !== 'object') return '';
    return String(config.sourceCode ?? config.code ?? config.compiledCode ?? '');
  }

  private async primeFlowTestContext(opts: {
    flow: FlowDefinition;
    targetStep: FlowStep | FlowStepTestInput;
    flowContext: any;
    ctx: any;
    timeout: number;
  }): Promise<void> {
    const { flow, targetStep, flowContext, ctx, timeout } = opts;
    let reachedTarget = false;
    const allSteps = [...(flow.steps || [])]
      .filter((step) => step.isEnabled)
      .sort((a, b) => (a.stepOrder || 0) - (b.stepOrder || 0));
    const rootSteps = allSteps.filter((step) => !step.parentId);
    const getChildren = (parentId: number | string, branch: string) =>
      allSteps.filter(
        (step) =>
          step.parentId &&
          String(step.parentId) === String(parentId) &&
          step.branch === branch,
      );

    const runStep = async (step: FlowStep): Promise<void> => {
      if (reachedTarget) return;
      if (this.isSameFlowStep(step, targetStep)) {
        reachedTarget = true;
        return;
      }

      const result = await this.executeTestStep(step, ctx, timeout);
      flowContext[step.key] = result;
      flowContext.$last = result;

      if (step.type !== 'condition') return;
      const branchSteps = getChildren(step.id, !!result ? 'true' : 'false');
      for (const child of branchSteps) {
        await runStep(child);
      }
    };

    for (const step of rootSteps) {
      await runStep(step);
      if (reachedTarget) return;
    }
  }

  private isSameFlowStep(
    candidate: FlowStep,
    target: FlowStep | FlowStepTestInput,
  ): boolean {
    const targetId = 'id' in target ? target.id : undefined;
    if (targetId !== undefined && String(candidate.id) === String(targetId)) {
      return true;
    }
    return !!target.key && candidate.key === target.key;
  }

  private async executeTestStep(
    step: FlowStep,
    ctx: any,
    timeout: number,
  ): Promise<any> {
    const config = step.config || {};
    if (step.type === 'trigger_flow') {
      return {
        triggered: true,
        flowId: config.flowId,
        flowName: config.flowName,
        note: 'test mode - not actually triggered',
      };
    }
    if (step.type === 'sleep') {
      return {
        slept: config.ms || 1000,
        note: 'test mode - not actually sleeping',
      };
    }
    return executeStepCore({
      type: step.type,
      config,
      timeout,
      ctx,
      executorEngineService: this.executorEngineService,
      shouldTransformCode: true,
    });
  }
}
