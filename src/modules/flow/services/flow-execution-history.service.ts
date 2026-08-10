import { Logger } from '../../../shared/logger';
import type { QueryBuilderService } from '@enfyra/kernel';
import {
  getErrorMessage,
} from '../../../shared/utils/error.util';
import type { FlowDefinition } from '../../../shared/types/flow.types';
import type {
  FlowExecutionHistoryId,
  FlowProgressSnapshot,
} from '../types/flow-execution-history.types';

export class FlowExecutionHistoryService {
  private readonly logger = new Logger(FlowExecutionHistoryService.name);
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async start(
    flow: FlowDefinition,
    payload: any,
    triggeredBy: any,
    startTime: number,
  ): Promise<FlowExecutionHistoryId> {
    try {
      const result = await (this.queryBuilderService as any).insert(
        'enfyra_flow_execution',
        {
          flow: flow.id,
          status: 'running',
          triggeredBy: triggeredBy?.id || null,
          payload: payload || {},
          completedSteps: [],
          currentStep: null,
          error: null,
          startedAt: new Date(startTime),
          completedAt: null,
          duration: null,
        },
      );
      return this.getExecutionHistoryId(result);
    } catch (error: any) {
      this.logger.error(
        `Flow execution history start failed for ${flow.name}: ${getErrorMessage(error)}`,
      );
      return null;
    }
  }

  async finalize(
    flow: FlowDefinition,
    payload: any,
    triggeredBy: any,
    executionHistoryId: FlowExecutionHistoryId,
    finalState: Record<string, any>,
  ): Promise<void> {
    try {
      if (executionHistoryId != null) {
        await (this.queryBuilderService as any).update(
          'enfyra_flow_execution',
          executionHistoryId,
          finalState,
        );
        return;
      }

      await (this.queryBuilderService as any).insert('enfyra_flow_execution', {
        flow: flow.id,
        status: finalState.status,
        triggeredBy: triggeredBy?.id || null,
        payload: payload || {},
        ...finalState,
      });
    } catch (error: any) {
      this.logger.error(
        `Flow execution history finalize failed for ${flow.name}: ${getErrorMessage(error)}`,
      );
    }
  }

  async updateProgress(
    flow: FlowDefinition,
    executionHistoryId: FlowExecutionHistoryId,
    progress: FlowProgressSnapshot,
  ): Promise<void> {
    if (executionHistoryId == null) return;
    try {
      await (this.queryBuilderService as any).update(
        'enfyra_flow_execution',
        executionHistoryId,
        {
          status: 'running',
          completedSteps: progress.completedSteps || [],
          currentStep: progress.currentStep || null,
        },
      );
    } catch (error: any) {
      this.logger.warn(
        `Flow execution progress update failed for ${flow.name}: ${getErrorMessage(error)}`,
      );
    }
  }

  private getExecutionHistoryId(result: any): number | string | null {
    const data = result?.data;
    const record = Array.isArray(data) ? data[0] : data || result;
    return record?.id ?? record?._id ?? null;
  }
}
