import { QueryBuilderService } from '@enfyra/kernel';
import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
import type { TCacheInvalidationPayload } from '../../../shared/types/cache.types';

const TABLE_NAME = SYSTEM_TABLES.runtimeReloadLog;

export type RuntimeReloadAuditStatus =
  | 'pending'
  | 'building'
  | 'activated'
  | 'failed';

export type RuntimeReloadAuditStepMetric = {
  name: string;
  durationMs: number;
  status: 'success' | 'failed';
  error?: string;
};

export type RuntimeReloadAuditStartInput = {
  reloadId: string;
  flow: string;
  table: string;
  scope: TCacheInvalidationPayload['scope'];
  action?: TCacheInvalidationPayload['action'];
  chain: string[];
  payload?: TCacheInvalidationPayload;
  instanceId?: string;
};

export type RuntimeReloadAuditFinishInput = {
  reloadId: string;
  status: 'activated' | 'failed';
  durationMs: number;
  steps: RuntimeReloadAuditStepMetric[];
  error?: string;
};

export class RuntimeReloadAuditService {
  private readonly logger = new Logger(RuntimeReloadAuditService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private available: boolean | null = null;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async markBuilding(input: RuntimeReloadAuditStartInput): Promise<boolean> {
    if (!(await this.isAvailable())) return false;

    const now = new Date();
    const row = {
      reloadId: input.reloadId,
      flow: input.flow,
      tableName: input.table,
      scope: input.scope,
      action: input.action ?? 'reload',
      status: 'building',
      chain: input.chain,
      steps: [],
      payload: input.payload ?? null,
      instanceId: input.instanceId ?? null,
      errorMessage: null,
      startedAt: now,
      completedAt: null,
      durationMs: null,
      createdAt: now,
      updatedAt: now,
    };

    try {
      if (this.queryBuilderService.isMongoDb()) {
        await this.queryBuilderService
          .getMongoDb()
          .collection(TABLE_NAME)
          .updateOne(
            { reloadId: input.reloadId },
            { $setOnInsert: row, $set: { status: 'building', updatedAt: now } },
            { upsert: true },
          );
        return true;
      }

      const knex = this.queryBuilderService.getKnex();
      const exists = await knex(TABLE_NAME)
        .where({ reloadId: input.reloadId })
        .first('reloadId');
      if (exists) {
        await knex(TABLE_NAME).where({ reloadId: input.reloadId }).update({
          status: 'building',
          errorMessage: null,
          completedAt: null,
          durationMs: null,
          updatedAt: now,
        });
      } else {
        await knex(TABLE_NAME).insert({
          ...row,
          chain: JSON.stringify(row.chain),
          steps: JSON.stringify(row.steps),
          payload: row.payload ? JSON.stringify(row.payload) : null,
        });
      }
      return true;
    } catch (error) {
      this.available = null;
      this.logger.warn(
        `Runtime reload audit unavailable; continuing without persisted audit: ${getErrorMessage(error)}`,
      );
      return false;
    }
  }

  async markActivated(
    input: Omit<RuntimeReloadAuditFinishInput, 'status'>,
  ): Promise<void> {
    await this.markTerminal({ ...input, status: 'activated' });
  }

  async markFailed(
    input: Omit<RuntimeReloadAuditFinishInput, 'status'>,
  ): Promise<void> {
    await this.markTerminal({ ...input, status: 'failed' });
  }

  async markInterruptedReloadsFailed(reason?: string): Promise<void> {
    if (!(await this.isAvailable())) return;

    const now = new Date();
    const errorMessage =
      reason ?? 'Runtime reload was interrupted before activation';

    try {
      if (this.queryBuilderService.isMongoDb()) {
        await this.queryBuilderService
          .getMongoDb()
          .collection(TABLE_NAME)
          .updateMany(
            { status: { $in: ['pending', 'building'] } },
            {
              $set: {
                status: 'failed',
                errorMessage,
                completedAt: now,
                updatedAt: now,
              },
            },
          );
        return;
      }

      await this.queryBuilderService
        .getKnex()(TABLE_NAME)
        .whereIn('status', ['pending', 'building'])
        .update({
          status: 'failed',
          errorMessage,
          completedAt: now,
          updatedAt: now,
        });
    } catch (error) {
      this.logger.warn(
        `Failed to repair interrupted runtime reload audit rows: ${getErrorMessage(error)}`,
      );
    }
  }

  private async markTerminal(
    input: RuntimeReloadAuditFinishInput,
  ): Promise<void> {
    if (!(await this.isAvailable())) return;

    const now = new Date();
    const patch = {
      status: input.status,
      steps: input.steps,
      errorMessage: input.error ? input.error.substring(0, 4000) : null,
      completedAt: now,
      durationMs: input.durationMs,
      updatedAt: now,
    };

    try {
      if (this.queryBuilderService.isMongoDb()) {
        await this.queryBuilderService
          .getMongoDb()
          .collection(TABLE_NAME)
          .updateOne(
            { reloadId: input.reloadId },
            { $set: patch },
            { upsert: false },
          );
        return;
      }

      await this.queryBuilderService
        .getKnex()(TABLE_NAME)
        .where({ reloadId: input.reloadId })
        .update({
          ...patch,
          steps: JSON.stringify(patch.steps),
        });
    } catch (error) {
      this.logger.warn(
        `Failed to persist runtime reload audit terminal status for ${input.reloadId}: ${getErrorMessage(error)}`,
      );
    }
  }

  private async isAvailable(): Promise<boolean> {
    if (this.available !== null) return this.available;
    try {
      if (this.queryBuilderService.isMongoDb()) {
        const exists = await this.queryBuilderService
          .getMongoDb()
          .listCollections({ name: TABLE_NAME }, { nameOnly: true })
          .hasNext();
        this.available = exists ? true : null;
        return exists;
      }

      const exists = await this.queryBuilderService
        .getKnex()
        .schema.hasTable(TABLE_NAME);
      this.available = exists ? true : null;
      return exists;
    } catch (error) {
      this.available = null;
      this.logger.warn(
        `Runtime reload audit table check failed: ${getErrorMessage(error)}`,
      );
      return false;
    }
  }
}
