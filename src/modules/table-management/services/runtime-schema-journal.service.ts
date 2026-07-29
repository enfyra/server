import { Logger } from '../../../shared/logger';
import type { QueryBuilderService } from '@enfyra/kernel';
import type {
  RuntimeSchemaJournalAdvanceOptions,
  RuntimeSchemaJournalEntry,
  RuntimeSchemaJournalStage,
} from '../types/runtime-schema-executor.types';

const TABLE_NAME = 'enfyra_runtime_schema_journal';

export class RuntimeSchemaJournalService {
  private readonly logger = new Logger(RuntimeSchemaJournalService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private tableReady = false;
  private mongoIndexReady = false;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  private async getStore(): Promise<{
    insert: (doc: Record<string, unknown>) => Promise<void>;
    update: (
      mutationId: string,
      fields: Record<string, unknown>,
    ) => Promise<void>;
    find: (mutationId: string) => Promise<Record<string, unknown> | null>;
    findByStage: (
      stages: string[],
    ) => Promise<Record<string, unknown>[]>;
  }> {
    if (this.queryBuilderService.isMongoDb()) {
      const db = this.queryBuilderService.getMongoDb();
      const col = db.collection(TABLE_NAME);
      if (!this.mongoIndexReady) {
        await col.createIndex({ mutationId: 1 }, { unique: true });
        this.mongoIndexReady = true;
      }
      return {
        insert: async (doc) => {
          await col.insertOne(doc as any);
        },
        update: async (mutationId, fields) => {
          await col.updateOne(
            { mutationId },
            { $set: { ...fields, updatedAt: new Date().toISOString() } },
          );
        },
        find: async (mutationId) => {
          return (await col.findOne({ mutationId })) as any;
        },
        findByStage: async (stages) => {
          return (await col
            .find({ stage: { $in: stages } })
            .toArray()) as any[];
        },
      };
    }
    await this.ensureSqlTable();
    const knex = this.queryBuilderService.getKnex();
    return {
      insert: async (doc) => {
        await knex(TABLE_NAME).insert(doc);
      },
      update: async (mutationId, fields) => {
        await knex(TABLE_NAME)
          .where({ mutationId })
          .update({ ...fields, updatedAt: new Date().toISOString() });
      },
      find: async (mutationId) => {
        return (await knex(TABLE_NAME).where({ mutationId }).first()) as any;
      },
      findByStage: async (stages) => {
        return (await knex(TABLE_NAME)
          .whereIn('stage', stages)
          .select('*')) as any[];
      },
    };
  }

  private async ensureSqlTable(): Promise<void> {
    if (this.tableReady) return;
    const knex = this.queryBuilderService.getKnex();
    const exists = await knex.schema.hasTable(TABLE_NAME);
    if (!exists) {
      try {
        await knex.schema.createTable(TABLE_NAME, (table: any) => {
          table.string('mutationId', 128).primary();
          table.string('contractHash', 128).notNullable();
          table.string('backend', 32).notNullable();
          table.string('stage', 64).notNullable();
          table.string('startedAt', 64).notNullable();
          table.string('updatedAt', 64).notNullable();
          table.text('completedNodeIds').nullable();
          table.text('error').nullable();
        });
      } catch (error: any) {
        const code = String(error?.code ?? '');
        const msg = String(error?.message ?? '').toLowerCase();
        const alreadyExists =
          code === '42P07' ||
          code === '42710' ||
          msg.includes('already exists') ||
          msg.includes('duplicate key');
        if (!alreadyExists) throw error;
      }
    }
    this.tableReady = true;
  }

  async create(entry: {
    mutationId: string;
    contractHash: string;
    backend: string;
  }): Promise<void> {
    const store = await this.getStore();
    const existing = await store.find(entry.mutationId);
    if (existing) {
      const stage = existing.stage as string;
      if (stage === 'failed' || stage === 'rolled_back' || stage === 'completed') {
        await store.update(entry.mutationId, {
          stage: 'captured' as RuntimeSchemaJournalStage,
          contractHash: entry.contractHash,
          error: null,
        });
        return;
      }
      throw new Error(
        `Runtime schema mutation ${entry.mutationId} already in progress at stage=${stage}`,
      );
    }
    const now = new Date().toISOString();
    await store.insert({
      mutationId: entry.mutationId,
      contractHash: entry.contractHash,
      backend: entry.backend,
      stage: 'captured' as RuntimeSchemaJournalStage,
      startedAt: now,
      updatedAt: now,
      completedNodeIds: this.queryBuilderService.isMongoDb()
        ? []
        : JSON.stringify([]),
    });
  }

  async advanceStage(
    mutationId: string,
    stage: RuntimeSchemaJournalStage,
    options?: RuntimeSchemaJournalAdvanceOptions,
  ): Promise<void> {
    const store = await this.getStore();
    const fields: Record<string, unknown> = { stage };
    if (options?.completedNodeIds) {
      fields.completedNodeIds = this.queryBuilderService.isMongoDb()
        ? options.completedNodeIds
        : JSON.stringify(options.completedNodeIds);
    }
    if (options?.sagaSessionId && this.queryBuilderService.isMongoDb()) {
      fields.sagaSessionId = options.sagaSessionId;
    }
    await store.update(mutationId, fields);
  }

  async markFailed(mutationId: string, error: string): Promise<void> {
    const store = await this.getStore();
    await store.update(mutationId, {
      stage: 'failed' as RuntimeSchemaJournalStage,
      error: error.substring(0, 4000),
    });
  }

  async markCompleted(mutationId: string): Promise<void> {
    const store = await this.getStore();
    await store.update(mutationId, {
      stage: 'completed' as RuntimeSchemaJournalStage,
    });
  }

  async markRecoveredRollbacks(mutationIds: readonly string[]): Promise<void> {
    if (mutationIds.length === 0) return;
    const store = await this.getStore();
    const unresolved = new Set([
      'captured',
      'executing',
      'target_attested',
      'db_committed',
      'activation_pending',
    ]);
    for (const mutationId of new Set(mutationIds)) {
      const entry = await store.find(mutationId);
      if (!entry || !unresolved.has(String(entry.stage))) continue;
      await store.update(mutationId, {
        stage: 'rolled_back' as RuntimeSchemaJournalStage,
        error: 'recovered: MySQL durable snapshot restored the pre-mutation database state',
      });
    }
  }

  async recoverUnresolved(): Promise<void> {
    const store = await this.getStore();
    const unresolvedStages = [
      'captured',
      'executing',
      'target_attested',
      'db_committed',
      'activation_pending',
    ];
    const unresolved = await store.findByStage(unresolvedStages);
    if (unresolved.length === 0) return;

    const dbCommittedStages = new Set(['target_attested', 'db_committed', 'activation_pending']);
    const failures: string[] = [];

    for (const entry of unresolved) {
      const e = entry as any;
      try {
        await this.assertNoLiveMongoSaga(e);
        if (dbCommittedStages.has(e.stage)) {
          await store.update(e.mutationId, {
            stage: 'activation_pending' as RuntimeSchemaJournalStage,
            error: `recovered: db was committed before crash at stage=${e.stage}; waiting for boot cache activation`,
          });
          this.logger.warn(
            `Recovered runtime journal ${e.mutationId}: db_committed at stage=${e.stage}, activation remains pending`,
          );
        } else {
          await store.update(e.mutationId, {
            stage: 'failed' as RuntimeSchemaJournalStage,
            error: `recovered: crash at stage=${e.stage}, db not committed`,
          });
          this.logger.warn(
            `Recovered runtime journal ${e.mutationId}: crash at stage=${e.stage}, marked failed`,
          );
        }
      } catch (err: any) {
        failures.push(`${e.mutationId}: ${err.message}`);
      }
    }

    if (failures.length > 0) {
      throw new Error(
        `Runtime journal recovery failed for ${failures.length} mutation(s): ${failures.join('; ')}`,
      );
    }
  }

  async completeRecoveredActivations(): Promise<void> {
    const store = await this.getStore();
    const pending = await store.findByStage(['activation_pending']);
    for (const entry of pending) {
      await store.update(String(entry.mutationId), {
        stage: 'completed' as RuntimeSchemaJournalStage,
        error: 'recovered: boot cache activation completed',
      });
    }
  }

  private async assertNoLiveMongoSaga(entry: any): Promise<void> {
    if (!this.queryBuilderService.isMongoDb()) return;
    const db = this.queryBuilderService.getMongoDb();
    const session = await db.collection('system_saga_sessions').findOne({
      $or: [
        ...(entry.sagaSessionId
          ? [
              { txId: entry.sagaSessionId },
              { sessionId: entry.sagaSessionId },
            ]
          : []),
        { mutationId: entry.mutationId },
      ],
    });
    if (!session) return;
    const leaseExpiresAt = session.expiresAt
      ? new Date(session.expiresAt).getTime()
      : 0;
    const live =
      ['active', 'committing', 'rolling_back'].includes(session.status) &&
      leaseExpiresAt > Date.now();
    throw new Error(
      live
        ? `Correlated runtime saga ${session.txId} is still live`
        : `Correlated runtime saga ${session.txId} requires saga recovery before runtime journal recovery`,
    );
  }

  async cleanup(maxAgeDays = 7): Promise<void> {
    const cutoff = new Date(
      Date.now() - maxAgeDays * 24 * 60 * 60 * 1000,
    ).toISOString();
    try {
      if (this.queryBuilderService.isMongoDb()) {
        const db = this.queryBuilderService.getMongoDb();
        await db.collection(TABLE_NAME).deleteMany({
          stage: { $in: ['completed', 'rolled_back'] },
          updatedAt: { $lt: cutoff },
        });
        return;
      }
      await this.ensureSqlTable();
      const knex = this.queryBuilderService.getKnex();
      await knex(TABLE_NAME)
        .whereIn('stage', ['completed', 'rolled_back'])
        .where('updatedAt', '<', cutoff)
        .delete();
    } catch {}
  }
}
