import { Logger } from '../../../shared/logger';
import type { QueryBuilderService } from '@enfyra/kernel';
import type {
  RuntimeSchemaJournalEntry,
  RuntimeSchemaJournalStage,
} from '../types/runtime-schema-executor.types';

const TABLE_NAME = 'enfyra_runtime_schema_journal';

export class RuntimeSchemaJournalService {
  private readonly logger = new Logger(RuntimeSchemaJournalService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private tableReady = false;

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
      if (stage === 'failed' || stage === 'rolled_back') {
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
    completedNodeIds?: string[],
  ): Promise<void> {
    const store = await this.getStore();
    const fields: Record<string, unknown> = { stage };
    if (completedNodeIds) {
      fields.completedNodeIds = this.queryBuilderService.isMongoDb()
        ? completedNodeIds
        : JSON.stringify(completedNodeIds);
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
    const details = unresolved
      .map(
        (entry: any) =>
          `${entry.mutationId} (stage=${entry.stage}, backend=${entry.backend})`,
      )
      .join('; ');
    throw new Error(
      `Unresolved runtime schema mutations found, boot cannot continue: ${details}`,
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
