import { Injectable, Logger } from '@nestjs/common';
import { KnexService } from '../knex.service';
import { randomUUID } from 'crypto';

export type MigrationStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'rolled_back';

export type MigrationOperation = 'create' | 'update' | 'delete';

export interface MigrationJournalEntry {
  id?: number;
  uuid: string;
  tableName: string;
  operation: MigrationOperation;
  status: MigrationStatus;
  upScript: string | null;
  downScript: string | null;
  beforeSnapshot: any;
  errorMessage: string | null;
  startedAt: Date | null;
  completedAt: Date | null;
  createdAt?: Date;
}

@Injectable()
export class MigrationJournalService {
  private readonly logger = new Logger(MigrationJournalService.name);

  constructor(private readonly knexService: KnexService) {}

  private getKnex() {
    return this.knexService.getKnex();
  }

  async record(params: {
    tableName: string;
    operation: MigrationOperation;
    upScript: string;
    downScript: string;
    beforeSnapshot?: any;
  }): Promise<string> {
    const uuid = `mj-${randomUUID()}`;
    const knex = this.getKnex();

    await knex('schema_migration_definition').insert({
      uuid,
      tableName: params.tableName,
      operation: params.operation,
      status: 'pending',
      upScript: params.upScript || null,
      downScript: params.downScript || null,
      beforeSnapshot: params.beforeSnapshot
        ? JSON.stringify(params.beforeSnapshot)
        : null,
      createdAt: new Date(),
    });

    this.logger.log(
      `Journal recorded: ${uuid} [${params.operation}] ${params.tableName}`,
    );
    return uuid;
  }

  async markRunning(uuid: string): Promise<void> {
    const knex = this.getKnex();
    await knex('schema_migration_definition')
      .where({ uuid })
      .update({ status: 'running', startedAt: new Date() });
  }

  async markCompleted(uuid: string): Promise<void> {
    const knex = this.getKnex();
    await knex('schema_migration_definition')
      .where({ uuid })
      .update({ status: 'completed', completedAt: new Date() });
    this.logger.log(`Journal completed: ${uuid}`);
  }

  async markFailed(uuid: string, error: string): Promise<void> {
    const knex = this.getKnex();
    await knex('schema_migration_definition')
      .where({ uuid })
      .update({
        status: 'failed',
        errorMessage: error?.substring(0, 4000) || 'Unknown error',
        completedAt: new Date(),
      });
    this.logger.warn(`Journal failed: ${uuid} — ${error?.substring(0, 200)}`);
  }

  async markRolledBack(uuid: string): Promise<void> {
    const knex = this.getKnex();
    await knex('schema_migration_definition')
      .where({ uuid })
      .update({ status: 'rolled_back', completedAt: new Date() });
    this.logger.warn(`Journal rolled back: ${uuid}`);
  }

  async executeRollback(uuid: string): Promise<void> {
    const knex = this.getKnex();
    const entry = await knex('schema_migration_definition')
      .where({ uuid })
      .first();

    if (!entry || !entry.downScript) {
      this.logger.warn(
        `No downScript found for journal ${uuid}, skipping rollback`,
      );
      return;
    }

    const statements = entry.downScript
      .split(';')
      .map((s: string) => s.trim())
      .filter((s: string) => s.length > 0);

    this.logger.warn(
      `Executing rollback for ${uuid}: ${statements.length} statement(s)`,
    );

    for (let i = statements.length - 1; i >= 0; i--) {
      const stmt = statements[i];
      try {
        await knex.raw(stmt);
        this.logger.log(`  Rollback [${i + 1}]: ${stmt.substring(0, 80)}`);
      } catch (error: any) {
        this.logger.warn(
          `  Rollback failed [${i + 1}]: ${stmt.substring(0, 80)} — ${error.message}`,
        );
      }
    }

    await this.markRolledBack(uuid);
  }

  async recoverPending(): Promise<void> {
    const knex = this.getKnex();
    let pending: any[];

    try {
      pending = await knex('schema_migration_definition')
        .whereIn('status', ['pending', 'running'])
        .select('*');
    } catch {
      this.logger.warn(
        'schema_migration_definition table not found, skipping recovery',
      );
      return;
    }

    if (pending.length === 0) return;

    this.logger.warn(
      `Found ${pending.length} pending/running migration(s), rolling back...`,
    );

    for (const entry of pending) {
      this.logger.warn(
        `Recovering ${entry.uuid} [${entry.operation}] ${entry.tableName}`,
      );
      try {
        await this.executeRollback(entry.uuid);
        this.logger.warn(
          `Recovery completed for ${entry.uuid} — DDL rolled back, metadata was not changed (DDL-first pattern)`,
        );
      } catch (error: any) {
        this.logger.error(
          `Recovery failed for ${entry.uuid}: ${error.message}`,
        );
      }
    }
  }

  async cleanup(maxAgeDays = 7): Promise<void> {
    const knex = this.getKnex();
    const cutoff = new Date(Date.now() - maxAgeDays * 24 * 60 * 60 * 1000);
    try {
      const deleted = await knex('schema_migration_definition')
        .whereIn('status', ['completed', 'rolled_back'])
        .where('completedAt', '<', cutoff)
        .delete();
      if (deleted > 0) {
        this.logger.log(`Cleaned up ${deleted} old journal entries`);
      }
    } catch {
      // table not found — skip silently
    }
  }
}
