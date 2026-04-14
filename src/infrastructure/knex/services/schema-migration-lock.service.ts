import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { Knex } from 'knex';
import { KnexService } from '../knex.service';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { DatabaseException } from '../../../core/exceptions/custom-exceptions';

export interface SchemaMigrationLockHandle {
  token: string;
  dbType: string;
}

type KnexLike = Knex | Knex.Transaction;

@Injectable()
export class SchemaMigrationLockService {
  private readonly logger = new Logger(SchemaMigrationLockService.name);
  private readonly lockName = 'schema_migration_lock';
  private readonly tableName = 'schema_migration_lock';
  private lockTableReady = false;

  constructor(
    private readonly knexService: KnexService,
    private readonly queryBuilderService: QueryBuilderService,
  ) {}

  async acquire(context: string): Promise<SchemaMigrationLockHandle> {
    const knex = this.knexService.getKnex();
    const dbType = this.queryBuilderService.getDatabaseType() || 'mysql';
    const token = randomUUID();
    const lockedBy = this.buildInstanceId();

    // All databases use table-row lock because connection-scoped locks
    // (pg_advisory_lock, GET_LOCK) are incompatible with Knex connection
    // pooling — acquire and release would use different connections.
    return await this.acquireTableRowLock(dbType, lockedBy, context, token);
  }

  async release(handle?: SchemaMigrationLockHandle | null): Promise<void> {
    if (!handle) {
      return;
    }
    const knex = this.knexService.getKnex();

    await this.clearLockRow(knex, handle.token);
  }

  private static readonly STALE_LOCK_THRESHOLD_MS = 120_000;

  private async acquireTableRowLock(
    dbType: string,
    lockedBy: string,
    context: string,
    token: string,
  ): Promise<SchemaMigrationLockHandle> {
    const knex = this.knexService.getKnex();
    await this.ensureLockTable();

    const handle = await knex.transaction(async (trx) => {
      const builder = trx(this.tableName).where({ id: 1 });
      if (typeof (builder as any).forUpdate === 'function') {
        builder.forUpdate();
      }
      const row = await builder.first();

      if (row?.isLocked) {
        const staleMs = Date.now() - new Date(row.lockedAt).getTime();
        if (staleMs > SchemaMigrationLockService.STALE_LOCK_THRESHOLD_MS) {
          this.logger.warn(
            `Clearing stale schema lock held by ${row.lockedBy} for ${context} (${Math.round(staleMs / 1000)}s old)`,
          );
          await trx(this.tableName).where({ id: 1 }).update({
            isLocked: false,
            lockedBy: null,
            lockedContext: null,
            lockedAt: null,
            lockToken: null,
          });
        } else {
          throw await this.buildLockedError(trx);
        }
      }

      const dbType = this.queryBuilderService.getDatabaseType() || 'mysql';
      const updateData: any = {
        isLocked: true,
        lockedBy,
        lockedContext: context,
        lockToken: token,
      };

      if (dbType === 'postgres') {
        updateData.lockedAt = new Date().toISOString();
      } else {
        updateData.lockedAt = trx.raw('NOW()');
      }

      await trx(this.tableName).where({ id: 1 }).update(updateData);
      return { token, dbType };
    });

    return handle;
  }

  private async ensureLockTable(): Promise<void> {
    if (this.lockTableReady) {
      return;
    }

    const baseKnex = this.knexService.getKnex();
    const exists = await baseKnex.schema.hasTable(this.tableName);
    if (!exists) {
      await baseKnex.schema.createTable(this.tableName, (table) => {
        table.integer('id').primary();
        table.boolean('isLocked').notNullable().defaultTo(false);
        table.string('lockedBy', 255).nullable();
        table.string('lockedContext', 255).nullable();
        table.dateTime('lockedAt').nullable();
        table.string('lockToken', 64).nullable();
        table.timestamp('createdAt').defaultTo(baseKnex.fn.now());
        table.timestamp('updatedAt').defaultTo(baseKnex.fn.now());
      });
      await baseKnex(this.tableName).insert({ id: 1, isLocked: false });
    } else {
      const columnInfo = await baseKnex(this.tableName).columnInfo();

      if (!columnInfo.createdAt) {
        await baseKnex.schema.alterTable(this.tableName, (table) => {
          table.timestamp('createdAt').defaultTo(baseKnex.fn.now());
        });
      }

      if (!columnInfo.updatedAt) {
        await baseKnex.schema.alterTable(this.tableName, (table) => {
          table.timestamp('updatedAt').defaultTo(baseKnex.fn.now());
        });
      }

      const row = await baseKnex(this.tableName).where({ id: 1 }).first();
      if (!row) {
        await baseKnex(this.tableName).insert({ id: 1, isLocked: false });
      }
    }

    this.lockTableReady = true;
  }

  private async setLockRow(
    knex: KnexLike,
    lockedBy: string,
    context: string,
    token: string,
  ): Promise<void> {
    await this.ensureLockTable();
    const dbType = this.queryBuilderService.getDatabaseType() || 'mysql';
    const updateData: any = {
      isLocked: true,
      lockedBy,
      lockedContext: context,
      lockToken: token,
    };

    if (dbType === 'postgres') {
      updateData.lockedAt = new Date().toISOString();
    } else {
      updateData.lockedAt = knex.raw('NOW()');
    }

    await knex(this.tableName).where({ id: 1 }).update(updateData);
  }

  private async clearLockRow(knex: KnexLike, token: string): Promise<void> {
    await this.ensureLockTable();
    const updatePayload = {
      isLocked: false,
      lockedBy: null,
      lockedContext: null,
      lockedAt: null,
      lockToken: null,
    };

    const updated = await knex(this.tableName)
      .where({ id: 1 })
      .andWhere((builder) => {
        if (token) {
          builder.where({ lockToken: token });
        }
      })
      .update(updatePayload);

    if (updated === 0 && token) {
      const row = await knex(this.tableName).where({ id: 1 }).first();
      if (row?.lockToken === token) {
        await knex(this.tableName).where({ id: 1 }).update(updatePayload);
      }
    }
  }

  private async buildLockedError(knex: KnexLike): Promise<DatabaseException> {
    const info = await this.readLockInfo(knex);
    return new DatabaseException(
      'Schema is being updated, please try again later.',
      {
        reason: 'schema_locked',
        lockedBy: info?.lockedBy || null,
        lockedAt: info?.lockedAt || null,
        lockedContext: info?.lockedContext || null,
      },
    );
  }

  private async readLockInfo(knex: KnexLike): Promise<any> {
    await this.ensureLockTable();
    return await knex(this.tableName).where({ id: 1 }).first();
  }

  private buildInstanceId(): string {
    const parts = [
      process.env.INSTANCE_ID,
      process.env.HOSTNAME,
      String(process.pid),
    ];
    return parts.filter(Boolean).join(':') || 'unknown-instance';
  }
}
