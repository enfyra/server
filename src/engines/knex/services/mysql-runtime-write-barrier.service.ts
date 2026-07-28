import { AsyncLocalStorage } from 'node:async_hooks';
import { randomUUID } from 'node:crypto';
import type { Knex } from 'knex';
import { DatabaseException } from '../../../domain/exceptions';
import type { InstanceService } from '../../../shared/services';
import type { KnexService } from '../knex.service';
import type { MySqlRuntimeWriteFenceContext } from '../types/mysql-runtime-write-barrier.types';

const FENCE_TABLE = 'system_runtime_write_fence';
const WRITER_TABLE = 'system_runtime_active_writes';
const FENCE_LEASE_MS = 30_000;
const FENCE_RENEWAL_MS = 5_000;
const RECOVERY_WAIT_MS = FENCE_LEASE_MS + 10_000;

export class MySqlRuntimeWriteBarrierService {
  private readonly schemaOwnerContext = new AsyncLocalStorage<boolean>();
  private readonly writerContext = new AsyncLocalStorage<boolean>();
  private readyPromise: Promise<void> | null = null;
  private readonly instanceId: string;

  constructor(
    private readonly deps: {
      knexService: KnexService;
      instanceService: InstanceService;
    },
  ) {
    this.instanceId = deps.instanceService.getInstanceId();
  }

  async runWithWriteLease<T>(
    callback: () => Promise<T>,
    context = 'sql-write',
  ): Promise<T> {
    if (this.schemaOwnerContext.getStore() || this.writerContext.getStore()) {
      return callback();
    }
    await this.ensureReady();
    const knex = this.deps.knexService.getSystemKnex();
    const token = randomUUID();
    const leaseMs = 10 * 60 * 1000;
    const now = new Date();
    await knex.transaction(async (trx) => {
      await trx(WRITER_TABLE).where('expiresAt', '<=', now).delete();
      const fence = await trx(FENCE_TABLE)
        .where({ id: 'global' })
        .forUpdate()
        .first();
      if (fence?.isFenced) {
        throw new DatabaseException(
          'Database writes are temporarily fenced for a schema migration',
          { reason: 'schema_locked', mutationId: fence.mutationId ?? null },
        );
      }
      await trx(WRITER_TABLE).insert({
        token,
        context: context.slice(0, 255),
        expiresAt: new Date(now.getTime() + leaseMs),
        createdAt: now,
        updatedAt: now,
      });
    });

    let leaseLost = false;
    const renewal = setInterval(() => {
      void knex(WRITER_TABLE)
        .where({ token })
        .update({
          expiresAt: new Date(Date.now() + leaseMs),
          updatedAt: new Date(),
        })
        .then((updated) => {
          if (Number(updated) !== 1) leaseLost = true;
        })
        .catch(() => {
          leaseLost = true;
        });
    }, 30_000);

    try {
      const result = await this.writerContext.run(true, callback);
      if (leaseLost) {
        throw new DatabaseException('Database write lease was lost', {
          reason: 'write_lease_lost',
        });
      }
      return result;
    } finally {
      clearInterval(renewal);
      await knex(WRITER_TABLE).where({ token }).delete();
    }
  }

  async runExclusive<T>(
    context: MySqlRuntimeWriteFenceContext,
    callback: () => Promise<T>,
  ): Promise<T> {
    await this.ensureReady();
    const knex = this.deps.knexService.getSystemKnex();
    const token = randomUUID();
    const acquired = await knex.transaction(async (trx) => {
      const fence = await trx(FENCE_TABLE)
        .where({ id: 'global' })
        .forUpdate()
        .first();
      if (fence?.isFenced) {
        const leaseExpiresAt = this.parseTime(fence.leaseExpiresAt);
        throw new DatabaseException(
          leaseExpiresAt > Date.now()
            ? 'Another MySQL runtime schema recovery or migration owns the write fence'
            : 'An expired MySQL runtime schema fence requires recovery before a new migration',
          {
            reason:
              leaseExpiresAt > Date.now()
                ? 'schema_locked'
                : 'schema_recovery_required',
            mutationId: fence.mutationId ?? null,
            ownerInstanceId: fence.ownerInstanceId ?? null,
          },
        );
      }
      const now = new Date();
      const updated = await trx(FENCE_TABLE).where({ id: 'global' }).update({
        isFenced: true,
        fenceToken: token,
        ownerInstanceId: this.instanceId,
        mutationId: context.mutationId,
        fenceEpoch: Number(fence?.fenceEpoch ?? 0) + 1,
        fencedAt: now,
        leaseExpiresAt: new Date(now.getTime() + FENCE_LEASE_MS),
        updatedAt: now,
      });
      return Number(updated) === 1;
    });
    if (!acquired) {
      throw new DatabaseException('Unable to acquire MySQL schema write fence', {
        reason: 'schema_lock_failed',
      });
    }

    let mayRelease = false;
    const lease = this.startFenceHeartbeat(knex, token);
    try {
      await this.waitForWritersToDrain(knex);
      const result = await this.schemaOwnerContext.run(true, callback);
      await this.assertFenceOwnership(knex, token, lease);
      mayRelease = true;
      return result;
    } catch (error) {
      if (
        (error as any)?.mysqlSnapshotRestored === true &&
        !lease.isLost()
      ) {
        mayRelease = true;
      }
      throw error;
    } finally {
      lease.stop();
      if (mayRelease) {
        await this.releaseFence(knex, token);
      }
    }
  }

  async recoverExclusive<T>(callback: () => Promise<T>): Promise<T> {
    await this.ensureReady();
    const knex = this.deps.knexService.getSystemKnex();
    const token = randomUUID();
    const deadline = Date.now() + RECOVERY_WAIT_MS;
    let claimed = false;
    while (!claimed) {
      const result = await this.tryClaimRecoveryFence(knex, token);
      if (result.claimed) {
        claimed = true;
        break;
      }
      if (Date.now() >= deadline) {
        throw new DatabaseException(
          'Timed out waiting for the live MySQL schema fence owner',
          { reason: 'schema_locked' },
        );
      }
      const waitMs = result.leaseExpiresAt
        ? Math.max(50, result.leaseExpiresAt - Date.now() + 50)
        : 500;
      await new Promise((resolve) =>
        setTimeout(resolve, Math.min(waitMs, deadline - Date.now())),
      );
    }

    const lease = this.startFenceHeartbeat(knex, token);
    try {
      await this.waitForWritersToDrain(knex);
      const result = await this.schemaOwnerContext.run(true, callback);
      await this.assertFenceOwnership(knex, token, lease);
      await this.releaseFence(knex, token);
      return result;
    } finally {
      lease.stop();
    }
  }

  private async tryClaimRecoveryFence(
    knex: Knex,
    token: string,
  ): Promise<{ claimed: boolean; leaseExpiresAt?: number }> {
    return knex.transaction(async (trx) => {
      const fence = await trx(FENCE_TABLE)
        .where({ id: 'global' })
        .forUpdate()
        .first();
      const now = new Date();
      if (
        fence?.isFenced &&
        this.parseTime(fence.leaseExpiresAt) > now.getTime()
      ) {
        return {
          claimed: false,
          leaseExpiresAt: this.parseTime(fence.leaseExpiresAt),
        };
      }
      const updated = await trx(FENCE_TABLE).where({ id: 'global' }).update({
        isFenced: true,
        fenceToken: token,
        ownerInstanceId: this.instanceId,
        mutationId: 'runtime-schema:recovery',
        fenceEpoch: Number(fence?.fenceEpoch ?? 0) + 1,
        fencedAt: now,
        leaseExpiresAt: new Date(now.getTime() + FENCE_LEASE_MS),
        updatedAt: now,
      });
      return { claimed: Number(updated) === 1 };
    });
  }

  private startFenceHeartbeat(knex: Knex, token: string): {
    isLost: () => boolean;
    stop: () => void;
  } {
    let lost = false;
    const renewal = setInterval(() => {
      const now = new Date();
      void knex(FENCE_TABLE)
        .where({
          id: 'global',
          isFenced: true,
          fenceToken: token,
          ownerInstanceId: this.instanceId,
        })
        .update({
          leaseExpiresAt: new Date(now.getTime() + FENCE_LEASE_MS),
          updatedAt: now,
        })
        .then((updated) => {
          if (Number(updated) !== 1) lost = true;
        })
        .catch(() => {
          lost = true;
        });
    }, FENCE_RENEWAL_MS);
    return {
      isLost: () => lost,
      stop: () => clearInterval(renewal),
    };
  }

  private async assertFenceOwnership(
    knex: Knex,
    token: string,
    lease: { isLost: () => boolean },
  ): Promise<void> {
    const fence = await knex(FENCE_TABLE)
      .where({
        id: 'global',
        isFenced: true,
        fenceToken: token,
        ownerInstanceId: this.instanceId,
      })
      .first();
    if (
      lease.isLost() ||
      !fence ||
      this.parseTime(fence.leaseExpiresAt) <= Date.now()
    ) {
      throw new DatabaseException('MySQL schema write fence lease was lost', {
        reason: 'schema_lease_lost',
      });
    }
  }

  private async releaseFence(knex: Knex, token: string): Promise<void> {
    const updated = await knex(FENCE_TABLE)
      .where({
        id: 'global',
        isFenced: true,
        fenceToken: token,
        ownerInstanceId: this.instanceId,
      })
      .update({
        isFenced: false,
        fenceToken: null,
        ownerInstanceId: null,
        mutationId: null,
        fencedAt: null,
        leaseExpiresAt: null,
        updatedAt: new Date(),
      });
    if (Number(updated) !== 1) {
      throw new DatabaseException('MySQL schema write fence lease was lost', {
        reason: 'schema_lease_lost',
      });
    }
  }

  private parseTime(value: unknown): number {
    if (!value) return 0;
    const parsed = new Date(value as string | number | Date).getTime();
    return Number.isFinite(parsed) ? parsed : 0;
  }

  private async waitForWritersToDrain(knex: Knex): Promise<void> {
    const deadline = Date.now() + 120_000;
    while (true) {
      await knex(WRITER_TABLE).where('expiresAt', '<=', new Date()).delete();
      const row = await knex(WRITER_TABLE)
        .count<{ count: number }>({ count: '*' })
        .first();
      if (Number(row?.count ?? 0) === 0) return;
      if (Date.now() >= deadline) {
        throw new Error('Timed out draining MySQL writers for schema migration');
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  }

  private async ensureReady(): Promise<void> {
    if (!this.readyPromise) this.readyPromise = this.createControlTables();
    await this.readyPromise;
  }

  private async createControlTables(): Promise<void> {
    const knex = this.deps.knexService.getSystemKnex();
    if (!(await knex.schema.hasTable(FENCE_TABLE))) {
      try {
        await knex.schema.createTable(FENCE_TABLE, (table) => {
          table.string('id', 32).primary();
          table.boolean('isFenced').notNullable().defaultTo(false);
          table.string('fenceToken', 64).nullable();
          table.string('ownerInstanceId', 128).nullable();
          table.string('mutationId', 128).nullable();
          table.bigInteger('fenceEpoch').notNullable().defaultTo(0);
          table.timestamp('fencedAt', { precision: 3 }).nullable();
          table.timestamp('leaseExpiresAt', { precision: 3 }).nullable();
          table.timestamp('updatedAt', { precision: 3 }).notNullable();
        });
      } catch (error: any) {
        if (error?.code !== 'ER_TABLE_EXISTS_ERROR') throw error;
      }
    }
    await this.ensureFenceColumns(knex);
    if (!(await knex.schema.hasTable(WRITER_TABLE))) {
      try {
        await knex.schema.createTable(WRITER_TABLE, (table) => {
          table.string('token', 64).primary();
          table.string('context', 255).notNullable();
          table.timestamp('expiresAt', { precision: 3 }).notNullable().index();
          table.timestamp('createdAt', { precision: 3 }).notNullable();
          table.timestamp('updatedAt', { precision: 3 }).notNullable();
        });
      } catch (error: any) {
        if (error?.code !== 'ER_TABLE_EXISTS_ERROR') throw error;
      }
    }
    await knex(FENCE_TABLE)
      .insert({
        id: 'global',
        isFenced: false,
        fenceToken: null,
        ownerInstanceId: null,
        mutationId: null,
        fenceEpoch: 0,
        fencedAt: null,
        leaseExpiresAt: null,
        updatedAt: new Date(),
      })
      .onConflict('id')
      .ignore();
  }

  private async ensureFenceColumns(knex: Knex): Promise<void> {
    const columns: Array<{
      name: string;
      add: (table: Knex.AlterTableBuilder) => void;
    }> = [
      {
        name: 'ownerInstanceId',
        add: (table) => table.string('ownerInstanceId', 128).nullable(),
      },
      {
        name: 'fenceEpoch',
        add: (table) => table.bigInteger('fenceEpoch').notNullable().defaultTo(0),
      },
      {
        name: 'leaseExpiresAt',
        add: (table) =>
          table.timestamp('leaseExpiresAt', { precision: 3 }).nullable(),
      },
    ];
    for (const column of columns) {
      if (await knex.schema.hasColumn(FENCE_TABLE, column.name)) continue;
      try {
        await knex.schema.alterTable(FENCE_TABLE, column.add);
      } catch (error: any) {
        if (error?.code !== 'ER_DUP_FIELDNAME') throw error;
      }
    }
  }
}
