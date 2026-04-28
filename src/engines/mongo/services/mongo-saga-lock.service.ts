import { Logger } from '../../../shared/logger';
import { randomUUID } from 'crypto';
import { Collection } from 'mongodb';
import { MongoService } from './mongo.service';
import { DatabaseException } from '../../../domain/exceptions';
import { getErrorMessage } from '../../../shared/utils/error.util';

export interface IResourceLock {
  _id: string;
  txId: string;
  resourceKey: string;
  resourceType: string;
  resourceId: string;
  lockMode: 'read' | 'write';
  lockedAt: Date;
  expiresAt: Date;
  lockedBy: string;
  lockedByInstance: string;
  retryCount: number;
}

export interface ITransactionMetadata {
  _id: string;
  txId: string;
  status: 'active' | 'committing' | 'rolling_back' | 'completed' | 'aborted';
  startedAt: Date;
  lastActivityAt: Date;
  expiresAt: Date;
  resources: string[];
  instanceId: string;
  pid: number;
}

export interface ILockAcquisitionResult {
  success: boolean;
  txId: string;
  acquiredLocks: string[];
  failedLocks?: string[];
  error?: string;
}

export interface ILockHandle {
  txId: string;
  resourceKey: string;
}

export interface IOrphanMarkerRecoveryPlan {
  shouldUnsetMarkers: boolean;
  needsRollbackFirst: boolean;
}

export class MongoSagaLockService {
  private readonly logger = new Logger(MongoSagaLockService.name);
  private readonly locksCollectionName = 'system_transaction_locks';
  private readonly txMetaCollectionName = 'system_transaction_metadata';
  private readonly lockTimeoutMs = 30000;
  private readonly txTimeoutMs = 60000;
  private readonly maxRetries = 3;
  private readonly retryDelayMs = 100;
  private collectionReady = false;
  private readonly instanceId: string;
  private readonly mongoService: MongoService;

  constructor(deps: { mongoService: MongoService }) {
    this.mongoService = deps.mongoService;
    this.instanceId = this.buildInstanceId();
  }

  private buildInstanceId(): string {
    const parts = [
      process.env.INSTANCE_ID,
      process.env.HOSTNAME,
      String(process.pid),
    ];
    return parts.filter(Boolean).join(':') || 'unknown-instance';
  }

  private async ensureCollections(): Promise<void> {
    if (this.collectionReady) {
      return;
    }

    const db = this.mongoService.getDb();

    const collections = await db.listCollections().toArray();
    const collectionNames = new Set(collections.map((c) => c.name));

    if (!collectionNames.has(this.locksCollectionName)) {
      try {
        await db.createCollection(this.locksCollectionName);
      } catch (error: any) {
        if (error.code !== 48) {
          throw error;
        }
      }
    }

    if (!collectionNames.has(this.txMetaCollectionName)) {
      try {
        await db.createCollection(this.txMetaCollectionName);
      } catch (error: any) {
        if (error.code !== 48) {
          throw error;
        }
      }
    }

    const locksCollection = db.collection(this.locksCollectionName);
    const existingLockIndexes = await locksCollection.indexes();
    const lockIndexNames = new Set(existingLockIndexes.map((i: any) => i.name));

    if (!lockIndexNames.has('txId_1')) {
      try {
        await locksCollection.createIndex({ txId: 1 });
      } catch {}
    }
    if (!lockIndexNames.has('resourceKey_1')) {
      try {
        await locksCollection.createIndex({ resourceKey: 1 });
      } catch {}
    }
    if (!lockIndexNames.has('resourceKey_1_lockMode_1')) {
      try {
        await locksCollection.createIndex({ resourceKey: 1, lockMode: 1 });
      } catch {}
    }
    if (!lockIndexNames.has('expiresAt_1')) {
      try {
        await locksCollection.createIndex(
          { expiresAt: 1 },
          { expireAfterSeconds: 0 },
        );
      } catch {}
    }

    const metaCollection = db.collection(this.txMetaCollectionName);
    const existingMetaIndexes = await metaCollection.indexes();
    const metaIndexNames = new Set(existingMetaIndexes.map((i: any) => i.name));

    if (!metaIndexNames.has('txId_1')) {
      try {
        await metaCollection.createIndex({ txId: 1 }, { unique: true });
      } catch {}
    }
    if (!metaIndexNames.has('expiresAt_1')) {
      try {
        await metaCollection.createIndex(
          { expiresAt: 1 },
          { expireAfterSeconds: 0 },
        );
      } catch {}
    }
    if (!metaIndexNames.has('status_1')) {
      try {
        await metaCollection.createIndex({ status: 1 });
      } catch {}
    }

    this.collectionReady = true;
  }

  private getLocksCollection(): Collection<IResourceLock> {
    return this.mongoService
      .getDb()
      .collection<IResourceLock>(this.locksCollectionName);
  }

  private getMetaCollection(): Collection<ITransactionMetadata> {
    return this.mongoService
      .getDb()
      .collection<ITransactionMetadata>(this.txMetaCollectionName);
  }

  async beginTransaction(): Promise<string> {
    await this.ensureCollections();

    const txId = `tx-${randomUUID()}`;
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.txTimeoutMs);

    await this.getMetaCollection().insertOne({
      _id: txId,
      txId,
      status: 'active',
      startedAt: now,
      lastActivityAt: now,
      expiresAt,
      resources: [],
      instanceId: this.instanceId,
      pid: process.pid,
    });

    this.logger.debug(`[${txId}] Transaction started`);
    return txId;
  }

  async renewTransactionLease(txId: string): Promise<void> {
    await this.ensureCollections();
    const now = new Date();
    const lockExp = new Date(now.getTime() + this.lockTimeoutMs);
    const txExp = new Date(now.getTime() + this.txTimeoutMs);
    const meta = await this.getMetaCollection().findOne({ txId });
    if (!meta || meta.status !== 'active') {
      return;
    }
    await this.getMetaCollection().updateOne(
      { txId, status: 'active' },
      {
        $set: {
          lastActivityAt: now,
          expiresAt: txExp,
        },
      },
    );
    await this.getLocksCollection().updateMany(
      { txId },
      { $set: { expiresAt: lockExp } },
    );
  }

  async acquireLocks(
    txId: string,
    resources: Array<{ type: string; id: string; mode: 'read' | 'write' }>,
    options?: { waitTimeout?: number; maxRetries?: number },
  ): Promise<ILockAcquisitionResult> {
    await this.ensureCollections();

    const maxRetries = options?.maxRetries || this.maxRetries;
    const acquiredLocks: string[] = [];
    const failedLocks: string[] = [];

    try {
      await this.updateTransactionActivity(txId);

      const sortedResources = [...resources].sort((a, b) => {
        const keyA = `${a.type}:${a.id}`;
        const keyB = `${b.type}:${b.id}`;
        return keyA.localeCompare(keyB);
      });

      for (const resource of sortedResources) {
        const resourceKey = `${resource.type}:${resource.id}`;
        let acquired = false;
        let attempts = 0;

        while (!acquired && attempts < maxRetries) {
          try {
            acquired = await this.tryAcquireSingleLock(
              txId,
              resource,
              resourceKey,
            );
            if (acquired) {
              acquiredLocks.push(resourceKey);
            }
          } catch (error) {
            if (attempts < maxRetries - 1) {
              await this.delay(this.retryDelayMs * Math.pow(2, attempts));
            }
          }
          attempts++;
        }

        if (!acquired) {
          failedLocks.push(resourceKey);
          await this.releaseLocks(txId, acquiredLocks);
          return {
            success: false,
            txId,
            acquiredLocks: [],
            failedLocks: [
              ...failedLocks,
              ...sortedResources
                .map((r) => `${r.type}:${r.id}`)
                .filter(
                  (k) => !acquiredLocks.includes(k) && !failedLocks.includes(k),
                ),
            ],
            error: `Failed to acquire lock for ${resourceKey} after ${maxRetries} attempts`,
          };
        }
      }

      await this.getMetaCollection().updateOne(
        { txId },
        {
          $addToSet: { resources: { $each: acquiredLocks } },
          $set: {
            lastActivityAt: new Date(),
            expiresAt: new Date(Date.now() + this.txTimeoutMs),
          },
        },
      );

      return {
        success: true,
        txId,
        acquiredLocks,
      };
    } catch (error) {
      await this.releaseLocks(txId, acquiredLocks);
      throw new DatabaseException(
        `Lock acquisition failed: ${getErrorMessage(error)}`,
        {
          txId,
          resources: resources.map((r) => `${r.type}:${r.id}`),
        },
      );
    }
  }

  private async tryAcquireSingleLock(
    txId: string,
    resource: { type: string; id: string; mode: 'read' | 'write' },
    resourceKey: string,
  ): Promise<boolean> {
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.lockTimeoutMs);
    const lockId = `${txId}:${resourceKey}`;

    await this.cleanupExpiredLocks();

    const existingOwnLock = await this.getLocksCollection().findOne({
      _id: lockId,
    });

    if (existingOwnLock) {
      if (existingOwnLock.lockMode === resource.mode) {
        await this.getLocksCollection().updateOne(
          { _id: lockId },
          { $set: { expiresAt } },
        );
        return true;
      }
      if (resource.mode === 'write' && existingOwnLock.lockMode === 'read') {
        const otherLocks = await this.getLocksCollection()
          .find({
            resourceKey,
            txId: { $ne: txId },
            expiresAt: { $gt: now },
          })
          .toArray();

        if (otherLocks.length > 0) {
          return false;
        }

        await this.getLocksCollection().updateOne(
          { _id: lockId },
          { $set: { lockMode: 'write', expiresAt } },
        );
        return true;
      }
    }

    const conflictingLocks = await this.getLocksCollection()
      .find({
        resourceKey,
        txId: { $ne: txId },
        expiresAt: { $gt: now },
      })
      .toArray();

    for (const lock of conflictingLocks) {
      if (lock.lockMode === 'write' || resource.mode === 'write') {
        return false;
      }
    }

    try {
      await this.getLocksCollection().insertOne({
        _id: lockId,
        txId,
        resourceKey,
        resourceType: resource.type,
        resourceId: resource.id,
        lockMode: resource.mode,
        lockedAt: now,
        expiresAt,
        lockedBy: this.instanceId,
        lockedByInstance: this.instanceId,
        retryCount: 0,
      } as any);
      return true;
    } catch (error: any) {
      if (error.code === 11000) {
        return true;
      }
      throw error;
    }
  }

  async acquireReadLocks(
    txId: string,
    resources: Array<{ type: string; id: string }>,
  ): Promise<ILockAcquisitionResult> {
    await this.ensureCollections();

    const resourceKeys = resources.map((r) => `${r.type}:${r.id}`);
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.lockTimeoutMs);

    const sortedKeys = [...resourceKeys].sort();

    const existingWriteLocks = await this.getLocksCollection()
      .find({
        resourceKey: { $in: sortedKeys },
        txId: { $ne: txId },
        lockMode: 'write',
        expiresAt: { $gt: now },
      })
      .toArray();

    if (existingWriteLocks.length > 0) {
      return {
        success: false,
        txId,
        acquiredLocks: [],
        failedLocks: existingWriteLocks.map((l) => l.resourceKey),
        error: 'Write locks exist on requested resources',
      };
    }

    const lockDocs = sortedKeys.map((key) => ({
      _id: `${txId}:${key}`,
      txId,
      resourceKey: key,
      resourceType: key.split(':')[0],
      resourceId: key.split(':')[1],
      lockMode: 'read' as const,
      lockedAt: now,
      expiresAt,
      lockedBy: this.instanceId,
      lockedByInstance: this.instanceId,
      retryCount: 0,
    }));

    try {
      await this.getLocksCollection().insertMany(lockDocs as any[], {
        ordered: false,
      });
    } catch (error: any) {
      if (error.code === 11000) {
        return {
          success: true,
          txId,
          acquiredLocks: sortedKeys,
        };
      }
      throw error;
    }

    await this.getMetaCollection().updateOne(
      { txId },
      {
        $addToSet: { resources: { $each: sortedKeys } },
        $set: {
          lastActivityAt: new Date(),
          expiresAt: new Date(Date.now() + this.txTimeoutMs),
        },
      },
    );

    return {
      success: true,
      txId,
      acquiredLocks: sortedKeys,
    };
  }

  async releaseLocks(
    txId: string,
    specificResourceKeys?: string[],
  ): Promise<void> {
    try {
      const query: any = { txId };
      if (specificResourceKeys && specificResourceKeys.length > 0) {
        query._id = { $in: specificResourceKeys.map((k) => `${txId}:${k}`) };
      }

      const result = await this.getLocksCollection().deleteMany(query);

      if (!specificResourceKeys) {
        await this.getMetaCollection().updateOne(
          { txId },
          { $set: { resources: [], lastActivityAt: new Date() } },
        );
      }

      this.logger.debug(`[${txId}] Released ${result.deletedCount} locks`);
    } catch (error) {
      this.logger.error(
        `[${txId}] Failed to release locks: ${getErrorMessage(error)}`,
      );
    }
  }

  async commitTransaction(txId: string): Promise<void> {
    await this.ensureCollections();

    try {
      await this.getMetaCollection().updateOne(
        { txId },
        {
          $set: {
            status: 'completed',
            lastActivityAt: new Date(),
          },
        },
      );

      await this.releaseLocks(txId);

      this.logger.debug(`[${txId}] Transaction committed`);
    } catch (error) {
      throw new DatabaseException(
        `Failed to commit transaction: ${getErrorMessage(error)}`,
        { txId },
      );
    }
  }

  async abortTransaction(txId: string, reason?: string): Promise<void> {
    await this.ensureCollections();

    try {
      await this.getMetaCollection().updateOne(
        { txId },
        {
          $set: {
            status: 'aborted',
            lastActivityAt: new Date(),
          },
        },
      );

      await this.releaseLocks(txId);

      this.logger.debug(
        `[${txId}] Transaction aborted${reason ? `: ${reason}` : ''}`,
      );
    } catch (error) {
      this.logger.error(
        `[${txId}] Failed to abort transaction: ${getErrorMessage(error)}`,
      );
    }
  }

  async detectDeadlocks(): Promise<string[][]> {
    const activeTxs = await this.getMetaCollection()
      .find({ status: 'active' })
      .toArray();

    const deadlocks: string[][] = [];

    for (const tx of activeTxs) {
      if (tx.lastActivityAt < new Date(Date.now() - 30000)) {
        const txLocks = await this.getLocksCollection()
          .find({ txId: tx.txId })
          .toArray();

        for (const lock of txLocks) {
          const blockingLocks = await this.getLocksCollection()
            .find({
              resourceKey: lock.resourceKey,
              txId: { $ne: tx.txId },
              expiresAt: { $gt: new Date() },
            })
            .toArray();

          for (const blockingLock of blockingLocks) {
            if (
              blockingLock.lockMode === 'write' ||
              lock.lockMode === 'write'
            ) {
              const blockingTx = await this.getMetaCollection().findOne({
                txId: blockingLock.txId,
              });

              if (
                blockingTx &&
                blockingTx.lastActivityAt < new Date(Date.now() - 30000)
              ) {
                deadlocks.push([tx.txId, blockingTx.txId]);
              }
            }
          }
        }
      }
    }

    return deadlocks;
  }

  async cleanupOrphanedLocks(): Promise<number> {
    const activeTxIds = await this.getMetaCollection()
      .find({ status: 'active' })
      .map((tx) => tx.txId)
      .toArray();

    const orphanedLocks = await this.getLocksCollection()
      .find({
        txId: { $nin: activeTxIds },
      })
      .toArray();

    if (orphanedLocks.length > 0) {
      const result = await this.getLocksCollection().deleteMany({
        _id: { $in: orphanedLocks.map((l) => l._id) },
      });
      return result.deletedCount || 0;
    }

    return 0;
  }

  async getSagaStatus(txId: string): Promise<ITransactionMetadata | null> {
    return this.getMetaCollection().findOne({ txId });
  }

  async getOrphanMarkerRecoveryPlan(
    txId: string,
  ): Promise<IOrphanMarkerRecoveryPlan> {
    await this.ensureCollections();
    const meta = await this.getSagaStatus(txId);
    if (!meta) {
      return { shouldUnsetMarkers: true, needsRollbackFirst: false };
    }
    if (meta.status !== 'active') {
      return { shouldUnsetMarkers: true, needsRollbackFirst: false };
    }
    const nowMs = Date.now();
    if (meta.expiresAt.getTime() <= nowMs) {
      return { shouldUnsetMarkers: true, needsRollbackFirst: true };
    }
    const staleThresholdMs = this.txTimeoutMs * 2;
    if (meta.lastActivityAt.getTime() < nowMs - staleThresholdMs) {
      return { shouldUnsetMarkers: true, needsRollbackFirst: true };
    }
    return { shouldUnsetMarkers: false, needsRollbackFirst: false };
  }

  async isResourceLocked(
    resourceType: string,
    resourceId: string,
  ): Promise<boolean> {
    const resourceKey = `${resourceType}:${resourceId}`;
    const lock = await this.getLocksCollection().findOne({
      resourceKey,
      expiresAt: { $gt: new Date() },
    });
    return lock !== null;
  }

  private async cleanupExpiredLocks(): Promise<void> {
    const now = new Date();
    await this.getLocksCollection().deleteMany({
      expiresAt: { $lt: now },
    });
  }

  private async updateTransactionActivity(txId: string): Promise<void> {
    const now = new Date();
    await this.getMetaCollection().updateOne(
      { txId },
      {
        $set: {
          lastActivityAt: now,
          expiresAt: new Date(now.getTime() + this.txTimeoutMs),
        },
      },
    );
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
