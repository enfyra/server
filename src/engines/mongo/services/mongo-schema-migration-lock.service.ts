import { Logger } from '../../../shared/logger';
import { randomUUID } from 'crypto';
import { Collection, Db } from 'mongodb';
import { MongoService } from './mongo.service';
import { DatabaseException } from '../../../domain/exceptions';
import { getMongoRawDb } from './mongo-raw-db.util';

interface SchemaMigrationLockDocument {
  _id: string;
  isLocked: boolean;
  lockedBy?: string | null;
  lockedContext?: string | null;
  lockToken?: string | null;
  lockedAt?: Date | null;
  lockExpiresAt?: Date | null;
  sagaSessionId?: string | null;
  purpose?: string | null;
  mutationId?: string | null;
  fenceEpoch?: number;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface MongoSchemaMigrationLockHandle {
  token: string;
}

export class MongoSchemaMigrationLockService {
  private readonly logger = new Logger(MongoSchemaMigrationLockService.name);
  private readonly collectionName = 'schema_migration_lock';
  private readonly documentId = 'global';
  private readonly lockDurationMs = 30_000;
  private collectionReady = false;
  private readonly mongoService: MongoService;

  constructor(deps: { mongoService: MongoService }) {
    this.mongoService = deps.mongoService;
  }

  async acquire(context: string): Promise<MongoSchemaMigrationLockHandle> {
    const collection = await this.getCollection();
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.lockDurationMs);
    const token = randomUUID();
    const lockedBy = this.buildInstanceId();
    const saga = this.mongoService.getActiveSagaSession();

    const updatedDoc = await collection.findOneAndUpdate(
      {
        _id: this.documentId,
        $or: [
          { isLocked: { $ne: true } },
          { lockExpiresAt: { $lte: now } },
        ],
      },
      {
        $set: {
          isLocked: true,
          lockedBy,
          lockedContext: context,
          lockToken: token,
          lockedAt: now,
          lockExpiresAt: expiresAt,
          sagaSessionId: saga?.txId ?? null,
          purpose: saga?.purpose ?? null,
          mutationId: saga?.mutationId ?? null,
        },
        $inc: { fenceEpoch: 1 },
        $currentDate: { updatedAt: true },
      },
      { returnDocument: 'after' },
    );

    if (updatedDoc) {
      return { token };
    }

    try {
      await collection.insertOne({
        _id: this.documentId,
        isLocked: true,
        lockedBy,
        lockedContext: context,
        lockToken: token,
        lockedAt: now,
        lockExpiresAt: expiresAt,
        sagaSessionId: saga?.txId ?? null,
        purpose: saga?.purpose ?? null,
        mutationId: saga?.mutationId ?? null,
        fenceEpoch: 1,
        createdAt: now,
        updatedAt: now,
      });
      return { token };
    } catch (error: any) {
      if (error?.code === 11000) {
        throw await this.buildLockedError(collection);
      }
      throw error;
    }
  }

  async acquireForRecovery(
    context: string,
    maxWaitMs = 25_000,
  ): Promise<MongoSchemaMigrationLockHandle> {
    const deadline = Date.now() + maxWaitMs;
    while (true) {
      try {
        return await this.acquire(context);
      } catch (error: any) {
        if (error?.details?.reason !== 'schema_locked' || Date.now() >= deadline) {
          throw error;
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  }

  async release(handle?: MongoSchemaMigrationLockHandle | null): Promise<void> {
    if (!handle) {
      return;
    }
    const collection = await this.getCollection();
    await collection.updateOne(
      { _id: this.documentId, lockToken: handle.token },
      {
        $set: {
          isLocked: false,
          lockedBy: null,
          lockedContext: null,
          lockToken: null,
          lockedAt: null,
          lockExpiresAt: null,
          sagaSessionId: null,
          purpose: null,
          mutationId: null,
        },
        $currentDate: { updatedAt: true },
      },
    );
  }

  async refreshHeartbeat(
    handle: MongoSchemaMigrationLockHandle,
  ): Promise<boolean> {
    if (!handle) return false;
    const collection = await this.getCollection();
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.lockDurationMs);
    const result = await collection.updateOne(
      { _id: this.documentId, lockToken: handle.token, isLocked: true },
      {
        $set: { lockExpiresAt: expiresAt },
        $currentDate: { updatedAt: true },
      },
    );
    return result.modifiedCount > 0;
  }

  async isStillHeld(
    handle: MongoSchemaMigrationLockHandle,
  ): Promise<boolean> {
    if (!handle) return false;
    const collection = await this.getCollection();
    const doc = await collection.findOne({
      _id: this.documentId,
      lockToken: handle.token,
      isLocked: true,
    });
    if (!doc) return false;
    if (doc.lockExpiresAt && new Date(doc.lockExpiresAt).getTime() <= Date.now()) {
      return false;
    }
    return true;
  }

  private async getCollection(): Promise<
    Collection<SchemaMigrationLockDocument>
  > {
    const db = getMongoRawDb(this.mongoService);
    return await this.ensureCollection(db);
  }

  private async ensureCollection(
    db: Db,
  ): Promise<Collection<SchemaMigrationLockDocument>> {
    if (this.collectionReady) {
      return db.collection<SchemaMigrationLockDocument>(this.collectionName);
    }

    const collections = await db
      .listCollections({ name: this.collectionName })
      .toArray();
    if (collections.length === 0) {
      try {
        await db.createCollection(this.collectionName);
      } catch (error: any) {
        if (error.code !== 48) {
          throw error;
        }
      }
    }

    this.collectionReady = true;
    return db.collection<SchemaMigrationLockDocument>(this.collectionName);
  }

  private async buildLockedError(
    collection: Collection<SchemaMigrationLockDocument>,
  ): Promise<DatabaseException> {
    const doc = await collection.findOne({ _id: this.documentId });
    return new DatabaseException(
      'Schema is being updated, please try again later.',
      {
        reason: 'schema_locked',
        lockedBy: doc?.lockedBy || null,
        lockedAt: doc?.lockedAt || null,
        lockedContext: doc?.lockedContext || null,
        sagaSessionId: doc?.sagaSessionId || null,
        mutationId: doc?.mutationId || null,
      },
    );
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
