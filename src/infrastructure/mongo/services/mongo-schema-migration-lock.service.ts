import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { Collection, Db } from 'mongodb';
import { MongoService } from './mongo.service';
import { DatabaseException } from '../../../core/exceptions/custom-exceptions';

interface SchemaMigrationLockDocument {
  _id: string;
  isLocked: boolean;
  lockedBy?: string | null;
  lockedContext?: string | null;
  lockToken?: string | null;
  lockedAt?: Date | null;
  lockExpiresAt?: Date | null;
  createdAt?: Date;
  updatedAt?: Date;
}

export interface MongoSchemaMigrationLockHandle {
  token: string;
}

@Injectable()
export class MongoSchemaMigrationLockService {
  private readonly logger = new Logger(MongoSchemaMigrationLockService.name);
  private readonly collectionName = 'schema_migration_lock';
  private readonly documentId = 'global';
  private readonly lockDurationMs = 5 * 60 * 1000;
  private collectionReady = false;

  constructor(private readonly mongoService: MongoService) {}

  async acquire(context: string): Promise<MongoSchemaMigrationLockHandle> {
    const collection = await this.getCollection();
    const now = new Date();
    const expiresAt = new Date(now.getTime() + this.lockDurationMs);
    const token = randomUUID();
    const lockedBy = this.buildInstanceId();

    const updatedDoc = await collection.findOneAndUpdate(
      {
        _id: this.documentId,
        $or: [
          { isLocked: { $ne: true } },
          { lockExpiresAt: { $lte: now } },
          { lockExpiresAt: null },
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
        },
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
        },
        $currentDate: { updatedAt: true },
      },
    );
  }

  private async getCollection(): Promise<Collection<SchemaMigrationLockDocument>> {
    const db = this.mongoService.getDb();
    return await this.ensureCollection(db);
  }

  private async ensureCollection(db: Db): Promise<Collection<SchemaMigrationLockDocument>> {
    if (this.collectionReady) {
      return db.collection<SchemaMigrationLockDocument>(this.collectionName);
    }

    const collections = await db.listCollections({ name: this.collectionName }).toArray();
    if (collections.length === 0) {
      await db.createCollection(this.collectionName);
    }

    this.collectionReady = true;
    return db.collection<SchemaMigrationLockDocument>(this.collectionName);
  }

  private async buildLockedError(collection: Collection<SchemaMigrationLockDocument>): Promise<DatabaseException> {
    const doc = await collection.findOne({ _id: this.documentId });
    return new DatabaseException('Schema đang được cập nhật, vui lòng thử lại sau.', {
      reason: 'schema_locked',
      lockedBy: doc?.lockedBy || null,
      lockedAt: doc?.lockedAt || null,
      lockedContext: doc?.lockedContext || null,
    });
  }

  private buildInstanceId(): string {
    const parts = [process.env.INSTANCE_ID, process.env.HOSTNAME, String(process.pid)];
    return parts.filter(Boolean).join(':') || 'unknown-instance';
  }
}

