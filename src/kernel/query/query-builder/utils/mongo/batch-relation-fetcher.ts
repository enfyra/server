import { Db } from 'mongodb';
import {
  BatchFetchEngine,
  BatchFetchDescriptor,
  MetadataGetter,
  BatchTrace,
} from '../../../query-dsl/batch-fetch-engine';
import { MongoBatchAdapter } from './mongo-batch-adapter';
import type { MongoPhysicalMigrationService } from '../../../../../engines/mongo';

export type MongoBatchFetchDescriptor = BatchFetchDescriptor;

export async function executeMongoBatchFetches(
  db: Db,
  parentDocs: any[],
  descriptors: BatchFetchDescriptor[],
  metadataGetter: MetadataGetter,
  maxDepth: number = 3,
  currentDepth: number = 0,
  parentTableName?: string,
  metadata?: any,
  trace?: BatchTrace,
  mongoPhysicalMigrationService?: MongoPhysicalMigrationService,
): Promise<void> {
  const fieldRenamesByTable =
    await mongoPhysicalMigrationService?.getActiveFieldRenamesByTable?.();
  const adapter = new MongoBatchAdapter(db, metadata, fieldRenamesByTable);
  const engine = new BatchFetchEngine(adapter, metadataGetter, trace);
  await engine.execute(
    parentDocs,
    descriptors,
    maxDepth,
    currentDepth,
    parentTableName,
  );
}
