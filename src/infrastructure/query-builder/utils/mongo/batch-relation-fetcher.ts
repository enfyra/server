import { Db } from 'mongodb';
import {
  BatchFetchEngine,
  BatchFetchDescriptor,
  MetadataGetter,
} from '../shared/batch-fetch-engine';
import { MongoBatchAdapter } from './mongo-batch-adapter';

export type MongoBatchFetchDescriptor = BatchFetchDescriptor;

export async function executeMongoBatchFetches(
  db: Db,
  parentDocs: any[],
  descriptors: BatchFetchDescriptor[],
  metadataGetter: MetadataGetter,
  maxDepth: number = 3,
  currentDepth: number = 0,
  parentTableName?: string,
): Promise<void> {
  const adapter = new MongoBatchAdapter(db);
  const engine = new BatchFetchEngine(adapter, metadataGetter);
  await engine.execute(
    parentDocs,
    descriptors,
    maxDepth,
    currentDepth,
    parentTableName,
  );
}
