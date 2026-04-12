import { Knex } from 'knex';
import {
  BatchFetchEngine,
  BatchFetchDescriptor,
  MetadataGetter,
} from '../shared/batch-fetch-engine';
import { SqlBatchAdapter } from './sql-batch-adapter';

export type { BatchFetchDescriptor } from '../shared/batch-fetch-engine';

export async function executeBatchFetches(
  knex: Knex,
  parentRows: any[],
  descriptors: BatchFetchDescriptor[],
  metadataGetter: MetadataGetter,
  maxDepth: number = 3,
  currentDepth: number = 0,
  parentTableName?: string,
  dbType: 'postgres' | 'mysql' | 'sqlite' = 'postgres',
): Promise<void> {
  const adapter = new SqlBatchAdapter(knex, dbType);
  const engine = new BatchFetchEngine(adapter, metadataGetter);
  await engine.execute(parentRows, descriptors, maxDepth, currentDepth, parentTableName);
}
