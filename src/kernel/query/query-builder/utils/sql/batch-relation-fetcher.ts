import { Knex } from 'knex';
import {
  BatchFetchEngine,
  BatchFetchDescriptor,
  MetadataGetter,
  BatchTrace,
} from '../../../query-dsl/batch-fetch-engine';
import { SqlBatchAdapter } from './sql-batch-adapter';

export type { BatchFetchDescriptor } from '../../../query-dsl/batch-fetch-engine';

export async function executeBatchFetches(
  knex: Knex,
  parentRows: any[],
  descriptors: BatchFetchDescriptor[],
  metadataGetter: MetadataGetter,
  maxDepth: number = 3,
  currentDepth: number = 0,
  parentTableName?: string,
  dbType: 'postgres' | 'mysql' | 'sqlite' = 'postgres',
  metadata?: any,
  trace?: BatchTrace,
): Promise<void> {
  const adapter = new SqlBatchAdapter(knex, dbType, metadata);
  const engine = new BatchFetchEngine(adapter, metadataGetter, trace);
  await engine.execute(
    parentRows,
    descriptors,
    maxDepth,
    currentDepth,
    parentTableName,
  );
}
