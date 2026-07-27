import type { TDynamicContext } from '../../../shared/types';

export type RuntimeSchemaMetadataTable = 'enfyra_column' | 'enfyra_relation';

export interface RuntimeMetadataSchemaMutationResult {
  preview?: unknown;
  recordId?: string | number;
  ownerTableId: string | number;
  affectedTables?: string[];
}

export interface RuntimeMetadataSchemaMutationInput {
  tableName: RuntimeSchemaMetadataTable;
  recordId?: string | number;
  data?: Record<string, any>;
  existing?: Record<string, any>;
  context?: TDynamicContext;
}
