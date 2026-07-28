import type { TDynamicContext } from '../../../shared/types';
import type { TCreateTableBody } from './table-handler.types';

export type RuntimeSchemaMetadataTable = 'enfyra_column' | 'enfyra_relation';

export interface RuntimeMetadataSchemaMutationResult {
  preview?: unknown;
  recordId?: string | number;
  ownerTableId?: string | number;
  affectedTables?: string[];
  mutationId?: string;
  tableRenames?: Array<{ id: string | number; oldName: string; newName: string }>;
}

export interface RuntimeMetadataSchemaMutationInput {
  tableName: RuntimeSchemaMetadataTable;
  recordId?: string | number;
  data?: Record<string, any>;
  existing?: Record<string, any>;
  context?: TDynamicContext;
}

export interface RuntimeTableMutationInput {
  tableId?: string | number;
  body?: TCreateTableBody;
  existing?: Record<string, any>;
  context?: TDynamicContext;
}
