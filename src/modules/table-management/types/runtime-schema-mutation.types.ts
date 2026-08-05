import type {
  SchemaMutationBackend,
  SchemaMutationContract,
  SchemaMutationLogicalChange,
} from '../../../shared/types/schema-mutation-contract.types';

export type RuntimeSchemaOperation = 'create' | 'update' | 'delete';

export interface RuntimeSchemaColumnContract {
  name: string;
  type: string;
  isNullable: boolean;
  isPrimary: boolean;
  isGenerated: boolean;
  defaultValue: unknown;
  description: string;
  values: unknown;
  isUnique: boolean;
  isPublished: boolean;
  isUpdatable: boolean;
  isEncrypted: boolean;
  isIndex: boolean;
  options: unknown;
  metadata: unknown;
  placeholder: string;
}

export interface RuntimeSchemaRelationContract {
  propertyName: string;
  type: string;
  targetTableName: string;
  mappedBy: string;
  foreignKeyColumn: string;
  junctionTableName: string;
  isNullable: boolean;
  onDelete: string;
  inversePropertyName: string;
  description: string;
  isEager: boolean;
  isInverseEager: boolean;
  isPublished: boolean;
  isUpdatable: boolean;
}

export interface RuntimeTableSchemaContract {
  name: string;
  description: string;
  alias: string;
  isSingleRecord: boolean;
  graphqlEnabled: boolean;
  validateBody: boolean;
  columns: readonly RuntimeSchemaColumnContract[];
  relations: readonly RuntimeSchemaRelationContract[];
  uniques: unknown;
  indexes: unknown;
}

export interface RuntimeSchemaNormalizationOptions {
  backend?: SchemaMutationBackend;
  mode?: 'intent' | 'persisted';
}

export type RuntimeSchemaChangeKind =
  | 'create-table'
  | 'delete-table'
  | 'alter-table-metadata'
  | 'sync-policy-metadata'
  | 'rename-table'
  | 'add-column'
  | 'remove-column'
  | 'rename-column'
  | 'alter-column'
  | 'add-relation'
  | 'remove-relation'
  | 'alter-relation'
  | 'add-unique'
  | 'remove-unique'
  | 'add-index'
  | 'remove-index';

export interface RuntimeSchemaLogicalChange extends SchemaMutationLogicalChange {
  kind: RuntimeSchemaChangeKind;
  before?: unknown;
  after?: unknown;
}

export interface RuntimeSchemaCascadeWarning {
  owningRelationId: string;
  owningPropertyName: string;
  owningSourceTableName: string;
  cascadeDeletesInverseRelations: readonly {
    inverseSourceTableName: string;
    propertyName: string;
    relationId: string;
  }[];
}

export interface RuntimeSchemaDiff {
  tableName: string;
  operation: RuntimeSchemaOperation;
  schemaChanged: boolean;
  policyMetadataChanged: boolean;
  isDestructive: boolean;
  removedColumns: readonly string[];
  addedColumns: readonly string[];
  renamedColumns: readonly { from: string; to: string }[];
  changedColumns: readonly string[];
  removedRelations: readonly string[];
  addedRelations: readonly string[];
  removedUniques: readonly string[];
  addedUniques: readonly string[];
  removedIndexes: readonly string[];
  addedIndexes: readonly string[];
  owningSideInverseCascadeWarnings: readonly RuntimeSchemaCascadeWarning[];
}

export interface RuntimeSchemaAffectedResources {
  tables: readonly string[];
  relationIds: readonly string[];
  cacheTables: readonly string[];
}

export interface RuntimeSchemaContractContext {
  operation: RuntimeSchemaOperation;
  actorId: string | null;
  tableId: string | null;
  tableName: string;
  sourceRevision: string | null;
  targetRevision: string | null;
  sourcePolicyMetadataRevision: string | null;
  targetPolicyMetadataRevision: string | null;
  executionBodyRevision: string | null;
  source: RuntimeTableSchemaContract | null;
  target: RuntimeTableSchemaContract | null;
  executionTarget: RuntimeTableSchemaContract | null;
  diff: RuntimeSchemaDiff;
  confirmationDigest: string;
  affectedResources: RuntimeSchemaAffectedResources;
}

export type RuntimeSchemaCommandKind =
  | 'attest-source'
  | 'capture-compensation'
  | 'apply-physical-change'
  | 'apply-metadata-change'
  | 'apply-artifacts'
  | 'attest-target'
  | 'stage-cache'
  | 'commit-database'
  | 'activate-runtime'
  | 'complete-change';

export interface RuntimeSchemaPhysicalPlanPayload {
  backend: 'postgresql' | 'mysql' | 'mongodb';
  upStatements?: readonly string[];
  upBatch?: string;
  downStatements?: readonly string[];
  downBatch?: string;
  upDiff?: unknown;
  downDiff?: unknown;
  metadataUpdate?: unknown;
  activeTableName?: string;
}

export interface RuntimeSchemaMutationCommand {
  kind: RuntimeSchemaCommandKind;
  change?: RuntimeSchemaLogicalChange;
  physicalPlan?: RuntimeSchemaPhysicalPlanPayload;
}

export type RuntimeSchemaMutationContract = SchemaMutationContract<
  RuntimeSchemaContractContext,
  RuntimeSchemaLogicalChange,
  RuntimeSchemaMutationCommand
>;

export interface RuntimeSchemaContractCompilation {
  contract: RuntimeSchemaMutationContract;
  requiredConfirmHash: string;
}

export interface RuntimeSchemaContractCompileInput {
  operation: RuntimeSchemaOperation;
  tableName: string;
  tableId?: unknown;
  currentUser?: unknown;
  beforeMetadata?: unknown;
  afterMetadata?: unknown;
  data?: unknown;
  requestContext?: unknown;
}

export interface RuntimeSchemaConstraintConflict {
  index: string[];
  uniqueFields: string[];
  uniqueConstraints: Array<{
    fields: string[];
    matchingFields: string[];
  }>;
}
