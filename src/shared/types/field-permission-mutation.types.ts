import type { TCompiledFieldPolicy } from '../../engines/cache';

export type TFieldPermissionMutationAction = 'create' | 'update';

export interface TFieldPermissionMutationTableMetadata {
  columns?: Array<{
    name: string;
    isPublished?: boolean;
  }>;
  relations?: Array<{
    propertyName: string;
    isPublished?: boolean;
  }>;
}

export interface TFieldPermissionMutationPolicyReader {
  getFieldPermissionPoliciesFor(
    user: unknown,
    tableName: string,
    action: TFieldPermissionMutationAction,
  ): TCompiledFieldPolicy[] | Promise<TCompiledFieldPolicy[]>;
}

export interface TStripUnauthorizedMutationFieldsOptions {
  action: TFieldPermissionMutationAction;
  body: Record<string, unknown>;
  policyReader: TFieldPermissionMutationPolicyReader;
  record?: unknown;
  tableMeta: TFieldPermissionMutationTableMetadata | null | undefined;
  tableName: string;
  user: any;
}
