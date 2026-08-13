export type RouteKind = 'generic' | 'schema' | 'table';

export interface MutationContext {
  tableName: string;
  id: string | number;
  body: Record<string, any>;
  existing: Record<string, any> | null;
}

export interface TableRouteStrategy {
  kind: RouteKind;
  normalizeCreate?(body: Record<string, any>): Promise<void> | void;
  normalizeUpdate?(
    body: Record<string, any>,
    existing: Record<string, any>,
    id: string | number,
  ): Promise<void> | void;
  afterCreateWrite?(ctx: MutationContext): Promise<void> | void;
  afterUpdateWrite?(ctx: MutationContext): Promise<void> | void;
  afterDeleteWrite?(ctx: MutationContext): Promise<void> | void;
  afterUpdateReload?(ctx: MutationContext): Promise<void> | void;
  afterDeleteReload?(ctx: MutationContext): Promise<void> | void;
}

export interface TableRouteHandlers {
  isSchemaRoutedTable(tableName: string): boolean;
  isTableDefinition(tableName: string): boolean;
  normalizeRouteMethods: (
    body: any,
    existing: any,
    field: 'publicMethods' | 'skipRoleGuardMethods',
  ) => void;
  normalizeExtension: (body: any, method: 'POST' | 'PATCH') => Promise<void>;
  assertColumnRuleUnique: (
    body: any,
    editingId: string | number | null,
  ) => Promise<void>;
  assertGuardCreate: (body: any) => Promise<void>;
  assertGuardUpdate: (id: string | number, body: any) => Promise<void>;
  assertGuardRuleCreate: (body: any) => Promise<void>;
  assertGuardRuleUpdate: (id: string | number, body: any) => Promise<void>;
  assertFlowTriggerBody: (body: any) => void;
  normalizeUserPassword: (body: Record<string, any>) => Promise<void>;
  normalizeFolderSlug: (body: Record<string, any>) => void;
  postStorageDefault: (currentId: string | number) => Promise<void>;
  postFlowJobs: (id: string | number, name: string) => Promise<unknown>;
  postUserRevocation: (id: string | number) => Promise<unknown>;
}
