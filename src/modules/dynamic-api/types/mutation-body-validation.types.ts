export type TMutationBodyMode = 'create' | 'update';

export interface TMutationBodyValidationOptions {
  tableName: string;
  mode: TMutationBodyMode;
  body: unknown;
  user: unknown;
  recordId?: unknown;
}

export interface TMutationBodyValidationResult {
  body: Record<string, unknown>;
  changed: boolean;
}
