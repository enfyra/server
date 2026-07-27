export type BootstrapDataFiles = {
  snapshot: Record<string, any>;
  defaultData: Record<string, any>;
  dataMigration: Record<string, any>;
};

export type BootstrapValidationIssue = {
  file: 'snapshot.ts' | 'default-data.ts' | 'data-migration.ts';
  table: string;
  path?: string;
  field: string;
  message: string;
};
