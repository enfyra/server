export type BootstrapDataFiles = {
  snapshot: Record<string, any>;
  defaultData: Record<string, any[]>;
  dataMigration: Record<string, any>;
};

export type BootstrapValidationIssue = {
  file: 'default-data.json' | 'data-migration.json';
  table: string;
  path?: string;
  field: string;
  message: string;
};
