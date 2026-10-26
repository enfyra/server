export interface SharedHelperDefinition {
  id?: string | number;
  name: string;
  isEnabled: boolean;
  code: string;
  compiledCode?: string | null;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
}

export interface SharedHelperRunner {
  run<T = unknown>(name: string, params?: unknown): Promise<T>;
}
