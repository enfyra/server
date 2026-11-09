export type ScriptRepairCallback = (compiledCode: string) => unknown | Promise<unknown>;

export type DynamicCodeBlock = {
  code: string;
  sourceCode?: string | null;
  scriptLanguage?: string | null;
  scriptId?: string | number | null;
  onCompiledCodeRepair?: ScriptRepairCallback;
  type: 'preHook' | 'handler' | 'postHook';
};
