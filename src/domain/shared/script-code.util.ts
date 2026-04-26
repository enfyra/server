import ts from 'typescript';
import { transformCode } from './code-transformer';

export type ScriptLanguage = 'javascript' | 'typescript';

export interface ScriptFields {
  scriptLanguage?: ScriptLanguage | string | null;
  sourceCode?: string | null;
  compiledCode?: string | null;
  [key: string]: any;
}

export interface ExecutableScriptResult {
  code: string | null;
  compiledCode?: string | null;
  shouldPersistCompiledCode: boolean;
}

const SCRIPT_TABLE_LEGACY_FIELDS: Record<string, string> = {
  route_handler_definition: 'logic',
  pre_hook_definition: 'code',
  post_hook_definition: 'code',
  bootstrap_script_definition: 'logic',
  websocket_definition: 'connectionHandlerScript',
  websocket_event_definition: 'handlerScript',
};

export function getScriptLegacyField(tableName: string): string | undefined {
  return SCRIPT_TABLE_LEGACY_FIELDS[tableName];
}

export function isScriptTable(tableName: string): boolean {
  return tableName in SCRIPT_TABLE_LEGACY_FIELDS;
}

export function normalizeScriptLanguage(value: unknown): ScriptLanguage {
  return value === 'javascript' ? 'javascript' : 'typescript';
}

export function compileScriptSource(
  sourceCode: string | null | undefined,
  scriptLanguage: unknown,
): string | null {
  if (sourceCode == null || sourceCode === '') return null;

  const transformed = transformCode(String(sourceCode));
  if (normalizeScriptLanguage(scriptLanguage) === 'javascript') {
    return transformed;
  }

  const result = ts.transpileModule(transformed, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.CommonJS,
      importsNotUsedAsValues: ts.ImportsNotUsedAsValues.Remove,
      sourceMap: false,
      inlineSourceMap: false,
      inlineSources: false,
    },
    reportDiagnostics: true,
  });

  const error = result.diagnostics?.find(
    (diagnostic) => diagnostic.category === ts.DiagnosticCategory.Error,
  );
  if (error) {
    throw new Error(ts.flattenDiagnosticMessageText(error.messageText, '\n'));
  }

  return result.outputText.trimEnd();
}

export function isExecutableJavaScript(code: string | null | undefined): boolean {
  if (!code) return false;
  try {
    new Function(`return (async () => {\n"use strict";\n${code}\n});`);
    return true;
  } catch {
    return false;
  }
}

export function normalizeScriptRecord(
  tableName: string,
  record: ScriptFields,
): ScriptFields {
  if (!isScriptTable(tableName) || !record || typeof record !== 'object') {
    return record;
  }

  const normalized = { ...record };
  const legacyField = getScriptLegacyField(tableName);
  if (
    legacyField &&
    (normalized.sourceCode === undefined ||
      normalized.sourceCode === null ||
      normalized.sourceCode === '') &&
    normalized[legacyField] !== undefined
  ) {
    normalized.sourceCode = normalized[legacyField];
  }

  normalized.scriptLanguage = normalizeScriptLanguage(
    normalized.scriptLanguage,
  );
  if (
    normalized.compiledCode === undefined ||
    normalized.compiledCode === null ||
    normalized.compiledCode === ''
  ) {
    normalized.compiledCode = compileScriptSource(
      normalized.sourceCode,
      normalized.scriptLanguage,
    );
  }

  if (legacyField && legacyField in normalized) {
    delete normalized[legacyField];
  }

  return normalized;
}

export function normalizeScriptPatch(
  tableName: string,
  patch: ScriptFields,
  existing?: ScriptFields | null,
): ScriptFields {
  if (!isScriptTable(tableName) || !patch || typeof patch !== 'object') {
    return patch;
  }

  const legacyField = getScriptLegacyField(tableName);
  const touchesSource =
    Object.prototype.hasOwnProperty.call(patch, 'sourceCode') ||
    (legacyField ? Object.prototype.hasOwnProperty.call(patch, legacyField) : false);
  const touchesLanguage = Object.prototype.hasOwnProperty.call(
    patch,
    'scriptLanguage',
  );

  if (!touchesSource && !touchesLanguage) return patch;

  const sourceCode = touchesSource
    ? patch.sourceCode ?? (legacyField ? patch[legacyField] : undefined)
    : existing?.sourceCode ??
      (legacyField && existing ? existing[legacyField] : undefined);
  const scriptLanguage = touchesLanguage
    ? patch.scriptLanguage
    : existing?.scriptLanguage;
  const compiledCode = compileScriptSource(sourceCode, scriptLanguage);
  const normalized: ScriptFields = { ...patch };

  if (touchesSource) {
    normalized.sourceCode = sourceCode ?? null;
  }
  normalized.scriptLanguage = normalizeScriptLanguage(scriptLanguage);
  normalized.compiledCode = compiledCode;

  if (legacyField && legacyField in normalized) {
    delete normalized[legacyField];
  }

  return normalized;
}

export function getExecutableScript(record: ScriptFields): string | null {
  return resolveExecutableScript(record).code;
}

export function resolveExecutableScript(
  record: ScriptFields,
): ExecutableScriptResult {
  if (!record || typeof record !== 'object') {
    return { code: null, compiledCode: null, shouldPersistCompiledCode: false };
  }

  if (
    typeof record.compiledCode === 'string' &&
    record.compiledCode !== '' &&
    isExecutableJavaScript(record.compiledCode)
  ) {
    return {
      code: record.compiledCode,
      compiledCode: record.compiledCode,
      shouldPersistCompiledCode: false,
    };
  }

  if (typeof record.sourceCode === 'string' && record.sourceCode !== '') {
    const compiledCode = compileScriptSource(
      record.sourceCode,
      record.scriptLanguage,
    );
    return {
      code: compiledCode,
      compiledCode,
      shouldPersistCompiledCode: compiledCode !== record.compiledCode,
    };
  }

  const legacyField = Object.keys(SCRIPT_TABLE_LEGACY_FIELDS).find(
    (tableName) => SCRIPT_TABLE_LEGACY_FIELDS[tableName] in record,
  );
  if (legacyField) {
    const legacyCode = record[SCRIPT_TABLE_LEGACY_FIELDS[legacyField]];
    if (typeof legacyCode === 'string' && legacyCode !== '') {
      const compiledCode = compileScriptSource(legacyCode, record.scriptLanguage);
      return {
        code: compiledCode,
        compiledCode,
        shouldPersistCompiledCode: compiledCode !== record.compiledCode,
      };
    }
  }

  if (typeof record.compiledCode === 'string' && record.compiledCode !== '') {
    return {
      code: record.compiledCode,
      compiledCode: record.compiledCode,
      shouldPersistCompiledCode: false,
    };
  }

  return { code: null, compiledCode: null, shouldPersistCompiledCode: false };
}

export function normalizeFlowStepScriptConfig(record: any): any {
  if (!record || typeof record !== 'object') return record;
  const type = record.type;
  if (type !== 'script' && type !== 'condition') return record;

  let config = record.config;
  if (typeof config === 'string') {
    try {
      config = JSON.parse(config);
    } catch {
      return record;
    }
  }
  if (!config || typeof config !== 'object') return record;

  const normalizedConfig = { ...config };
  normalizedConfig.sourceCode =
    normalizedConfig.sourceCode ?? normalizedConfig.code ?? null;
  normalizedConfig.scriptLanguage = normalizeScriptLanguage(
    normalizedConfig.scriptLanguage,
  );
  normalizedConfig.compiledCode = compileScriptSource(
    normalizedConfig.sourceCode,
    normalizedConfig.scriptLanguage,
  );
  delete normalizedConfig.code;

  return {
    ...record,
    config: typeof record.config === 'string'
      ? JSON.stringify(normalizedConfig)
      : normalizedConfig,
  };
}
