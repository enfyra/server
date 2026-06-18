import ts from 'typescript';
import { transformTemplateSyntax } from './template-syntax.util';
import { SCRIPT_TABLE_LEGACY_FIELDS } from './script-table-contract.constants';

type ScriptLanguage = 'javascript' | 'typescript';

interface ScriptFields {
  scriptLanguage?: ScriptLanguage | string | null;
  sourceCode?: string | null;
  compiledCode?: string | null;
  [key: string]: any;
}

interface ExecutableScriptResult {
  code: string | null;
  compiledCode?: string | null;
  shouldPersistCompiledCode: boolean;
}

interface ScriptContractRepairResult extends ExecutableScriptResult {
  sourceCode: string | null;
  scriptLanguage: ScriptLanguage;
}

export function getScriptLegacyField(tableName: string): string | undefined {
  return scriptContractService.getLegacyField(tableName);
}

export function isScriptTable(tableName: string): boolean {
  return scriptContractService.isScriptTable(tableName);
}

export function normalizeScriptLanguage(value: unknown): ScriptLanguage {
  return scriptContractService.normalizeLanguage(value);
}

class ScriptContractService {
  getLegacyField(tableName: string): string | undefined {
    return SCRIPT_TABLE_LEGACY_FIELDS[tableName];
  }

  isScriptTable(tableName: string): boolean {
    return tableName in SCRIPT_TABLE_LEGACY_FIELDS;
  }

  normalizeLanguage(value: unknown): ScriptLanguage {
    return value === 'javascript' ? 'javascript' : 'typescript';
  }

  compileSource(
    sourceCode: string | null | undefined,
    scriptLanguage: unknown,
  ): string | null {
    if (sourceCode == null || sourceCode === '') return null;

    const transformed = transformTemplateSyntax(String(sourceCode));
    const language = this.normalizeLanguage(scriptLanguage);
    const compiled =
      language === 'javascript'
        ? transformed
        : this.transpileTypeScript(transformed);

    this.assertExecutableJavaScript(compiled);
    return compiled;
  }

  isExecutableJavaScript(code: string | null | undefined): boolean {
    if (!code) return false;
    try {
      this.assertExecutableJavaScript(code);
      return true;
    } catch {
      return false;
    }
  }

  normalizeRecord(tableName: string, record: ScriptFields): ScriptFields {
    if (
      !this.isScriptTable(tableName) ||
      !record ||
      typeof record !== 'object'
    ) {
      return record;
    }

    const normalized = { ...record };
    const legacyField = this.getLegacyField(tableName);
    if (
      legacyField &&
      (normalized.sourceCode === undefined ||
        normalized.sourceCode === null ||
        normalized.sourceCode === '') &&
      normalized[legacyField] !== undefined
    ) {
      normalized.sourceCode = normalized[legacyField];
    }

    normalized.scriptLanguage = this.normalizeLanguage(
      normalized.scriptLanguage,
    );
    normalized.compiledCode = this.compileSource(
      normalized.sourceCode,
      normalized.scriptLanguage,
    );

    if (legacyField && legacyField in normalized) {
      delete normalized[legacyField];
    }

    return normalized;
  }

  normalizePatch(
    tableName: string,
    patch: ScriptFields,
    existing?: ScriptFields | null,
  ): ScriptFields {
    if (!this.isScriptTable(tableName) || !patch || typeof patch !== 'object') {
      return patch;
    }

    const legacyField = this.getLegacyField(tableName);
    const touchesSource =
      Object.prototype.hasOwnProperty.call(patch, 'sourceCode') ||
      (legacyField
        ? Object.prototype.hasOwnProperty.call(patch, legacyField)
        : false);
    const touchesLanguage = Object.prototype.hasOwnProperty.call(
      patch,
      'scriptLanguage',
    );

    if (!touchesSource && !touchesLanguage) return patch;

    const sourceCode = touchesSource
      ? (patch.sourceCode ?? (legacyField ? patch[legacyField] : undefined))
      : (existing?.sourceCode ??
        (legacyField && existing ? existing[legacyField] : undefined));
    const scriptLanguage = touchesLanguage
      ? patch.scriptLanguage
      : existing?.scriptLanguage;
    const normalized: ScriptFields = { ...patch };

    if (touchesSource) {
      normalized.sourceCode = sourceCode ?? null;
    }
    normalized.scriptLanguage = this.normalizeLanguage(scriptLanguage);
    normalized.compiledCode = this.compileSource(
      sourceCode,
      normalized.scriptLanguage,
    );

    if (legacyField && legacyField in normalized) {
      delete normalized[legacyField];
    }

    return normalized;
  }

  getExecutableScript(record: ScriptFields): string | null {
    return this.resolveExecutableScript(record).code;
  }

  resolveExecutableScript(record: ScriptFields): ExecutableScriptResult {
    if (!record || typeof record !== 'object') {
      return {
        code: null,
        compiledCode: null,
        shouldPersistCompiledCode: false,
      };
    }

    if (typeof record.sourceCode === 'string' && record.sourceCode !== '') {
      const compiledCode = this.compileSource(
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
        const compiledCode = this.compileSource(
          legacyCode,
          record.scriptLanguage,
        );
        return {
          code: compiledCode,
          compiledCode,
          shouldPersistCompiledCode: compiledCode !== record.compiledCode,
        };
      }
    }

    if (
      typeof record.compiledCode === 'string' &&
      record.compiledCode !== '' &&
      this.isExecutableJavaScript(record.compiledCode)
    ) {
      return {
        code: record.compiledCode,
        compiledCode: record.compiledCode,
        shouldPersistCompiledCode: false,
      };
    }

    return { code: null, compiledCode: null, shouldPersistCompiledCode: false };
  }

  normalizeFlowStepScriptConfig(record: any): any {
    if (!record || typeof record !== 'object') return record;
    const type = record.type;
    if (type !== 'script' && type !== 'condition') return record;

    const normalized = { ...record };
    let config = record.config;
    if (typeof config === 'string') {
      try {
        config = JSON.parse(config);
      } catch {
        return record;
      }
    }
    const configObject = config && typeof config === 'object' ? config : {};

    normalized.sourceCode =
      normalized.sourceCode ??
      configObject.sourceCode ??
      configObject.code ??
      null;
    normalized.scriptLanguage = this.normalizeLanguage(
      normalized.scriptLanguage ?? configObject.scriptLanguage,
    );
    normalized.compiledCode = this.compileSource(
      normalized.sourceCode,
      normalized.scriptLanguage,
    );

    const normalizedConfig = { ...configObject };
    delete normalizedConfig.sourceCode;
    delete normalizedConfig.scriptLanguage;
    delete normalizedConfig.compiledCode;
    delete normalizedConfig.code;

    return {
      ...normalized,
      config:
        typeof record.config === 'string'
          ? JSON.stringify(normalizedConfig)
          : normalizedConfig,
    };
  }

  repairContract(record: ScriptFields): ScriptContractRepairResult {
    const scriptLanguage = this.normalizeLanguage(record?.scriptLanguage);
    const sourceCode = record?.sourceCode ?? null;
    const compiledCode = this.compileSource(sourceCode, scriptLanguage);
    return {
      sourceCode,
      scriptLanguage,
      code: compiledCode,
      compiledCode,
      shouldPersistCompiledCode: compiledCode !== record?.compiledCode,
    };
  }

  private transpileTypeScript(transformedCode: string): string {
    const result = ts.transpileModule(transformedCode, {
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

  private assertExecutableJavaScript(code: string | null): void {
    if (!code) return;
    new Function(`return (async () => {\n"use strict";\n${code}\n});`);
  }
}

const scriptContractService = new ScriptContractService();

export function compileScriptSource(
  sourceCode: string | null | undefined,
  scriptLanguage: unknown,
): string | null {
  return scriptContractService.compileSource(sourceCode, scriptLanguage);
}

export function isExecutableJavaScript(
  code: string | null | undefined,
): boolean {
  return scriptContractService.isExecutableJavaScript(code);
}

export function normalizeScriptRecord(
  tableName: string,
  record: ScriptFields,
): ScriptFields {
  return scriptContractService.normalizeRecord(tableName, record);
}

export function normalizeScriptPatch(
  tableName: string,
  patch: ScriptFields,
  existing?: ScriptFields | null,
): ScriptFields {
  return scriptContractService.normalizePatch(tableName, patch, existing);
}

export function getExecutableScript(record: ScriptFields): string | null {
  return scriptContractService.getExecutableScript(record);
}

export function resolveExecutableScript(
  record: ScriptFields,
): ExecutableScriptResult {
  return scriptContractService.resolveExecutableScript(record);
}

export function normalizeFlowStepScriptConfig(record: any): any {
  return scriptContractService.normalizeFlowStepScriptConfig(record);
}
