import { QueryBuilderService } from '@enfyra/kernel';
import { DatabaseConfigService } from '../../../shared/services';
import {
  compileScriptSource,
  normalizeScriptLanguage,
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../../shared/utils/script-code.util';

export class RuntimeScriptRepairService {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: { queryBuilderService: QueryBuilderService }) {
    this.queryBuilderService = deps.queryBuilderService;
  }

  async repairScriptRecord(
    tableName: string,
    record: any,
  ): Promise<string | null> {
    const previousCompiledCode = record?.compiledCode;
    const normalized = normalizeScriptRecord(tableName, record);
    Object.assign(record, normalized);

    if (typeof record?.sourceCode === 'string' && record.sourceCode !== '') {
      const compiledCode = compileScriptSource(
        record.sourceCode,
        record.scriptLanguage,
      );
      if (compiledCode !== previousCompiledCode) {
        record.compiledCode = compiledCode;
        await this.persistRecordPatch(tableName, record, { compiledCode });
      }
      return compiledCode;
    }

    const result = resolveExecutableScript(record);
    if (result.shouldPersistCompiledCode) {
      record.compiledCode = result.compiledCode;
      await this.persistRecordPatch(tableName, record, {
        compiledCode: result.compiledCode,
      });
    }
    return result.code;
  }

  async repairFlowStepScriptRecord(step: any): Promise<void> {
    const idField = DatabaseConfigService.getPkField();

    if (!step.sourceCode && !step.compiledCode) {
      return;
    }

    step.scriptLanguage = normalizeScriptLanguage(step.scriptLanguage);
    const result = resolveExecutableScript(step);
    step.compiledCode = result.compiledCode;

    if (!result.shouldPersistCompiledCode) {
      return;
    }

    const id = step[idField];
    if (id == null) return;
    await this.queryBuilderService.update('enfyra_flow_step', id, {
      sourceCode: step.sourceCode ?? null,
      scriptLanguage: step.scriptLanguage ?? 'typescript',
      compiledCode: step.compiledCode ?? null,
    });
  }

  private async persistRecordPatch(
    tableName: string,
    record: any,
    patch: Record<string, any>,
  ): Promise<void> {
    const id = DatabaseConfigService.getRecordId(record);
    if (id == null) return;
    await this.queryBuilderService.update(tableName, id, patch);
  }
}
