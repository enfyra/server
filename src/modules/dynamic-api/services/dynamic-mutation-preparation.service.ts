type MutableColumnMetadata = {
  name: string;
  isPublished?: boolean;
  isUpdatable?: boolean;
  type?: string;
};

type MutableTableMetadata = {
  columns?: MutableColumnMetadata[];
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export class DynamicMutationPreparationService {
  normalizeCreate(tableName: string, body: Record<string, unknown>) {
    try {
      return normalizeScriptRecord(tableName, body);
    } catch (error) {
      throw this.toScriptBadRequest(error);
    }
  }

  normalizeUpdate(
    tableName: string,
    body: Record<string, unknown>,
    existing: Record<string, unknown>,
  ) {
    try {
      return normalizeScriptPatch(tableName, body, existing);
    } catch (error) {
      throw this.toScriptBadRequest(error);
    }
  }

  normalizeFlowStep(body: Record<string, unknown>) {
    try {
      return normalizeFlowStepScriptConfig(body);
    } catch (error) {
      throw this.toScriptBadRequest(error);
    }
  }

  prepareUpdateBody(
    data: Record<string, unknown>,
    tableMetadata: unknown,
  ): Record<string, unknown> {
    if (!isRecord(tableMetadata)) return data;
    const columns = (tableMetadata as MutableTableMetadata).columns;
    if (!columns) return data;

    const prepared = { ...data };
    for (const column of columns) {
      if (column.isUpdatable === false) delete prepared[column.name];
      if (
        column.isPublished === false &&
        this.isEmptyUnpublishedValue(prepared[column.name], column.type)
      ) {
        delete prepared[column.name];
      }
    }
    return prepared;
  }

  private isEmptyUnpublishedValue(value: unknown, type: string | undefined): boolean {
    const stringLike = [
      'varchar',
      'text',
      'uuid',
      'ObjectId',
      'enum',
      'simple-json',
      'code',
      'array-select',
      'richtext',
      'date',
      'datetime',
      'timestamp',
    ].includes(type ?? '');
    return value === null || value === undefined || (stringLike && value === '');
  }

  private toScriptBadRequest(error: unknown): BadRequestException {
    const details = error instanceof Error ? error : null;
    return new BadRequestException(
      `Invalid script source: ${details?.message ?? String(error) ?? 'Invalid script source'}`,
      {
        code:
          isRecord(error) && typeof error.code === 'string'
            ? error.code
            : details?.name ?? 'SCRIPT_VALIDATION_ERROR',
      },
    );
  }
}
import { BadRequestException } from '../../../domain/exceptions';
import {
  normalizeFlowStepScriptConfig,
  normalizeScriptPatch,
  normalizeScriptRecord,
} from '../../../shared/utils/script-code.util';
