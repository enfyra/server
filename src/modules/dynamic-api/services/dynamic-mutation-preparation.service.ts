import { BadRequestException } from '../../../domain/exceptions';
import {
  normalizeFlowStepScriptConfig,
  normalizeScriptPatch,
  normalizeScriptRecord,
} from '../../../shared/utils/script-code.util';

type MutableColumnMetadata = {
  name: string;
  isUpdatable?: boolean;
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
    }
    return prepared;
  }

  async prepareCreateBody(
    raw: any,
    tableName: string,
    tableMetadata: unknown,
    mutationAuthorizationService: {
      stripUnauthorizedDirectFields: (
        operation: 'create' | 'update',
        body: Record<string, unknown>,
      ) => Promise<Record<string, unknown>>;
      assertMutationSafety: (
        operation: 'create' | 'update' | 'delete',
        body: any,
        existing: any,
      ) => Promise<void>;
    },
    tableValidationService: {
      assertTableValid: (options: {
        operation: 'create' | 'update' | 'delete';
        tableName: string;
        tableMetadata: any;
      }) => Promise<void>;
    },
    strategy?: { normalizeCreate?: (body: any) => Promise<void> | void },
  ): Promise<Record<string, any>> {
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
      throw new BadRequestException('data is required and must be an object');
    }

    let body = { ...raw };
    body = await mutationAuthorizationService.stripUnauthorizedDirectFields(
      'create' as const,
      body,
    );
    await tableValidationService.assertTableValid({
      operation: 'create',
      tableName,
      tableMetadata,
    });
    await mutationAuthorizationService.assertMutationSafety(
      'create',
      body,
      null,
    );
    await strategy?.normalizeCreate?.(body);
    Object.assign(body, this.normalizeCreate(tableName, body));
    if (tableName === 'enfyra_flow_step') {
      Object.assign(body, this.normalizeFlowStep(body));
    }
    if (body.id !== undefined) {
      delete body.id;
    }
    if (body._id !== undefined) {
      delete body._id;
    }
    return body;
  }

  async executeCreateBody(
    body: Record<string, any>,
    tableName: string,
    mutationAuthorizationService: {
      runWithFieldPermissionCheck: <T>(fn: () => Promise<T>) => Promise<T>;
      runWithMutationPolicy: <T>(fn: () => Promise<T>) => Promise<T>;
    },
    queryBuilderService: {
      insert: (tableName: string, body: any) => Promise<any>;
    },
  ): Promise<any> {
    return mutationAuthorizationService.runWithFieldPermissionCheck(() =>
      mutationAuthorizationService.runWithMutationPolicy(() =>
        queryBuilderService.insert(tableName, body),
      ),
    );
  }

  private toScriptBadRequest(error: unknown): BadRequestException {
    const details = error instanceof Error ? error : null;
    return new BadRequestException(
      `Invalid script source: ${details?.message ?? String(error) ?? 'Invalid script source'}`,
      {
        code:
          isRecord(error) && typeof error.code === 'string'
            ? error.code
            : (details?.name ?? 'SCRIPT_VALIDATION_ERROR'),
      },
    );
  }
}
