import { BadRequestException } from '../../../domain/exceptions';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import { buildZodFromMetadata } from '../../../shared/utils/zod-from-metadata';
import { parseOrBadRequest } from '../../../shared/utils/zod-parse.util';
import { stripUnauthorizedMutationFields } from '../../../shared/utils/strip-unauthorized-mutation-fields.util';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type {
  TMutationBodyMode,
  TMutationBodyValidationOptions,
  TMutationBodyValidationResult,
} from '../types/mutation-body-validation.types';

export class MutationBodyValidationService {
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    runtimeRegistryService: RuntimeRegistryService;
    queryBuilderService: QueryBuilderService;
  }) {
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.queryBuilderService = deps.queryBuilderService;
  }

  async validate(
    options: TMutationBodyValidationOptions,
  ): Promise<TMutationBodyValidationResult> {
    const { tableName, mode, body, user, recordId } = options;
    if (body === null || body === undefined) {
      throw new BadRequestException(['body is required']);
    }
    if (Array.isArray(body)) {
      throw new BadRequestException(['body must be an object, not an array']);
    }
    if (typeof body !== 'object') {
      throw new BadRequestException(['body must be an object']);
    }

    const metadata: any = this.runtimeRegistryService.requireMetadata();
    const tableMeta = metadata?.tables?.get(tableName) ?? null;
    if (!tableMeta || tableMeta.validateBody === false) {
      return { body: body as Record<string, unknown>, changed: false };
    }
    const rulesByColumn: Map<string, any[]> =
      this.runtimeRegistryService.requireActiveData(
        CACHE_IDENTIFIERS.COLUMN_RULE,
      );
    const schema = buildZodFromMetadata({
      tableMeta,
      mode,
      rulesForColumn: (columnId) => rulesByColumn.get(String(columnId)) ?? [],
      getTableMetadata: (name) => metadata?.tables?.get(name) ?? null,
    });
    if (!schema) {
      return { body: body as Record<string, unknown>, changed: false };
    }

    let permissionRecord: Record<string, unknown> = body as Record<
      string,
      unknown
    >;
    if (mode === 'update' && recordId != null) {
      const idField =
        tableMeta.columns?.find((column: any) => column?.isPrimary)?.name ??
        'id';
      const existing = await this.queryBuilderService.find({
        table: tableName,
        fields: '*',
        filter: { [idField]: { _eq: recordId } },
        limit: 1,
      });
      permissionRecord = existing?.data?.[0] ?? permissionRecord;
    }

    const permissionBody = await stripUnauthorizedMutationFields({
      action: mode,
      body: body as Record<string, unknown>,
      policyReader: this.runtimeRegistryService,
      record: permissionRecord,
      tableMeta,
      tableName,
      user,
    });
    const validationBody =
      mode === 'update'
        ? stripNonUpdatableColumns(permissionBody, tableMeta)
        : permissionBody;

    parseOrBadRequest(schema, validationBody);
    return {
      body: validationBody,
      changed: validationBody !== body,
    };
  }
}

function stripNonUpdatableColumns(
  body: Record<string, unknown>,
  tableMeta: any,
): Record<string, unknown> {
  const nonUpdatableColumns = (tableMeta?.columns || [])
    .filter((column: any) => column?.isUpdatable === false)
    .map((column: any) => column.name)
    .filter(Boolean);

  if (nonUpdatableColumns.length === 0) return body;

  let stripped = body;
  for (const key of nonUpdatableColumns) {
    if (Object.prototype.hasOwnProperty.call(stripped, key)) {
      if (stripped === body) stripped = { ...body };
      delete stripped[key];
    }
  }
  return stripped;
}
