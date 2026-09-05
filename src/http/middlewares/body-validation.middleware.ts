import { Request, Response, NextFunction } from 'express';
import type { ZodType } from 'zod';
import type { Cradle } from '../../container';
import type { AwilixContainer } from 'awilix';
import { buildZodFromMetadata } from '../../shared/utils/zod-from-metadata';
import { parseOrBadRequest } from '../../shared/utils/zod-parse.util';
import { BadRequestException } from '../../domain/exceptions';
import { CACHE_IDENTIFIERS } from '../../shared/utils/cache-events.constants';
import { stripUnauthorizedMutationFields } from '../../shared/utils/strip-unauthorized-mutation-fields.util';

type Mode = 'create' | 'update';

export function invalidateBodyValidationCache(): void {
  return;
}

function buildSchema(
  mode: Mode,
  tableMeta: any,
  metadata: any,
  rulesByColumn: Map<string, any[]>,
): ZodType | null {
  if (!tableMeta) return null;

  return buildZodFromMetadata({
    tableMeta,
    mode,
    rulesForColumn: (columnId) => rulesByColumn.get(String(columnId)) ?? [],
    getTableMetadata: (name) => metadata?.tables?.get(name) ?? null,
  });
}

function stripNonUpdatableColumnsForPatch(body: any, tableMeta: any): any {
  if (!body || typeof body !== 'object' || Array.isArray(body)) return body;

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

function assignValidationBody(req: Request, routeData: any, body: any): void {
  req.body = body;
  if (routeData?.context) {
    routeData.context.$body = body;
  }
}

export function bodyValidationMiddleware(container: AwilixContainer<Cradle>) {
  const runtimeRegistryService = container.cradle.runtimeRegistryService;
  const queryBuilderService = container.cradle.queryBuilderService;

  return async (req: Request, res: Response, next: NextFunction) => {
    const method = req.method?.toUpperCase();
    if (method !== 'POST' && method !== 'PATCH') return next();

    const routeData: any = (req as any).routeData;
    const mainTable: any = routeData?.mainTable;
    if (!mainTable?.name) return next();
    if (mainTable.validateBody === false) return next();

    const path: string | undefined = routeData?.path;
    const canonicalCollection = `/${mainTable.name}`;
    const canonicalItem = `/${mainTable.name}/:id`;
    if (path !== canonicalCollection && path !== canonicalItem) return next();

    const mode: Mode = method === 'POST' ? 'create' : 'update';
    const metadata: any = runtimeRegistryService.requireMetadata();
    if (!metadata) return next();
    const rulesByColumn: Map<string, any[]> =
      runtimeRegistryService.requireActiveData(CACHE_IDENTIFIERS.COLUMN_RULE);
    const tableMeta = metadata.tables?.get(mainTable.name) ?? null;
    const schema = tableMeta
      ? buildSchema(mode, tableMeta, metadata, rulesByColumn)
      : null;
    if (!schema) return next();

    const body = req.body;
    if (body === null || body === undefined) {
      return next(new BadRequestException(['body is required']));
    }
    if (Array.isArray(body)) {
      return next(
        new BadRequestException(['body must be an object, not an array']),
      );
    }
    if (typeof body !== 'object') {
      return next(new BadRequestException(['body must be an object']));
    }

    const validateBody = (validationBody: any) => {
      if (validationBody !== body) {
        assignValidationBody(req, routeData, validationBody);
      }
      parseOrBadRequest(schema, validationBody);
    };

    try {
      const user = routeData?.context?.$user ?? (req as any).user ?? null;
      let permissionRecord = body;
      if (mode === 'update' && req.params?.id != null) {
        const idField =
          tableMeta.columns?.find((column: any) => column?.isPrimary)?.name ??
          'id';
        const existing = await queryBuilderService.find({
          table: mainTable.name,
          fields: '*',
          filter: { [idField]: { _eq: req.params.id } },
          limit: 1,
        });
        permissionRecord = existing?.data?.[0] ?? body;
      }
      const permissionBody = await stripUnauthorizedMutationFields({
        action: mode,
        body,
        policyReader: runtimeRegistryService,
        record: permissionRecord,
        tableMeta,
        tableName: mainTable.name,
        user,
      });
      const validationBody =
        mode === 'update'
          ? stripNonUpdatableColumnsForPatch(permissionBody, tableMeta)
          : permissionBody;
      validateBody(validationBody);
    } catch (err) {
      return next(err);
    }

    next();
  };
}
