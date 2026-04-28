import { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import type { Cradle } from '../../container';
import type { AwilixContainer } from 'awilix';
import {
  MetadataCacheService,
  ColumnRuleCacheService,
} from '../../engines/cache';
import { buildZodFromMetadata } from '../../shared/utils/zod-from-metadata';
import { parseOrBadRequest } from '../../shared/utils/zod-parse.util';
import { BadRequestException } from '../../domain/exceptions';

type Mode = 'create' | 'update';

const schemaCache = new Map<string, z.ZodType>();

function cacheKey(
  tableName: string,
  mode: Mode,
  version: number | string,
): string {
  return `${tableName}:${mode}:${version}`;
}

export function invalidateBodyValidationCache(): void {
  schemaCache.clear();
}

function buildSchema(
  mode: Mode,
  tableMeta: any,
  metadata: ReturnType<MetadataCacheService['getDirectMetadata']>,
  ruleCache: ColumnRuleCacheService,
): z.ZodType | null {
  if (!tableMeta) return null;

  return buildZodFromMetadata({
    tableMeta,
    mode,
    rulesForColumn: (columnId) => ruleCache.getRulesForColumnSync(columnId),
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
  const metadataCache = container.cradle.metadataCacheService;
  const ruleCache = container.cradle.columnRuleCacheService;
  const eventEmitter = container.cradle.eventEmitter;

  eventEmitter?.on('cache:metadata:loaded', invalidateBodyValidationCache);
  eventEmitter?.on('metadata_LOADED', invalidateBodyValidationCache);
  eventEmitter?.on('column-rule_LOADED', invalidateBodyValidationCache);

  return (req: Request, res: Response, next: NextFunction) => {
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
    const metadata = metadataCache.getDirectMetadata();
    const version = metadata?.version ?? 0;
    const key = cacheKey(mainTable.name, mode, version);
    let tableMeta = mainTable;

    let schema = schemaCache.get(key);
    if (!schema) {
      const buildMetadata = metadataCache.getDirectMetadata();
      tableMeta = buildMetadata?.tables?.get(mainTable.name) ?? null;
      const built = tableMeta
        ? buildSchema(mode, tableMeta, buildMetadata, ruleCache)
        : null;
      if (!built) return next();
      if (schemaCache.size > 500) schemaCache.clear();
      schemaCache.set(key, built);
      schema = built;
    }

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
      const validationBody =
        mode === 'update'
          ? stripNonUpdatableColumnsForPatch(body, tableMeta)
          : body;
      validateBody(validationBody);
    } catch (err) {
      return next(err);
    }

    next();
  };
}
