import { Request, Response, NextFunction } from 'express';
import { z } from 'zod';
import type { Cradle } from '../../container';
import type { AwilixContainer } from 'awilix';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { ColumnRuleCacheService } from '../../infrastructure/cache/services/column-rule-cache.service';
import { buildZodFromMetadata } from '../../shared/utils/zod-from-metadata';
import { parseOrBadRequest } from '../../shared/utils/zod-parse.util';
import { BadRequestException } from '../../core/exceptions/custom-exceptions';

type Mode = 'create' | 'update';

const schemaCache = new Map<string, z.ZodType>();

function cacheKey(tableName: string, mode: Mode, version: number | string): string {
  return `${tableName}:${mode}:${version}`;
}

export function invalidateBodyValidationCache(): void {
  schemaCache.clear();
}

function buildSchema(
  tableName: string,
  mode: Mode,
  metadataCache: MetadataCacheService,
  ruleCache: ColumnRuleCacheService,
): z.ZodType | null {
  const tableMeta = metadataCache.getDirectMetadata()?.tables?.get(tableName);
  if (!tableMeta) return null;

  return buildZodFromMetadata({
    tableMeta,
    mode,
    rulesForColumn: (columnId) => ruleCache.getRulesForColumnSync(columnId),
    getTableMetadata: (name) =>
      metadataCache.getDirectMetadata()?.tables?.get(name) ?? null,
  });
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
    const version = metadataCache.getDirectMetadata()?.version ?? 0;
    const key = cacheKey(mainTable.name, mode, version);

    let schema = schemaCache.get(key);
    if (!schema) {
      const built = buildSchema(mainTable.name, mode, metadataCache, ruleCache);
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
      return next(new BadRequestException(['body must be an object, not an array']));
    }
    if (typeof body !== 'object') {
      return next(new BadRequestException(['body must be an object']));
    }

    try {
      parseOrBadRequest(schema, body);
    } catch (err) {
      return next(err);
    }
    next();
  };
}
