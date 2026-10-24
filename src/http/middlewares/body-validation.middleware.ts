import { Request, Response, NextFunction } from 'express';
import type { Cradle } from '../../container';
import type { AwilixContainer } from 'awilix';
import { BadRequestException } from '../../domain/exceptions';

type Mode = 'create' | 'update';

export function invalidateBodyValidationCache(): void {
  return;
}

function assignValidationBody(req: Request, routeData: any, body: any): void {
  req.body = body;
  if (routeData?.context) {
    routeData.context.$body = body;
  }
}

export function bodyValidationMiddleware(container: AwilixContainer<Cradle>) {
  const mutationBodyValidationService =
    container.cradle.mutationBodyValidationService;

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
    const body = req.body;
    if (body === null || body === undefined) {
      return next(new BadRequestException(['body is required']));
    }

    try {
      const user = routeData?.context?.$user ?? (req as any).user ?? null;
      const recordId = routeData.params?.id ?? req.params?.id;
      const result = await mutationBodyValidationService.validate({
        tableName: mainTable.name,
        mode,
        body,
        user,
        ...(recordId !== undefined ? { recordId } : {}),
      });
      if (result.changed) {
        assignValidationBody(req, routeData, result.body);
      }
    } catch (err) {
      if (err instanceof BadRequestException) return next(err);
      throw err;
    }

    next();
  };
}
