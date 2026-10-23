import type { Express, RequestHandler } from 'express';
import { NotFoundException } from '../../domain/exceptions';
import type { RequestWithRouteData } from '../../shared/types/dynamic-context.types';
import { getRouteCandidatePaths, normalizeRoutePath } from '../../shared/utils/route-match.util';

const requireNativeOperationBinding: RequestHandler = (request, _response, next) => {
  const req = request as RequestWithRouteData;
  const operation = req.routeData;
  const config = operation?.routeMethodConfig;
  const nativePath: unknown = req.route?.path;
  const bound = operation &&
    config?.available === true &&
    (config.method?.name ?? config.method) === req.method &&
    typeof nativePath === 'string' &&
    getRouteCandidatePaths(operation, req.method).some((path) =>
      normalizeRoutePath(path).toLowerCase() === normalizeRoutePath(nativePath).toLowerCase(),
    ) &&
    Object.entries(req.params).every(([name, value]) => operation.params?.[name] === value) &&
    Object.keys(operation.params ?? {}).length === Object.keys(req.params).length;
  next(bound ? undefined : new NotFoundException());
};

export function createNativeRouteRegistrar(app: Express): Express {
  const methods = new Set(['get', 'post', 'put', 'patch', 'delete', 'head', 'options', 'all']);
  return new Proxy(app, {
    get(target, property, receiver) {
      const value: unknown = Reflect.get(target, property, receiver);
      if (typeof property !== 'string' || !methods.has(property) || typeof value !== 'function') return value;
      return (path: string, ...handlers: RequestHandler[]) => Reflect.apply(value, target, [path, requireNativeOperationBinding, ...handlers]);
    },
  });
}
