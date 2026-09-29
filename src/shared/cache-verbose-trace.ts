import { AsyncLocalStorage } from 'node:async_hooks';
import { Logger } from './logger';

interface CacheVerboseTraceScope {
  cacheName: string;
  cacheIdentifier: string;
  reloadType: 'full' | 'partial';
}

const cacheVerboseTraceStore = new AsyncLocalStorage<CacheVerboseTraceScope>();
const cacheQueryLogger = new Logger('CacheQueryTrace');
const TRACED_QUERY_METHODS = new Set([
  'aggregate',
  'count',
  'find',
  'findById',
  'findOne',
  'findOneWhere',
  'findWhere',
  'select',
]);

export function runWithCacheVerboseTrace<T>(
  scope: CacheVerboseTraceScope,
  callback: () => Promise<T>,
): Promise<T> {
  return cacheVerboseTraceStore.run(scope, callback);
}

export function getCacheVerboseTraceScope(): CacheVerboseTraceScope | undefined {
  return cacheVerboseTraceStore.getStore();
}

export function createCacheQueryTracingProxy<T extends object>(
  queryBuilderService: T,
): T {
  return new Proxy(queryBuilderService, {
    get(target, property, receiver) {
      const value = Reflect.get(target, property, receiver);
      if (typeof value !== 'function') return value;

      const methodName = String(property);
      const bound = value.bind(target);
      if (!TRACED_QUERY_METHODS.has(methodName)) return bound;

      return async (...args: unknown[]) => {
        const scope = getCacheVerboseTraceScope();
        if (!scope) return bound(...args);

        const startedAt = Date.now();
        try {
          const result = await bound(...args);
          cacheQueryLogger.verbose(
            formatCacheQueryTrace({
              scope,
              methodName,
              args,
              result,
              durationMs: Date.now() - startedAt,
              databaseType: getDatabaseType(target),
            }),
          );
          return result;
        } catch (error) {
          cacheQueryLogger.verbose(
            formatCacheQueryTrace({
              scope,
              methodName,
              args,
              durationMs: Date.now() - startedAt,
              databaseType: getDatabaseType(target),
              failed: true,
            }),
          );
          throw error;
        }
      };
    },
  });
}

export function summarizeCacheValue(value: unknown): string {
  if (value instanceof Map) return `map(${value.size})`;
  if (Array.isArray(value)) return `array(${value.length})`;
  if (value == null) return String(value);
  if (typeof value !== 'object') return typeof value;

  const entries = Object.entries(value as Record<string, unknown>)
    .slice(0, 20)
    .map(([key, item]) => {
      if (Array.isArray(item)) return `${key}=array(${item.length})`;
      if (item instanceof Map) return `${key}=map(${item.size})`;
      if (item && typeof item === 'object') return `${key}=object`;
      return `${key}=${typeof item}`;
    });
  return `object(${entries.join(',')})`;
}

function formatCacheQueryTrace(options: {
  scope: CacheVerboseTraceScope;
  methodName: string;
  args: unknown[];
  result?: unknown;
  durationMs: number;
  databaseType: string;
  failed?: boolean;
}): string {
  const query = summarizeQueryArguments(options.methodName, options.args);
  const outcome = options.failed
    ? 'status=failed'
    : `status=ok rows=${getResultCount(options.result)}`;
  return [
    'cache-query',
    `cache=${options.scope.cacheName}`,
    `id=${options.scope.cacheIdentifier}`,
    `reload=${options.scope.reloadType}`,
    `backend=${options.databaseType}`,
    `operation=${options.methodName}`,
    query,
    outcome,
    `durationMs=${options.durationMs}`,
  ]
    .filter(Boolean)
    .join(' ');
}

function summarizeQueryArguments(methodName: string, args: unknown[]): string {
  const first = args[0];
  if (first == null || typeof first !== 'object' || Array.isArray(first)) {
    return `target=${summarizeScalar(first)}`;
  }

  const options = first as Record<string, unknown>;
  const table = options.table ?? options.tableName ?? 'unknown';
  const fields = summarizeFields(options.fields);
  const filter = summarizeFilterShape(options.filter ?? options.where);
  const sort = summarizeFields(options.sort);
  const page = summarizeScalar(options.page);
  const limit = summarizeScalar(options.limit);
  return [
    `table=${String(table)}`,
    `fields=${fields}`,
    `filter=${filter}`,
    `sort=${sort}`,
    `page=${page}`,
    `limit=${limit}`,
    methodName === 'aggregate' ? 'aggregate=present' : '',
  ]
    .filter(Boolean)
    .join(' ');
}

function summarizeFilterShape(value: unknown, depth = 0): string {
  if (value == null) return 'none';
  if (depth >= 6) return '…';
  if (Array.isArray(value)) {
    return `[${value.map((item) => summarizeFilterShape(item, depth + 1)).join(',')}]`;
  }
  if (typeof value !== 'object') return '?';

  const entries = Object.entries(value as Record<string, unknown>)
    .slice(0, 20)
    .map(([key, item]) => `${key}:${summarizeFilterShape(item, depth + 1)}`);
  return `{${entries.join(',')}}`;
}

function summarizeFields(value: unknown): string {
  if (Array.isArray(value)) {
    const fields = value.slice(0, 30).map(String);
    return `[${fields.join(',')}${value.length > fields.length ? ',…' : ''}]`;
  }
  if (typeof value === 'string') return value;
  return 'none';
}

function summarizeScalar(value: unknown): string {
  if (value == null) return 'none';
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value);
  }
  if (typeof value === 'string') return value.length > 40 ? 'string' : value;
  return typeof value;
}

function getResultCount(result: unknown): number {
  if (Array.isArray(result)) return result.length;
  if (result && typeof result === 'object') {
    const record = result as Record<string, unknown>;
    if (Array.isArray(record.data)) return record.data.length;
    if (typeof record.count === 'number') return record.count;
    return 1;
  }
  return result == null ? 0 : 1;
}

function getDatabaseType(target: object): string {
  const candidate = target as { getDatabaseType?: () => unknown };
  try {
    return String(candidate.getDatabaseType?.() ?? 'unknown');
  } catch {
    return 'unknown';
  }
}
