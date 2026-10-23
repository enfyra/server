import type { ICache } from '../../src/domain/shared/interfaces/cache.interface';
import type { RedisCacheService } from '../../src/engines/cache/services/redis-cache.service';
import type { TableMutationPayload, RouteExecutedPayload } from '../../src/shared/types/cache.types';
import type { RuntimeLogMetadata } from '../../src/shared/types/runtime-log.types';
import type { Logger } from '../../src/shared/logger';
import type { StorageReadableStream } from '../../src/modules/file-management/types/storage-stream.types';

type IsAny<T> = 0 extends (1 & T) ? true : false;
type IsUnknown<T> = IsAny<T> extends true ? false : unknown extends T ? [keyof T] extends [never] ? true : false : false;
type Assert<T extends true> = T;

type CacheRead = Assert<IsUnknown<Awaited<ReturnType<ICache['get']>>>>;
type CacheDeleteRead = Assert<IsUnknown<Awaited<ReturnType<ICache['getAndDelete']>>>>;
type RedisRead = Assert<IsUnknown<Awaited<ReturnType<RedisCacheService['get']>>>>;
type RedisDeleteRead = Assert<IsUnknown<Awaited<ReturnType<RedisCacheService['getAndDelete']>>>>;
type MutationData = Assert<IsUnknown<TableMutationPayload['data']>>;
type RouteResult = Assert<IsUnknown<RouteExecutedPayload['result']>>;
type MetadataValue = Assert<IsUnknown<RuntimeLogMetadata[string]>>;
type LogMessage = Assert<IsUnknown<Parameters<Logger['error']>[0]>>;
type StreamLength = Assert<StorageReadableStream['contentLength'] extends number | undefined ? true : false>;

export function storageBoundaryExamples(stream: StorageReadableStream): void {
  stream.contentLength = 12;
  // @ts-expect-error Storage stream length metadata must be numeric.
  stream.contentLength = '12';
}

export async function cacheBoundaryExamples(cache: ICache, redis: RedisCacheService): Promise<void> {
  const cached = await cache.get('dynamic');
  const removed = await redis.getAndDelete('dynamic');
  // @ts-expect-error Dynamic cache values require narrowing before property reads.
  cached.id;
  // @ts-expect-error Atomic removal does not establish the stored payload shape.
  removed.id;
  const known = await cache.get<{ id: string }>('owned-contract');
  known?.id.toUpperCase();
}
