import { EventEmitter2 } from 'eventemitter2';
import { Logger } from '../../../shared/logger';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { CACHE_EVENTS } from '../../../shared/utils/cache-events.constants';
import type {
  RuntimeCacheIdentifier,
  RuntimeRegistryEntry,
  RuntimeRegistrySnapshot,
} from '../types/runtime-registry.types';

export interface RuntimeCacheViewSource {
  getCacheAsync?: () => Promise<unknown>;
  getRawCache?: () => unknown;
}

export class RuntimeRegistryService {
  private readonly logger = new Logger(RuntimeRegistryService.name);
  private readonly eventEmitter?: EventEmitter2;
  private readonly entries = new Map<
    RuntimeCacheIdentifier,
    RuntimeRegistryEntry
  >();
  private initialized = false;

  constructor(deps: { eventEmitter?: EventEmitter2; lazyRef?: unknown } = {}) {
    this.eventEmitter = deps.eventEmitter;
  }

  init(): void {
    if (this.initialized) return;
    this.initialized = true;
  }

  async publishFromCache(
    identifier: RuntimeCacheIdentifier,
    service: RuntimeCacheViewSource,
  ): Promise<RuntimeRegistrySnapshot> {
    const nextVersion = (this.entries.get(identifier)?.version ?? 0) + 1;
    this.entries.set(identifier, {
      identifier,
      version: nextVersion,
      status: 'building',
    });

    try {
      const data =
        typeof service.getCacheAsync === 'function'
          ? await service.getCacheAsync()
          : service.getRawCache?.();
      if (data === undefined) {
        throw new Error(`Cache ${identifier} did not return active data`);
      }
      const activatedAt = new Date().toISOString();
      const entry: RuntimeRegistryEntry = {
        identifier,
        version: nextVersion,
        status: 'activated',
        activatedAt,
        data,
      };
      this.entries.set(identifier, entry);
      this.eventEmitter?.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
        identifier,
        version: nextVersion,
        activatedAt,
      });
      return { identifier, version: nextVersion, activatedAt, data };
    } catch (error) {
      const message = getErrorMessage(error);
      this.entries.set(identifier, {
        identifier,
        version: nextVersion,
        status: 'failed',
        failedAt: new Date().toISOString(),
        error: message,
      });
      this.logger.error(
        `Failed to publish runtime cache ${identifier}: ${message}`,
      );
      throw error;
    }
  }

  getSnapshot<T = unknown>(
    identifier: RuntimeCacheIdentifier,
  ): RuntimeRegistrySnapshot<T> | undefined {
    const entry = this.entries.get(identifier);
    if (!entry || entry.status !== 'activated' || entry.data === undefined) {
      return undefined;
    }
    return {
      identifier,
      version: entry.version,
      activatedAt: entry.activatedAt!,
      data: entry.data as T,
    };
  }

  getEntry(
    identifier: RuntimeCacheIdentifier,
  ): RuntimeRegistryEntry | undefined {
    const entry = this.entries.get(identifier);
    return entry ? { ...entry } : undefined;
  }
}
