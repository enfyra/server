import { Injectable, Logger } from '@nestjs/common';
import { OnEvent, EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { RepoRegistryService } from '../../../infrastructure/cache/services/repo-registry.service';
import { TDynamicContext } from '../../../shared/types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
  REDIS_TTL
} from '../../../shared/utils/constant';
import { transformCode } from '../../../infrastructure/handler-executor/code-transformer';
import { CACHE_EVENTS, CACHE_IDENTIFIERS, shouldReloadCache } from '../../../shared/utils/cache-events.constants';

@Injectable()
export class BootstrapScriptService {
  private readonly logger = new Logger(BootstrapScriptService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private handlerExecutorService: HandlerExecutorService,
    private cacheService: CacheService,
    private repoRegistryService: RepoRegistryService,
    private instanceService: InstanceService,
    private eventEmitter: EventEmitter2,
  ) {}

  @OnEvent(CACHE_EVENTS.METADATA_LOADED)
  async onMetadataLoaded() {
    const start = Date.now();
    await this.waitForTableExists();
    const scriptCount = await this.executeBootstrapScripts();
    this.logger.log(`Completed in ${Date.now() - start}ms (${scriptCount} scripts)`);
  }

  @OnEvent(CACHE_EVENTS.INVALIDATE)
  async handleCacheInvalidation(payload: { tableName: string; action: string }) {
    if (shouldReloadCache(payload.tableName, CACHE_IDENTIFIERS.BOOTSTRAP)) {
      this.logger.log(`Cache invalidation event received for table: ${payload.tableName}`);
      await this.reloadBootstrapScripts();
    }
  }

  async reloadBootstrapScripts() {
    const start = Date.now();
    await this.withBootstrapLock(async () => {
      await this.cacheService.clearAll();
      await this.executeBootstrapScriptsWithoutLock();
    }, 'reload');
    this.logger.log(`Reload completed in ${Date.now() - start}ms`);
  }

  private async withBootstrapLock<R>(
    operation: () => Promise<R>,
    context: 'startup' | 'reload'
  ): Promise<R | void> {
    const lockKey = BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY;
    const lockValue = this.instanceService.getInstanceId();
    const lockTimeout = REDIS_TTL.BOOTSTRAP_LOCK_TTL;
    const lockAcquired = await this.cacheService.acquire(lockKey, lockValue, lockTimeout);
    if (!lockAcquired) {
      this.logger.log(`Skipped - another instance is executing`);
      return;
    }
    try {
      return await operation();
    } catch (error) {
      this.logger.error(`${context} failed:`, error);
      throw error;
    } finally {
      await this.cacheService.release(lockKey, lockValue);
    }
  }

  private async waitForTableExists(maxRetries = 10, delayMs = 500): Promise<void> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        if (this.queryBuilder.isMongoDb()) {
          const db = this.queryBuilder.getMongoDb();
          const collections = await db.listCollections({ name: 'bootstrap_script_definition' }).toArray();
          if (collections.length > 0) return;
        } else {
          const knex = this.queryBuilder.getKnex();
          const exists = await knex.schema.hasTable('bootstrap_script_definition');
          if (exists) return;
        }
      } catch (error) {
        if (attempt === maxRetries) {
          throw new Error(`bootstrap_script_definition not found after ${maxRetries} attempts`);
        }
        await new Promise(resolve => setTimeout(resolve, delayMs));
      }
    }
  }

  private async executeBootstrapScripts(): Promise<number> {
    let scriptCount = 0;
    await this.withBootstrapLock(async () => {
      scriptCount = await this.executeBootstrapScriptsWithoutLock();
    }, 'startup');
    return scriptCount;
  }

  private async executeBootstrapScriptsWithoutLock(): Promise<number> {
    const result = await this.queryBuilder.select({
      tableName: 'bootstrap_script_definition',
      filter: { isEnabled: { _eq: true } },
      sort: ['priority'],
    });
    const scripts = result.data;
    for (const script of scripts) {
      if (script.logic) {
        script.logic = transformCode(script.logic);
      }
    }
    let executedCount = 0;
    for (const script of scripts) {
      try {
        if (!script.logic || script.logic.trim() === '') {
          continue;
        }
        await this.executeScript(script);
        executedCount++;
      } catch (error) {
        this.logger.error(`Script ${script.name} failed:`, error);
      }
    }
    return executedCount;
  }

  private async executeScript(script: any) {
    const ctx: TDynamicContext = {
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $logs: (...args: any[]) => {
        this.logger.log(`[${script.name}] ${args.join(' ')}`);
      },
      $helpers: {
        autoSlug: (text: string) => text.toLowerCase().replace(/\s+/g, '-'),
      },
      $cache: this.cacheService,
      $repos: {} as any,
      $share: {
        $logs: [],
      },
    };
    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    const timeoutMs = script.timeout || 30000;
    const executionResult = await this.handlerExecutorService.run(
      script.logic,
      ctx,
      timeoutMs,
    );
    return executionResult;
  }

  async reloadBootstrapScriptsWithoutClear() {
    await this.executeBootstrapScriptsWithoutLock();
  }
}
