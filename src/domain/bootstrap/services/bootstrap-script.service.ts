import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import { ExecutorEngineService } from '../../../engine/executor-engine/services/executor-engine.service';
import { CacheService } from '../../../engine/cache/services/cache.service';
import { RepoRegistryService } from '../../../engine/cache/services/repo-registry.service';
import { TDynamicContext } from '../../../shared/types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { InstanceService } from '../../../shared/services/instance.service';
import { createFetchHelper } from '../../../shared/helpers/fetch.helper';
import {
  BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import { transformCode } from '../../../engine/executor-engine/code-transformer';

export class BootstrapScriptService {
  private readonly logger = new Logger(BootstrapScriptService.name);
  private readonly queryBuilderService: QueryBuilderService;
  private readonly executorEngineService: ExecutorEngineService;
  private readonly cacheService: CacheService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly instanceService: InstanceService;
  private readonly eventEmitter: EventEmitter2;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    executorEngineService: ExecutorEngineService;
    cacheService: CacheService;
    repoRegistryService: RepoRegistryService;
    instanceService: InstanceService;
    eventEmitter: EventEmitter2;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.executorEngineService = deps.executorEngineService;
    this.cacheService = deps.cacheService;
    this.repoRegistryService = deps.repoRegistryService;
    this.instanceService = deps.instanceService;
    this.eventEmitter = deps.eventEmitter;
  }

  async onMetadataLoaded() {
    const start = Date.now();
    await this.waitForTableExists();
    const scriptCount = await this.executeBootstrapScripts();
    this.logger.log(
      `Completed in ${Date.now() - start}ms (${scriptCount} scripts)`,
    );
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
    context: 'startup' | 'reload',
  ): Promise<R | void> {
    const lockKey = BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY;
    const lockValue = this.instanceService.getInstanceId();
    const lockTimeout = REDIS_TTL.BOOTSTRAP_LOCK_TTL;
    const lockAcquired = await this.cacheService.acquire(
      lockKey,
      lockValue,
      lockTimeout,
    );
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

  private async waitForTableExists(
    maxRetries = 10,
    delayMs = 500,
  ): Promise<void> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        if (this.queryBuilderService.isMongoDb()) {
          const db = this.queryBuilderService.getMongoDb();
          const collections = await db
            .listCollections({ name: 'bootstrap_script_definition' })
            .toArray();
          if (collections.length > 0) return;
        } else {
          const knex = this.queryBuilderService.getKnex();
          const exists = await knex.schema.hasTable(
            'bootstrap_script_definition',
          );
          if (exists) return;
        }
      } catch (error) {
        if (attempt === maxRetries) {
          throw new Error(
            `bootstrap_script_definition not found after ${maxRetries} attempts`,
          );
        }
        await new Promise((resolve) => setTimeout(resolve, delayMs));
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
    const result = await this.queryBuilderService.find({
      table: 'bootstrap_script_definition',
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
    ctx.$helpers.$fetch = createFetchHelper();
    ctx.$repos = this.repoRegistryService.createReposProxy(ctx);
    const timeoutMs = script.timeout || 30000;
    const executionResult = await this.executorEngineService.run(
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
