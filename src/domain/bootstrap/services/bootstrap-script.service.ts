import { Logger } from '../../../shared/logger';
import { EventEmitter2 } from 'eventemitter2';
import { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { IExecutorEngine } from '../../shared/interfaces/executor-engine.interface';
import { ICache } from '../../shared/interfaces/cache.interface';
import { IRepoRegistry } from '../../shared/interfaces/repo-registry.interface';
import { TDynamicContext } from '../../../shared/types';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import {
  InstanceService,
  DatabaseConfigService,
} from '../../../shared/services';
import { createFetchHelper } from '../../../shared/helpers';
import {
  BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
  REDIS_TTL,
} from '../../../shared/utils/constant';
import {
  normalizeScriptRecord,
  resolveExecutableScript,
} from '../../../kernel/execution';

export class BootstrapScriptService {
  private readonly logger = new Logger(BootstrapScriptService.name);
  private readonly queryBuilderService: IQueryBuilder;
  private readonly executorEngineService: IExecutorEngine;
  private readonly cacheService: ICache;
  private readonly repoRegistryService: IRepoRegistry;
  private readonly instanceService: InstanceService;
  private readonly eventEmitter: EventEmitter2;

  constructor(deps: {
    queryBuilderService: IQueryBuilder;
    executorEngineService: IExecutorEngine;
    cacheService: ICache;
    repoRegistryService: IRepoRegistry;
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
    for (let i = 0; i < scripts.length; i++) {
      const script = normalizeScriptRecord(
        'bootstrap_script_definition',
        scripts[i],
      );
      const resolved = resolveExecutableScript(script);
      script.logic = resolved.code;
      if (resolved.shouldPersistCompiledCode) {
        script.compiledCode = resolved.compiledCode;
        const id = DatabaseConfigService.getRecordId(script);
        if (id != null) {
          await this.queryBuilderService.update(
            'bootstrap_script_definition',
            id,
            {
              compiledCode: resolved.compiledCode,
            },
          );
        }
      }
      scripts[i] = script;
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
