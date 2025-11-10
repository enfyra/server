import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { DynamicRepository } from '../../../modules/dynamic-api/repositories/dynamic.repository';
import { TableHandlerService } from '../../../modules/table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { SystemProtectionService } from '../../../modules/dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../../modules/dynamic-api/services/table-validation.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { InstanceService } from '../../../shared/services/instance.service';
import {
  BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY,
  REDIS_TTL
} from '../../../shared/utils/constant';
import { transformCode } from '../../../infrastructure/handler-executor/code-transformer';

@Injectable()
export class BootstrapScriptService implements OnApplicationBootstrap {
  private readonly logger = new Logger(BootstrapScriptService.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private metadataCacheService: MetadataCacheService,
    private handlerExecutorService: HandlerExecutorService,
    private cacheService: CacheService,
    private tableHandlerService: TableHandlerService,
    private queryEngine: QueryEngine,
    private routeCacheService: RouteCacheService,
    private storageConfigCacheService: StorageConfigCacheService,
    private systemProtectionService: SystemProtectionService,
    private tableValidationService: TableValidationService,
    private redisPubSubService: RedisPubSubService,
    private instanceService: InstanceService,
  ) {}

  async onApplicationBootstrap() {
    this.logger.log('Starting BootstrapScriptService...');
    
    // Wait for bootstrap_script_definition table to be created
    await this.waitForTableExists();
    await this.executeBootstrapScripts();
    this.logger.log('BootstrapScriptService completed successfully');
  }

  async reloadBootstrapScripts() {
    this.logger.log('Reloading BootstrapScriptService...');
    
    await this.withBootstrapLock(async () => {
      // Clear all Redis data (like app restart)
      await this.cacheService.clearAll();
      this.logger.log('Cleared all Redis data');
      
      // Execute bootstrap scripts (already have lock from wrapper method)
      await this.executeBootstrapScriptsWithoutLock();
      this.logger.log('BootstrapScriptService reload completed successfully');
    }, 'reload');
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
      this.logger.log(`🔴 Bootstrap ${context} skipped - another instance is executing (${lockValue})`);
      return;
    }
    
    try {
      this.logger.log(`Bootstrap ${context} acquired lock - starting execution (${lockValue})`);
      return await operation();
      
    } catch (error) {
      this.logger.error(`BootstrapScriptService ${context} failed:`, error);
      throw error;
    } finally {
      await this.cacheService.release(lockKey, lockValue);
      this.logger.log(`Bootstrap ${context} lock released (${lockValue})`);
    }
  }

  private async waitForTableExists(maxRetries = 10, delayMs = 500): Promise<void> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        if (this.queryBuilder.isMongoDb()) {
          // MongoDB: check collection exists
          const db = this.queryBuilder.getMongoDb();
          const collections = await db.listCollections({ name: 'bootstrap_script_definition' }).toArray();
          if (collections.length > 0) {
            this.logger.log(`bootstrap_script_definition collection found on attempt ${attempt}`);
            return;
          }
        } else {
          // SQL: check table exists
          const knex = this.queryBuilder.getKnex();
          const exists = await knex.schema.hasTable('bootstrap_script_definition');
          if (exists) {
            this.logger.log(`bootstrap_script_definition table found on attempt ${attempt}`);
            return;
          }
        }
      } catch (error) {
        this.logger.log(`⏳ Attempt ${attempt}/${maxRetries}: bootstrap_script_definition not ready, waiting ${delayMs}ms...`);
        if (attempt < maxRetries) {
          await new Promise(resolve => setTimeout(resolve, delayMs));
        } else {
          throw new Error(`bootstrap_script_definition not found after ${maxRetries} attempts`);
        }
      }
    }
  }

  private async executeBootstrapScripts() {
    await this.withBootstrapLock(async () => {
      await this.executeBootstrapScriptsWithoutLock();
    }, 'startup');
  }

  private async executeBootstrapScriptsWithoutLock() {
    // Get enabled scripts ordered by priority
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

    this.logger.log(`Found ${scripts.length} bootstrap scripts to execute`);

    for (const script of scripts) {
      try {
        if (!script.logic || script.logic.trim() === '') {
          this.logger.warn(`Script ${script.name} has no logic, skipping`);
          continue;
        }
        
        this.logger.log(`Executing script: ${script.name} (priority: ${script.priority})`);
        
        await this.executeScript(script);
        this.logger.log(`Script ${script.name} completed successfully`);
      } catch (error) {
        this.logger.error(`Script ${script.name} failed:`, error);
        // Continue with other scripts even if one fails
      }
    }
      
    this.logger.log(`Bootstrap scripts execution completed`);
  }

  private async executeScript(script: any) {
    const tablesResult = await this.queryBuilder.select({ tableName: 'table_definition' });
    const tableDefinitions = tablesResult.data;
    
    const dynamicFindEntries = await Promise.all(
      tableDefinitions.map(async (tableDef) => {
        const tableName = tableDef.name;
        const dynamicRepo = new DynamicRepository({
          context: null,
          tableName: tableName,
          tableHandlerService: this.tableHandlerService,
          queryBuilder: this.queryBuilder,
          metadataCacheService: this.metadataCacheService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          storageConfigCacheService: this.storageConfigCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
          bootstrapScriptService: this,
          redisPubSubService: this.redisPubSubService,
        });

        await dynamicRepo.init();
        this.logger.debug(`📦 Created dynamic repository for table: ${tableName}`);

        return [tableName, dynamicRepo];
      }),
    );

    const repos = Object.fromEntries(dynamicFindEntries);

    // Create minimal context for bootstrap scripts
    const ctx: TDynamicContext = {
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $logs: (...args: any[]) => {
        this.logger.log(`[${script.name}] ${args.join(' ')}`);
      },
      $helpers: {
        autoSlug: (text: string) => text.toLowerCase().replace(/\s+/g, '-'),
      },
      $cache: this.cacheService,
      $repos: repos,
      $share: {
        $logs: [],
      },
    };

    // Set context for each repo after repos object is created (avoid circular reference)
    Object.values(ctx.$repos).forEach((repo: any) => {
      repo.context = ctx;
    });

    // Execute script with timeout
    const timeoutMs = script.timeout || 30000; // Default 30 seconds for bootstrap
    this.logger.log(`Executing script with timeout: ${timeoutMs}ms`);
    
    const executionResult = await this.handlerExecutorService.run(
      script.logic,
      ctx,
      timeoutMs,
    );

    this.logger.log(`Script execution result:`, executionResult);
    return executionResult;
  }

  async reloadBootstrapScriptsWithoutClear() {
    await this.executeBootstrapScriptsWithoutLock();
  }
}