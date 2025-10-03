import { Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { CacheService } from '../../../infrastructure/cache/services/cache.service';
import { DynamicRepository } from '../../../modules/dynamic-api/repositories/dynamic.repository';
import { TableHandlerService } from '../../../modules/table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { SystemProtectionService } from '../../../modules/dynamic-api/services/system-protection.service';
import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';
import { SchemaReloadService } from '../../../modules/schema-management/services/schema-reload.service';
import { RedisPubSubService } from '../../../infrastructure/cache/services/redis-pubsub.service';
import { 
  BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY, 
  REDIS_TTL 
} from '../../../shared/utils/constant';
import { Repository } from 'typeorm';

@Injectable()
export class BootstrapScriptService implements OnApplicationBootstrap {
  private readonly logger = new Logger(BootstrapScriptService.name);

  constructor(
    private dataSourceService: DataSourceService,
    private handlerExecutorService: HandlerExecutorService,
    private cacheService: CacheService,
    private tableHandlerService: TableHandlerService,
    private queryEngine: QueryEngine,
    private routeCacheService: RouteCacheService,
    private systemProtectionService: SystemProtectionService,
    private schemaReloadService: SchemaReloadService,
    private redisPubSubService: RedisPubSubService,
  ) {}

  async onApplicationBootstrap() {
    this.logger.log('🚀 Starting BootstrapScriptService...');
    
    // Wait for bootstrap_script_definition table to be created
    await this.waitForTableExists();
    await this.executeBootstrapScripts();
    this.logger.log('✅ BootstrapScriptService completed successfully');
  }

  async reloadBootstrapScripts() {
    this.logger.log('🔄 Reloading BootstrapScriptService...');
    
    await this.withBootstrapLock(async () => {
      // Clear all Redis data (like app restart)
      await this.cacheService.clearAll();
      this.logger.log('🧹 Cleared all Redis data');
      
      // Execute bootstrap scripts (already have lock from wrapper method)
      await this.executeBootstrapScriptsWithoutLock();
      this.logger.log('✅ BootstrapScriptService reload completed successfully');
    }, 'reload');
  }

  private async withBootstrapLock<R>(
    operation: () => Promise<R>, 
    context: 'startup' | 'reload'
  ): Promise<R | void> {
    const lockKey = BOOTSTRAP_SCRIPT_EXECUTION_LOCK_KEY;
    const lockValue = this.schemaReloadService.sourceInstanceId;
    const lockTimeout = REDIS_TTL.BOOTSTRAP_LOCK_TTL;
    
    const lockAcquired = await this.cacheService.acquire(lockKey, lockValue, lockTimeout);
    if (!lockAcquired) {
      this.logger.log(`🔴 Bootstrap ${context} skipped - another instance is executing (${this.schemaReloadService.sourceInstanceId})`);
      return; // Another instance is already handling
    }
    
    try {
      this.logger.log(`🔒 Bootstrap ${context} acquired lock - starting execution (${this.schemaReloadService.sourceInstanceId})`);
      return await operation();
      
    } catch (error) {
      this.logger.error(`❌ BootstrapScriptService ${context} failed:`, error);
      throw error;
    } finally {
      // Always release the lock
      await this.cacheService.release(lockKey, lockValue);
      this.logger.log(`🔓 Bootstrap ${context} lock released (${this.schemaReloadService.sourceInstanceId})`);
    }
  }

  private async waitForTableExists(maxRetries = 10, delayMs = 500): Promise<void> {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const scriptRepo = this.dataSourceService.getRepository('bootstrap_script_definition');
        if (scriptRepo && scriptRepo.find) {
          this.logger.log(`✅ bootstrap_script_definition table found on attempt ${attempt}`);
          return;
        }
      } catch (error) {
        this.logger.log(`⏳ Attempt ${attempt}/${maxRetries}: Table not ready yet, waiting ${delayMs}ms...`);
        if (attempt < maxRetries) {
          await new Promise(resolve => setTimeout(resolve, delayMs));
        } else {
          throw new Error(`bootstrap_script_definition table not found after ${maxRetries} attempts`);
        }
      }
    }
    
    throw new Error(`bootstrap_script_definition table not found after ${maxRetries} attempts`);
  }

  private async executeBootstrapScripts() {
    await this.withBootstrapLock(async () => {
      await this.executeBootstrapScriptsWithoutLock();
    }, 'startup');
  }

  private async executeBootstrapScriptsWithoutLock() {
    // Table should exist at this point due to waitForTableExists()
    const scriptRepo: Repository<any> = this.dataSourceService.getRepository('bootstrap_script_definition');
      
    // Get enabled scripts ordered by priority
    const scripts = await scriptRepo.find({
      where: { 
        isEnabled: true,
      },
      order: { priority: 'ASC' },
    });

    this.logger.log(`📋 Found ${scripts.length} bootstrap scripts to execute`);

    for (const script of scripts) {
      try {
        if (!script.logic || script.logic.trim() === '') {
          this.logger.warn(`⚠️ Script ${script.name} has no logic, skipping`);
          continue;
        }
        
        this.logger.log(`🔄 Executing script: ${script.name} (priority: ${script.priority})`);
        
        await this.executeScript(script);
        this.logger.log(`✅ Script ${script.name} completed successfully`);
      } catch (error) {
        this.logger.error(`❌ Script ${script.name} failed:`, error);
        // Continue with other scripts even if one fails
      }
    }
      
    this.logger.log(`✅ Bootstrap scripts execution completed`);
  }

  private async executeScript(script: any) {
    // Get all table definitions to create repositories
    const tableDefRepo = this.dataSourceService.getRepository('table_definition');
    const tableDefinitions = await tableDefRepo.find();
    
    // Create dynamic repositories for all tables (similar to route-detect middleware)
    const dynamicFindEntries = await Promise.all(
      tableDefinitions.map(async (tableDef) => {
        const tableName = (tableDef as any).name;
        const dynamicRepo = new DynamicRepository({
          context: null, // Will be set later to avoid circular reference
          tableName: tableName,
          tableHandlerService: this.tableHandlerService,
          dataSourceService: this.dataSourceService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          systemProtectionService: this.systemProtectionService,
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
    this.logger.log(`⏱️ Executing script with timeout: ${timeoutMs}ms`);
    
    const result = await this.handlerExecutorService.run(
      script.logic,
      ctx,
      timeoutMs,
    );

    this.logger.log(`📝 Script execution result:`, result);
    return result;
  }

  async reloadBootstrapScriptsWithoutClear() {
    await this.executeBootstrapScriptsWithoutLock();
  }
}