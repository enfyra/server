import { Injectable, Logger } from '@nestjs/common';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { BcryptService } from '../../auth/services/bcrypt.service';
import * as fs from 'fs';
import * as path from 'path';

// Import processors
import { BaseTableProcessor } from '../processors/base-table-processor';
import { UserDefinitionProcessor } from '../processors/user-definition.processor';
import { MenuDefinitionProcessor } from '../processors/menu-definition.processor';
import { RouteDefinitionProcessor } from '../processors/route-definition.processor';
import { RouteHandlerDefinitionProcessor } from '../processors/route-handler-definition.processor';
import { MethodDefinitionProcessor } from '../processors/method-definition.processor';
import { HookDefinitionProcessor } from '../processors/hook-definition.processor';
import { SettingDefinitionProcessor } from '../processors/setting-definition.processor';
import { ExtensionDefinitionProcessor } from '../processors/extension-definition.processor';
import { FolderDefinitionProcessor } from '../processors/folder-definition.processor';
import { BootstrapScriptDefinitionProcessor } from '../processors/bootstrap-script-definition.processor';
import { GenericTableProcessor } from '../processors/generic-table.processor';

const initJson = JSON.parse(
  fs.readFileSync(
    path.join(process.cwd(), 'src/core/bootstrap/data/init.json'),
    'utf8',
  ),
);

@Injectable()
export class DefaultDataService {
  private readonly logger = new Logger(DefaultDataService.name);
  private readonly processors = new Map<string, BaseTableProcessor>();

  constructor(
    private readonly dataSourceService: DataSourceService,
    private readonly bcryptService: BcryptService,
    // Inject specific processors
    private readonly userProcessor: UserDefinitionProcessor,
    private readonly menuProcessor: MenuDefinitionProcessor,
    private readonly routeProcessor: RouteDefinitionProcessor,
    private readonly routeHandlerProcessor: RouteHandlerDefinitionProcessor,
    private readonly methodProcessor: MethodDefinitionProcessor,
    private readonly hookProcessor: HookDefinitionProcessor,
    private readonly settingProcessor: SettingDefinitionProcessor,
    private readonly extensionProcessor: ExtensionDefinitionProcessor,
    private readonly folderProcessor: FolderDefinitionProcessor,
    private readonly bootstrapScriptProcessor: BootstrapScriptDefinitionProcessor,
  ) {
    this.initializeProcessors();
  }

  private initializeProcessors(): void {
    // Register specific processors
    this.processors.set('user_definition', this.userProcessor);
    this.processors.set('menu_definition', this.menuProcessor);
    this.processors.set('route_definition', this.routeProcessor);
    this.processors.set('route_handler_definition', this.routeHandlerProcessor);
    this.processors.set('method_definition', this.methodProcessor);
    this.processors.set('hook_definition', this.hookProcessor);
    this.processors.set('setting_definition', this.settingProcessor);
    this.processors.set('extension_definition', this.extensionProcessor);
    this.processors.set('folder_definition', this.folderProcessor);
    this.processors.set('bootstrap_script_definition', this.bootstrapScriptProcessor);
    
    // Dynamic processors for remaining tables - auto-detect from initJson
    const allTables = Object.keys(initJson);
    const registeredTables = Array.from(this.processors.keys());
    
    for (const tableName of allTables) {
      if (!registeredTables.includes(tableName)) {
        this.processors.set(tableName, new GenericTableProcessor(tableName));
      }
    }
  }

  async insertAllDefaultRecords(): Promise<void> {
    this.logger.log('🚀 Starting default data upsert with refactored processors...');
    
    let totalCreated = 0;
    let totalSkipped = 0;

    for (const [tableName, rawRecords] of Object.entries(initJson)) {
      const processor = this.processors.get(tableName);
      if (!processor) {
        this.logger.warn(`⚠️ No processor found for table '${tableName}', skipping.`);
        continue;
      }

      if (!rawRecords || (Array.isArray(rawRecords) && rawRecords.length === 0)) {
        this.logger.debug(`❎ Table '${tableName}' has no default data, skipping.`);
        continue;
      }

      this.logger.log(`🔄 Processing table '${tableName}'...`);

      try {
        const repo = this.dataSourceService.getRepository(tableName);
        const records = Array.isArray(rawRecords) ? rawRecords : [rawRecords];
        
        // Dynamic context based on processor needs
        let context: any = undefined;
        if (tableName === 'menu_definition') {
          context = { repo };
        }
        // Add more context rules as needed for other processors
        
        const result = await processor.process(records, repo, context);
        
        totalCreated += result.created;
        totalSkipped += result.skipped;
        
        this.logger.log(
          `✅ Completed '${tableName}': ${result.created} created, ${result.skipped} skipped`
        );
      } catch (error) {
        this.logger.error(`❌ Error processing table '${tableName}': ${error.message}`);
        this.logger.debug(`Error details:`, error);
      }
    }

    this.logger.log(
      `🎉 Default data upsert completed! Total: ${totalCreated} created, ${totalSkipped} skipped`
    );
  }
}