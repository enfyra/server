import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
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
import { RoutePermissionDefinitionProcessor } from '../processors/route-permission-definition.processor';
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
  private readonly dbType: string;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly configService: ConfigService,
    private readonly bcryptService: BcryptService,
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
    private readonly routePermissionProcessor: RoutePermissionDefinitionProcessor,
  ) {
    this.dbType = this.configService.get<string>('DB_TYPE') || 'mysql';
    this.initializeProcessors();
  }

  private initializeProcessors(): void {
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
    
    const allTables = Object.keys(initJson);
    const registeredTables = Array.from(this.processors.keys());
    
    for (const tableName of allTables) {
      if (!registeredTables.includes(tableName)) {
        this.processors.set(tableName, new GenericTableProcessor(tableName));
      }
    }
  }

  async insertAllDefaultRecords(): Promise<void> {
    this.logger.log('🚀 Starting default data upsert...');
    
    // MongoDB: Direct JSON insert (simpler, no processors needed)
    if (this.dbType === 'mongodb') {
      return this.insertAllDefaultRecordsMongo();
    }
    
    // SQL: Use processors for complex relations
    const qb = this.queryBuilder.getConnection();
    let totalCreated = 0;
    let totalSkipped = 0;

    for (const [tableName, rawRecords] of Object.entries(initJson)) {
      const processor = this.processors.get(tableName);
      if (!processor) {
        this.logger.warn(`⚠️ No processor found for '${tableName}', skipping.`);
        continue;
      }

      if (!rawRecords || (Array.isArray(rawRecords) && rawRecords.length === 0)) {
        this.logger.debug(`❎ Table '${tableName}' has no data, skipping.`);
        continue;
      }

      this.logger.log(`🔄 Processing '${tableName}'...`);

      try {
        const records = Array.isArray(rawRecords) ? rawRecords : [rawRecords];

        const dbType = this.queryBuilder.getDatabaseType();
        const context = { knex: qb, tableName, dbType };

        const result = await processor.processKnex(records, qb, tableName, context);
        
        totalCreated += result.created;
        totalSkipped += result.skipped;
        
        this.logger.log(
          `✅ '${tableName}': ${result.created} created, ${result.skipped} skipped`
        );
      } catch (error) {
        this.logger.error(`❌ Error processing '${tableName}': ${error.message}`);
        this.logger.debug(`Error:`, error);
      }
    }

    this.logger.log(
      `🎉 Default data upsert completed! Total: ${totalCreated} created, ${totalSkipped} skipped`
    );
  }

  private async insertAllDefaultRecordsMongo(): Promise<void> {
    this.logger.log('🍃 MongoDB: Inserting default data with processors...');

    let totalCreated = 0;

    // Get raw MongoDB connection (no metadata needed during bootstrap)
    const db = this.queryBuilder.getConnection();

    for (const [collectionName, rawRecords] of Object.entries(initJson)) {
      if (!rawRecords || (Array.isArray(rawRecords) && rawRecords.length === 0)) {
        continue;
      }

      this.logger.log(`🔄 Processing collection '${collectionName}'...`);

      try {
        // Check if collection already has data (raw query)
        const count = await db.collection(collectionName).countDocuments();
        if (count > 0) {
          this.logger.log(`⏩ '${collectionName}' already has ${count} records, skipping`);
          continue;
        }

        // Get processor (same as SQL flow)
        const processor = this.processors.get(collectionName);
        if (!processor) {
          this.logger.warn(`⚠️ No processor found for '${collectionName}', skipping.`);
          continue;
        }

        const records = Array.isArray(rawRecords) ? rawRecords : [rawRecords];

        // Special handling for menu_definition (needs ordered processing with raw DB)
        if (collectionName === 'menu_definition' && typeof processor['processMongo'] === 'function') {
          const created = await processor['processMongo'](records, collectionName, db);
          totalCreated += created;
          this.logger.log(`✅ '${collectionName}': ${created} created`);
          continue;
        }

        // Transform records using processor (with raw DB context)
        const context = { db, collectionName };
        const transformedRecords = await processor.transformRecords(records, context);
        const validRecords = transformedRecords.filter(r => r !== null);

        if (validRecords.length === 0) {
          this.logger.log(`⏩ '${collectionName}': No valid records after transformation`);
          continue;
        }

        // Insert transformed records (raw MongoDB insert)
        const now = new Date();
        const recordsWithTimestamps = validRecords.map(r => ({
          ...r,
          createdAt: r.createdAt || now,
          updatedAt: r.updatedAt || now,
        }));

        await db.collection(collectionName).insertMany(recordsWithTimestamps);
        totalCreated += validRecords.length;

        this.logger.log(`✅ '${collectionName}': ${validRecords.length} created`);
      } catch (error) {
        this.logger.error(`❌ Error processing '${collectionName}': ${error.message}`);
      }
    }

    this.logger.log(`🎉 MongoDB default data completed! Total: ${totalCreated} created`);
  }

  private async insertAndGetId(
    trx: any,
    tableName: string,
    data: any,
  ): Promise<number> {
    if (this.dbType === 'postgres') {
      const [result] = await trx(tableName).insert(data).returning('id');
      return result.id;
    } else {
      const [id] = await trx(tableName).insert(data);
      return id;
    }
  }
}