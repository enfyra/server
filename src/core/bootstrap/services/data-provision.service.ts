import { Injectable, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { BcryptService } from '../../auth/services/bcrypt.service';
import * as fs from 'fs';
import * as path from 'path';

import {
  BaseTableProcessor,
  UpsertResult,
} from '../processors/base-table-processor';
import { UserDefinitionProcessor } from '../processors/user-definition.processor';
import { MenuDefinitionProcessor } from '../processors/menu-definition.processor';
import { RouteDefinitionProcessor } from '../processors/route-definition.processor';
import { RouteHandlerDefinitionProcessor } from '../processors/route-handler-definition.processor';
import { MethodDefinitionProcessor } from '../processors/method-definition.processor';
import { PreHookDefinitionProcessor } from '../processors/pre-hook-definition.processor';
import { PostHookDefinitionProcessor } from '../processors/post-hook-definition.processor';
import { SettingDefinitionProcessor } from '../processors/setting-definition.processor';
import { ExtensionDefinitionProcessor } from '../processors/extension-definition.processor';
import { FolderDefinitionProcessor } from '../processors/folder-definition.processor';
import { BootstrapScriptDefinitionProcessor } from '../processors/bootstrap-script-definition.processor';
import { RoutePermissionDefinitionProcessor } from '../processors/route-permission-definition.processor';
import { WebsocketDefinitionProcessor } from '../processors/websocket-definition.processor';
import { WebsocketEventDefinitionProcessor } from '../processors/websocket-event-definition.processor';
import { FlowDefinitionProcessor } from '../processors/flow-definition.processor';
import { FlowStepDefinitionProcessor } from '../processors/flow-step-definition.processor';
import { FlowExecutionDefinitionProcessor } from '../processors/flow-execution-definition.processor';
import { GenericTableProcessor } from '../processors/generic-table.processor';

const initJson = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), 'data/default-data.json'), 'utf8'),
);

@Injectable()
export class DataProvisionService {
  private readonly logger = new Logger(DataProvisionService.name);
  private readonly processors = new Map<string, BaseTableProcessor>();
  private readonly dbType: string;

  constructor(
    private readonly queryBuilder: QueryBuilderService,
    private readonly databaseConfig: DatabaseConfigService,
    private readonly bcryptService: BcryptService,
    private readonly userProcessor: UserDefinitionProcessor,
    private readonly menuProcessor: MenuDefinitionProcessor,
    private readonly routeProcessor: RouteDefinitionProcessor,
    private readonly routeHandlerProcessor: RouteHandlerDefinitionProcessor,
    private readonly methodProcessor: MethodDefinitionProcessor,
    private readonly preHookProcessor: PreHookDefinitionProcessor,
    private readonly postHookProcessor: PostHookDefinitionProcessor,
    private readonly settingProcessor: SettingDefinitionProcessor,
    private readonly extensionProcessor: ExtensionDefinitionProcessor,
    private readonly folderProcessor: FolderDefinitionProcessor,
    private readonly bootstrapScriptProcessor: BootstrapScriptDefinitionProcessor,
    private readonly routePermissionProcessor: RoutePermissionDefinitionProcessor,
    private readonly websocketDefinitionProcessor: WebsocketDefinitionProcessor,
    private readonly websocketEventDefinitionProcessor: WebsocketEventDefinitionProcessor,
    private readonly flowDefinitionProcessor: FlowDefinitionProcessor,
    private readonly flowStepDefinitionProcessor: FlowStepDefinitionProcessor,
    private readonly flowExecutionDefinitionProcessor: FlowExecutionDefinitionProcessor,
  ) {
    this.dbType = this.databaseConfig.getDbType();
    this.initializeProcessors();
  }

  private initializeProcessors(): void {
    this.processors.set('user_definition', this.userProcessor);
    this.processors.set('menu_definition', this.menuProcessor);
    this.processors.set('route_definition', this.routeProcessor);
    this.processors.set('route_handler_definition', this.routeHandlerProcessor);
    this.processors.set('method_definition', this.methodProcessor);
    this.processors.set('pre_hook_definition', this.preHookProcessor);
    this.processors.set('post_hook_definition', this.postHookProcessor);
    this.processors.set('setting_definition', this.settingProcessor);
    this.processors.set('extension_definition', this.extensionProcessor);
    this.processors.set('folder_definition', this.folderProcessor);
    this.processors.set(
      'bootstrap_script_definition',
      this.bootstrapScriptProcessor,
    );
    this.processors.set(
      'route_permission_definition',
      this.routePermissionProcessor,
    );
    this.processors.set(
      'websocket_definition',
      this.websocketDefinitionProcessor,
    );
    this.processors.set(
      'websocket_event_definition',
      this.websocketEventDefinitionProcessor,
    );
    this.processors.set('flow_definition', this.flowDefinitionProcessor);
    this.processors.set(
      'flow_step_definition',
      this.flowStepDefinitionProcessor,
    );
    this.processors.set(
      'flow_execution_definition',
      this.flowExecutionDefinitionProcessor,
    );

    const allTables = Object.keys(initJson);
    const registeredTables = Array.from(this.processors.keys());

    for (const tableName of allTables) {
      if (!registeredTables.includes(tableName)) {
        this.processors.set(tableName, new GenericTableProcessor(tableName));
      }
    }
  }

  async insertAllDefaultRecords(): Promise<void> {
    this.logger.log('Starting default data upsert...');

    let totalCreated = 0;
    let totalSkipped = 0;

    const userProcessor = this.processors.get('user_definition');
    if (userProcessor) {
      try {
        this.logger.log(
          `Processing 'user_definition' (ensure rootAdmin from env)...`,
        );
        const result = await userProcessor.processWithQueryBuilder(
          [],
          this.queryBuilder,
          'user_definition',
          {},
        );
        totalCreated += result.created;
        totalSkipped += result.skipped;
      } catch (error) {
        this.logger.error(
          `Error processing 'user_definition': ${error.message}`,
        );
      }
    }

    for (const [tableName, rawRecords] of Object.entries(initJson)) {
      const processor = this.processors.get(tableName);
      if (!processor) {
        this.logger.warn(`No processor found for '${tableName}', skipping.`);
        continue;
      }

      if (
        !rawRecords ||
        (Array.isArray(rawRecords) && rawRecords.length === 0)
      ) {
        this.logger.debug(`Table '${tableName}' has no data, skipping.`);
        continue;
      }

      this.logger.log(`Processing '${tableName}'...`);

      try {
        const records = Array.isArray(rawRecords) ? rawRecords : [rawRecords];

        const result = await processor.processWithQueryBuilder(
          records,
          this.queryBuilder,
          tableName,
          {},
        );

        totalCreated += result.created;
        totalSkipped += result.skipped;

        this.logger.log(
          `'${tableName}': ${result.created} created, ${result.skipped} skipped`,
        );
      } catch (error) {
        this.logger.error(`Error processing '${tableName}': ${error.message}`);
        this.logger.debug(`Error:`, error);
      }
    }

    this.logger.log(
      `Default data upsert completed! Total: ${totalCreated} created, ${totalSkipped} skipped`,
    );
  }

  async insertTableRecords(tableName: string): Promise<UpsertResult> {
    const rawRecords = initJson[tableName];
    if (!rawRecords) {
      this.logger.warn(`No data found in default-data.json for '${tableName}'`);
      return { created: 0, skipped: 0 };
    }

    const processor = this.processors.get(tableName);
    if (!processor) {
      this.logger.warn(`No processor found for '${tableName}'`);
      return { created: 0, skipped: 0 };
    }

    const records = Array.isArray(rawRecords) ? rawRecords : [rawRecords];

    return await processor.processWithQueryBuilder(
      records,
      this.queryBuilder,
      tableName,
      {},
    );
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
