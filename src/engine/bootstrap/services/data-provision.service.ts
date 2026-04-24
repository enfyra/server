import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '../../../engine/query-builder/query-builder.service';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { DatabaseConfigService } from '../../../shared/services/database-config.service';
import { BcryptService } from '../../../domain/auth/services/bcrypt.service';
import * as fs from 'fs';
import * as path from 'path';

import {
  BaseTableProcessor,
  UpsertResult,
} from '../../../domain/bootstrap/processors/base-table-processor';
import { UserDefinitionProcessor } from '../../../domain/bootstrap/processors/user-definition.processor';
import { MenuDefinitionProcessor } from '../../../domain/bootstrap/processors/menu-definition.processor';
import { RouteDefinitionProcessor } from '../../../domain/bootstrap/processors/route-definition.processor';
import { RouteHandlerDefinitionProcessor } from '../../../domain/bootstrap/processors/route-handler-definition.processor';
import { MethodDefinitionProcessor } from '../../../domain/bootstrap/processors/method-definition.processor';
import { PreHookDefinitionProcessor } from '../../../domain/bootstrap/processors/pre-hook-definition.processor';
import { PostHookDefinitionProcessor } from '../../../domain/bootstrap/processors/post-hook-definition.processor';
import { FieldPermissionDefinitionProcessor } from '../../../domain/bootstrap/processors/field-permission-definition.processor';
import { SettingDefinitionProcessor } from '../../../domain/bootstrap/processors/setting-definition.processor';
import { ExtensionDefinitionProcessor } from '../../../domain/bootstrap/processors/extension-definition.processor';
import { FolderDefinitionProcessor } from '../../../domain/bootstrap/processors/folder-definition.processor';
import { BootstrapScriptDefinitionProcessor } from '../../../domain/bootstrap/processors/bootstrap-script-definition.processor';
import { RoutePermissionDefinitionProcessor } from '../../../domain/bootstrap/processors/route-permission-definition.processor';
import { WebsocketDefinitionProcessor } from '../../../domain/bootstrap/processors/websocket-definition.processor';
import { WebsocketEventDefinitionProcessor } from '../../../domain/bootstrap/processors/websocket-event-definition.processor';
import { FlowDefinitionProcessor } from '../../../domain/bootstrap/processors/flow-definition.processor';
import { FlowStepDefinitionProcessor } from '../../../domain/bootstrap/processors/flow-step-definition.processor';
import { FlowExecutionDefinitionProcessor } from '../../../domain/bootstrap/processors/flow-execution-definition.processor';
import { GenericTableProcessor } from '../../../domain/bootstrap/processors/generic-table.processor';
import { GraphQLDefinitionProcessor } from '../../../domain/bootstrap/processors/graphql-definition.processor';

const initJson = JSON.parse(
  fs.readFileSync(path.join(process.cwd(), 'data/default-data.json'), 'utf8'),
);

export class DataProvisionService {
  private readonly logger = new Logger(DataProvisionService.name);
  private readonly processors = new Map<string, BaseTableProcessor>();
  private readonly queryBuilderService: QueryBuilderService;
  private readonly databaseConfigService: DatabaseConfigService;
  private readonly bcryptService: BcryptService;
  private readonly userDefinitionProcessor: UserDefinitionProcessor;
  private readonly menuDefinitionProcessor: MenuDefinitionProcessor;
  private readonly routeDefinitionProcessor: RouteDefinitionProcessor;
  private readonly routeHandlerDefinitionProcessor: RouteHandlerDefinitionProcessor;
  private readonly methodDefinitionProcessor: MethodDefinitionProcessor;
  private readonly preHookDefinitionProcessor: PreHookDefinitionProcessor;
  private readonly postHookDefinitionProcessor: PostHookDefinitionProcessor;
  private readonly fieldPermissionDefinitionProcessor: FieldPermissionDefinitionProcessor;
  private readonly settingDefinitionProcessor: SettingDefinitionProcessor;
  private readonly extensionDefinitionProcessor: ExtensionDefinitionProcessor;
  private readonly folderDefinitionProcessor: FolderDefinitionProcessor;
  private readonly bootstrapScriptDefinitionProcessor: BootstrapScriptDefinitionProcessor;
  private readonly routePermissionDefinitionProcessor: RoutePermissionDefinitionProcessor;
  private readonly websocketDefinitionProcessor: WebsocketDefinitionProcessor;
  private readonly websocketEventDefinitionProcessor: WebsocketEventDefinitionProcessor;
  private readonly flowDefinitionProcessor: FlowDefinitionProcessor;
  private readonly flowStepDefinitionProcessor: FlowStepDefinitionProcessor;
  private readonly flowExecutionDefinitionProcessor: FlowExecutionDefinitionProcessor;
  private readonly graphqlDefinitionProcessor: GraphQLDefinitionProcessor;
  private readonly dbType: string;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    databaseConfigService: DatabaseConfigService;
    bcryptService: BcryptService;
    userDefinitionProcessor: UserDefinitionProcessor;
    menuDefinitionProcessor: MenuDefinitionProcessor;
    routeDefinitionProcessor: RouteDefinitionProcessor;
    routeHandlerDefinitionProcessor: RouteHandlerDefinitionProcessor;
    methodDefinitionProcessor: MethodDefinitionProcessor;
    preHookDefinitionProcessor: PreHookDefinitionProcessor;
    postHookDefinitionProcessor: PostHookDefinitionProcessor;
    fieldPermissionDefinitionProcessor: FieldPermissionDefinitionProcessor;
    settingDefinitionProcessor: SettingDefinitionProcessor;
    extensionDefinitionProcessor: ExtensionDefinitionProcessor;
    folderDefinitionProcessor: FolderDefinitionProcessor;
    bootstrapScriptDefinitionProcessor: BootstrapScriptDefinitionProcessor;
    routePermissionDefinitionProcessor: RoutePermissionDefinitionProcessor;
    websocketDefinitionProcessor: WebsocketDefinitionProcessor;
    websocketEventDefinitionProcessor: WebsocketEventDefinitionProcessor;
    flowDefinitionProcessor: FlowDefinitionProcessor;
    flowStepDefinitionProcessor: FlowStepDefinitionProcessor;
    flowExecutionDefinitionProcessor: FlowExecutionDefinitionProcessor;
    graphqlDefinitionProcessor: GraphQLDefinitionProcessor;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.databaseConfigService = deps.databaseConfigService;
    this.bcryptService = deps.bcryptService;
    this.userDefinitionProcessor = deps.userDefinitionProcessor;
    this.menuDefinitionProcessor = deps.menuDefinitionProcessor;
    this.routeDefinitionProcessor = deps.routeDefinitionProcessor;
    this.routeHandlerDefinitionProcessor = deps.routeHandlerDefinitionProcessor;
    this.methodDefinitionProcessor = deps.methodDefinitionProcessor;
    this.preHookDefinitionProcessor = deps.preHookDefinitionProcessor;
    this.postHookDefinitionProcessor = deps.postHookDefinitionProcessor;
    this.fieldPermissionDefinitionProcessor =
      deps.fieldPermissionDefinitionProcessor;
    this.settingDefinitionProcessor = deps.settingDefinitionProcessor;
    this.extensionDefinitionProcessor = deps.extensionDefinitionProcessor;
    this.folderDefinitionProcessor = deps.folderDefinitionProcessor;
    this.bootstrapScriptDefinitionProcessor =
      deps.bootstrapScriptDefinitionProcessor;
    this.routePermissionDefinitionProcessor =
      deps.routePermissionDefinitionProcessor;
    this.websocketDefinitionProcessor = deps.websocketDefinitionProcessor;
    this.websocketEventDefinitionProcessor =
      deps.websocketEventDefinitionProcessor;
    this.flowDefinitionProcessor = deps.flowDefinitionProcessor;
    this.flowStepDefinitionProcessor = deps.flowStepDefinitionProcessor;
    this.flowExecutionDefinitionProcessor =
      deps.flowExecutionDefinitionProcessor;
    this.graphqlDefinitionProcessor = deps.graphqlDefinitionProcessor;
    this.dbType = this.databaseConfigService.getDbType();
    this.initializeProcessors();
  }

  private initializeProcessors(): void {
    this.processors.set('user_definition', this.userDefinitionProcessor);
    this.processors.set('menu_definition', this.menuDefinitionProcessor);
    this.processors.set('route_definition', this.routeDefinitionProcessor);
    this.processors.set(
      'route_handler_definition',
      this.routeHandlerDefinitionProcessor,
    );
    this.processors.set('method_definition', this.methodDefinitionProcessor);
    this.processors.set('pre_hook_definition', this.preHookDefinitionProcessor);
    this.processors.set(
      'post_hook_definition',
      this.postHookDefinitionProcessor,
    );
    this.processors.set(
      'field_permission_definition',
      this.fieldPermissionDefinitionProcessor,
    );
    this.processors.set('setting_definition', this.settingDefinitionProcessor);
    this.processors.set(
      'extension_definition',
      this.extensionDefinitionProcessor,
    );
    this.processors.set('folder_definition', this.folderDefinitionProcessor);
    this.processors.set(
      'bootstrap_script_definition',
      this.bootstrapScriptDefinitionProcessor,
    );
    this.processors.set(
      'route_permission_definition',
      this.routePermissionDefinitionProcessor,
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
    this.processors.set('gql_definition', this.graphqlDefinitionProcessor);

    const allTables = Object.keys(initJson);
    const registeredTables = Array.from(this.processors.keys());

    for (const tableName of allTables) {
      if (!registeredTables.includes(tableName)) {
        this.processors.set(
          tableName,
          new GenericTableProcessor({ tableName }),
        );
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
          this.queryBuilderService,
          'user_definition',
          {},
        );
        totalCreated += result.created;
        totalSkipped += result.skipped;
      } catch (error) {
        this.logger.error(
          `Error processing 'user_definition': ${getErrorMessage(error)}`,
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
          this.queryBuilderService,
          tableName,
          {},
        );

        totalCreated += result.created;
        totalSkipped += result.skipped;

        this.logger.log(
          `'${tableName}': ${result.created} created, ${result.skipped} skipped`,
        );
      } catch (error) {
        this.logger.error(`Error processing '${tableName}': ${getErrorMessage(error)}`);
        this.logger.debug(`Error: ${getErrorMessage(error)}`);
      }
    }

    this.logger.log(
      `Default data upsert completed! Total: ${totalCreated} created, ${totalSkipped} skipped`,
    );

    if (this.routeDefinitionProcessor) {
      this.logger.log('Ensuring missing route handlers...');
      try {
        await this.routeDefinitionProcessor.ensureMissingHandlers();
      } catch (error) {
        this.logger.error(`Error ensuring route handlers: ${getErrorMessage(error)}`);
        this.logger.debug(getErrorMessage(error));
      }
    }
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
      this.queryBuilderService,
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
