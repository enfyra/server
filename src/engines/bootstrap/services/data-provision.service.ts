import { Logger } from '../../../shared/logger';
import { QueryBuilderService } from '@enfyra/kernel';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { DatabaseConfigService } from '../../../shared/services';
import { BcryptService } from '../../../domain/auth';
import * as fs from 'fs';
import * as path from 'path';

import {
  BaseTableProcessor,
  UpsertResult,
  UserDefinitionProcessor,
  MenuDefinitionProcessor,
  RouteDefinitionProcessor,
  RouteHandlerDefinitionProcessor,
  MethodDefinitionProcessor,
  PreHookDefinitionProcessor,
  PostHookDefinitionProcessor,
  FieldPermissionDefinitionProcessor,
  SettingDefinitionProcessor,
  ExtensionDefinitionProcessor,
  FolderDefinitionProcessor,
  BootstrapScriptDefinitionProcessor,
  RoutePermissionDefinitionProcessor,
  WebsocketDefinitionProcessor,
  WebsocketEventDefinitionProcessor,
  FlowDefinitionProcessor,
  FlowStepDefinitionProcessor,
  FlowExecutionDefinitionProcessor,
  GenericTableProcessor,
  GraphQLDefinitionProcessor,
} from '../../../domain/bootstrap';
import { bootstrapVerboseLog } from '../utils/bootstrap-logging.util';
import { SYSTEM_TABLES } from '../../../shared/utils/system-tables.constants';
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
    this.processors.set(SYSTEM_TABLES.user, this.userDefinitionProcessor);
    this.processors.set(SYSTEM_TABLES.menu, this.menuDefinitionProcessor);
    this.processors.set(SYSTEM_TABLES.route, this.routeDefinitionProcessor);
    this.processors.set(
      SYSTEM_TABLES.routeHandler,
      this.routeHandlerDefinitionProcessor,
    );
    this.processors.set(SYSTEM_TABLES.method, this.methodDefinitionProcessor);
    this.processors.set(SYSTEM_TABLES.preHook, this.preHookDefinitionProcessor);
    this.processors.set(
      SYSTEM_TABLES.postHook,
      this.postHookDefinitionProcessor,
    );
    this.processors.set(
      SYSTEM_TABLES.fieldPermission,
      this.fieldPermissionDefinitionProcessor,
    );
    this.processors.set(SYSTEM_TABLES.setting, this.settingDefinitionProcessor);
    this.processors.set(
      SYSTEM_TABLES.extension,
      this.extensionDefinitionProcessor,
    );
    this.processors.set(SYSTEM_TABLES.folder, this.folderDefinitionProcessor);
    this.processors.set(
      SYSTEM_TABLES.bootstrapScript,
      this.bootstrapScriptDefinitionProcessor,
    );
    this.processors.set(
      SYSTEM_TABLES.routePermission,
      this.routePermissionDefinitionProcessor,
    );
    this.processors.set(
      SYSTEM_TABLES.websocket,
      this.websocketDefinitionProcessor,
    );
    this.processors.set(
      SYSTEM_TABLES.websocketEvent,
      this.websocketEventDefinitionProcessor,
    );
    this.processors.set(SYSTEM_TABLES.flow, this.flowDefinitionProcessor);
    this.processors.set(
      SYSTEM_TABLES.flowStep,
      this.flowStepDefinitionProcessor,
    );
    this.processors.set(
      SYSTEM_TABLES.flowExecution,
      this.flowExecutionDefinitionProcessor,
    );
    this.processors.set(SYSTEM_TABLES.graphql, this.graphqlDefinitionProcessor);

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
    this.verbose('Starting default data upsert...');

    let totalCreated = 0;
    let totalSkipped = 0;

    const userProcessor = this.processors.get(SYSTEM_TABLES.user);
    if (userProcessor) {
      try {
        this.verbose(
          `Processing '${SYSTEM_TABLES.user}' (ensure rootAdmin from env)...`,
        );
        const result = await userProcessor.processWithQueryBuilder(
          [],
          this.queryBuilderService,
          SYSTEM_TABLES.user,
          {},
        );
        totalCreated += result.created;
        totalSkipped += result.skipped;
      } catch (error) {
        this.logger.error(
          `Error processing '${SYSTEM_TABLES.user}': ${getErrorMessage(error)}`,
        );
        throw error;
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
        this.verbose(`Table '${tableName}' has no data, skipping.`);
        continue;
      }

      this.verbose(`Processing '${tableName}'...`);

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

        this.verbose(
          `'${tableName}': ${result.created} created, ${result.skipped} skipped`,
        );
      } catch (error) {
        this.logger.error(
          `Error processing '${tableName}': ${getErrorMessage(error)}`,
        );
        this.logger.debug(`Error: ${getErrorMessage(error)}`);
        throw error;
      }
    }

    this.verbose(
      `Default data upsert completed! Total: ${totalCreated} created, ${totalSkipped} skipped`,
    );

    if (this.routeDefinitionProcessor) {
      this.verbose('Ensuring missing route handlers...');
      try {
        await this.routeDefinitionProcessor.ensureMissingHandlers();
      } catch (error) {
        this.logger.error(
          `Error ensuring route handlers: ${getErrorMessage(error)}`,
        );
        this.logger.debug(getErrorMessage(error));
        throw error;
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

  private verbose(message: string): void {
    bootstrapVerboseLog(this.logger, message);
  }
}
