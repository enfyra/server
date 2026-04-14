import { Global, Module } from '@nestjs/common';
import { ProvisionService } from './services/provision.service';
import { MetadataProvisionService } from './services/metadata-provision.service';
import { MetadataProvisionSqlService } from './services/metadata-provision-sql.service';
import { MetadataProvisionMongoService } from './services/metadata-provision-mongo.service';
import { DataProvisionService } from './services/data-provision.service';
import { BootstrapScriptService } from './services/bootstrap-script.service';
import { DataMigrationService } from './services/data-migration.service';
import { MetadataMigrationService } from './services/metadata-migration.service';

import { UserDefinitionProcessor } from './processors/user-definition.processor';
import { MenuDefinitionProcessor } from './processors/menu-definition.processor';
import { RouteDefinitionProcessor } from './processors/route-definition.processor';
import { RouteHandlerDefinitionProcessor } from './processors/route-handler-definition.processor';
import { MethodDefinitionProcessor } from './processors/method-definition.processor';
import { PreHookDefinitionProcessor } from './processors/pre-hook-definition.processor';
import { PostHookDefinitionProcessor } from './processors/post-hook-definition.processor';
import { SettingDefinitionProcessor } from './processors/setting-definition.processor';
import { ExtensionDefinitionProcessor } from './processors/extension-definition.processor';
import { FolderDefinitionProcessor } from './processors/folder-definition.processor';
import { BootstrapScriptDefinitionProcessor } from './processors/bootstrap-script-definition.processor';
import { RoutePermissionDefinitionProcessor } from './processors/route-permission-definition.processor';
import { WebsocketDefinitionProcessor } from './processors/websocket-definition.processor';
import { WebsocketEventDefinitionProcessor } from './processors/websocket-event-definition.processor';
import { FlowDefinitionProcessor } from './processors/flow-definition.processor';
import { FlowStepDefinitionProcessor } from './processors/flow-step-definition.processor';
import { FlowExecutionDefinitionProcessor } from './processors/flow-execution-definition.processor';
import { GraphQLDefinitionProcessor } from './processors/graphql-definition.processor';

@Global()
@Module({
  providers: [
    ProvisionService,
    MetadataProvisionService,
    MetadataProvisionSqlService,
    MetadataProvisionMongoService,
    DataProvisionService,
    DataMigrationService,
    MetadataMigrationService,
    UserDefinitionProcessor,
    MenuDefinitionProcessor,
    RouteDefinitionProcessor,
    RouteHandlerDefinitionProcessor,
    MethodDefinitionProcessor,
    PreHookDefinitionProcessor,
    PostHookDefinitionProcessor,
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
    GraphQLDefinitionProcessor,
    BootstrapScriptService,
  ],
  exports: [
    ProvisionService,
    MetadataProvisionService,
    DataProvisionService,
    BootstrapScriptService,
    DataMigrationService,
    MetadataMigrationService,
  ],
})
export class BootstrapProvisionModule {}
