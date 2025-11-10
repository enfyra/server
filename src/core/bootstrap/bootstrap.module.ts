import { Global, Module } from '@nestjs/common';
import { BootstrapService } from './services/bootstrap.service';
import { CoreInitService } from './services/core-init.service';
import { CoreInitSqlService } from './services/core-init-sql.service';
import { CoreInitMongoService } from './services/core-init-mongo.service';
import { DefaultDataService } from './services/default-data.service';
import { BootstrapScriptService } from './services/bootstrap-script.service';

import { UserDefinitionProcessor } from './processors/user-definition.processor';
import { MenuDefinitionProcessor } from './processors/menu-definition.processor';
import { RouteDefinitionProcessor } from './processors/route-definition.processor';
import { RouteHandlerDefinitionProcessor } from './processors/route-handler-definition.processor';
import { MethodDefinitionProcessor } from './processors/method-definition.processor';
import { HookDefinitionProcessor } from './processors/hook-definition.processor';
import { SettingDefinitionProcessor } from './processors/setting-definition.processor';
import { ExtensionDefinitionProcessor } from './processors/extension-definition.processor';
import { FolderDefinitionProcessor } from './processors/folder-definition.processor';
import { BootstrapScriptDefinitionProcessor } from './processors/bootstrap-script-definition.processor';
import { RoutePermissionDefinitionProcessor } from './processors/route-permission-definition.processor';
import { AiConfigDefinitionProcessor } from './processors/ai-config-definition.processor';

@Global()
@Module({
  providers: [
    BootstrapService,
    CoreInitService,
    CoreInitSqlService,
    CoreInitMongoService,
    DefaultDataService,
    UserDefinitionProcessor,
    MenuDefinitionProcessor,
    RouteDefinitionProcessor,
    RouteHandlerDefinitionProcessor,
    MethodDefinitionProcessor,
    HookDefinitionProcessor,
    SettingDefinitionProcessor,
    ExtensionDefinitionProcessor,
    FolderDefinitionProcessor,
    BootstrapScriptDefinitionProcessor,
    RoutePermissionDefinitionProcessor,
    AiConfigDefinitionProcessor,
    BootstrapScriptService,
  ],
  exports: [BootstrapService, CoreInitService, DefaultDataService, BootstrapScriptService],
})
export class BootstrapModule {}
