import type { Express } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { registerAuthRoutes } from './auth.routes';
import { registerOAuthRoutes } from './oauth.routes';
import { registerAdminRoutes } from './admin.routes';
import { registerLogRoutes } from './log.routes';
import { registerMetadataRoutes } from './metadata.routes';
import { registerExtensionRoutes } from './extension.routes';
import { registerAssetsRoutes } from './assets.routes';
import { registerFileRoutes } from './file.routes';
import { registerFolderRoutes } from './folder.routes';
import { registerGraphqlSchemaRoutes } from './graphql-schema.routes';
import { registerMeRoutes } from './me.routes';
import { registerPackageRoutes } from './package.routes';
import { registerDynamicRoutes } from './dynamic.routes';

export function registerRoutes(app: Express, container: AwilixContainer<Cradle>) {
  registerAuthRoutes(app, container);
  registerOAuthRoutes(app, container);
  registerAdminRoutes(app, container);
  registerLogRoutes(app, container);
  registerMetadataRoutes(app, container);
  registerExtensionRoutes(app, container);
  registerAssetsRoutes(app, container);
  registerFileRoutes(app, container);
  registerFolderRoutes(app, container);
  registerGraphqlSchemaRoutes(app, container);
  registerMeRoutes(app, container);
  registerPackageRoutes(app, container);
  registerDynamicRoutes(app, container);
}
