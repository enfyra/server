import { Injectable, NestMiddleware, Logger } from '@nestjs/common';
import { QueryBuilderService } from '../query-builder/query-builder.service';
import { JwtService } from '@nestjs/jwt';
import { TableHandlerService } from '../../modules/table-management/services/table-handler.service';
import { DynamicRepository } from '../../modules/dynamic-api/repositories/dynamic.repository';
import { TDynamicContext } from '../../shared/interfaces/dynamic-context.interface';
import { QueryEngine } from '../query-engine/services/query-engine.service';
import { RouteCacheService } from '../cache/services/route-cache.service';
import { StorageConfigCacheService } from '../cache/services/storage-config-cache.service';
import { AiConfigCacheService } from '../cache/services/ai-config-cache.service';
import { MetadataCacheService } from '../cache/services/metadata-cache.service';
import { SystemProtectionService } from '../../modules/dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../modules/dynamic-api/services/table-validation.service';
import { BcryptService } from '../../core/auth/services/bcrypt.service';
import { ScriptErrorFactory } from '../../shared/utils/script-error-factory';
import { autoSlug } from '../../shared/utils/auto-slug.helper';
import { CacheService } from '../cache/services/cache.service';
import { SwaggerService } from '../swagger/services/swagger.service';
import { GraphqlService } from 'src/modules/graphql/services/graphql.service';
import { UploadFileHelper } from '../../shared/helpers/upload-file.helper';

@Injectable()
export class RouteDetectMiddleware implements NestMiddleware {
  private readonly logger = new Logger(RouteDetectMiddleware.name);

  constructor(
    private queryBuilder: QueryBuilderService,
    private jwtService: JwtService,
    private queryEngine: QueryEngine,
    private tableHandlerService: TableHandlerService,
    private routeCacheService: RouteCacheService,
    private storageConfigCacheService: StorageConfigCacheService,
    private aiConfigCacheService: AiConfigCacheService,
    private metadataCacheService: MetadataCacheService,
    private systemProtectionService: SystemProtectionService,
    private tableValidationService: TableValidationService,
    private cacheService: CacheService,
    private bcryptService: BcryptService,
    private swaggerService: SwaggerService,
    private graphqlService: GraphqlService,
    private uploadFileHelper: UploadFileHelper,
  ) {}

  async use(req: any, res: any, next: (error?: any) => void) {
    const method = req.method;
    const routeEngine = this.routeCacheService.getRouteEngine();
    const matchedRoute = routeEngine.find(method, req.baseUrl);
    const systemTables = [
      'table_definition',
      'column_definition',
      'relation_definition',
    ];

      if (matchedRoute) {
        const realClientIP = this.detectClientIP(req);
        
      const context: TDynamicContext = {
        $body: req.routeData?.context?.$body || req.body || {},
        $data: undefined,
        $statusCode: undefined,
        $throw: ScriptErrorFactory.createThrowHandlers(),
        $logs(...args) {},
        $helpers: {
          $jwt: (payload: any, exp: string) =>
            this.jwtService.sign(payload, { expiresIn: exp }),
          $bcrypt: {
            hash: async (plain: string) => await this.bcryptService.hash(plain),
            compare: async (p: string, h: string) =>
              await this.bcryptService.compare(p, h),
          },
          autoSlug: autoSlug,
        },
        $cache: this.cacheService,
        $params: matchedRoute.params ?? {},
        $query: req.query ?? {},
        $user: req.user ?? null,
        $repos: {},
          $req: {
            ...req,
            ip: realClientIP,
          },
        $share: {
          $logs: [],
        },
        $api: {
          request: {
            method: req.method,
            url: req.url,
            timestamp: new Date().toISOString(),
            correlationId: req.headers['x-correlation-id'] as string || this.generateCorrelationId(),
            userAgent: req.headers['user-agent'],
            ip: realClientIP,
          },
        },
      };
      context.$logs = (...args: any[]) => {
        context.$share.$logs.push(...args);
      };

      if (req.file) {
        context.$uploadedFile = {
          originalname: req.file.originalname,
          mimetype: req.file.mimetype,
          encoding: req.file.encoding || 'utf8',
          buffer: req.file.buffer,
          size: req.file.size,
          fieldname: req.file.fieldname,
        };
      }
      const dynamicFindEntries = await Promise.all(
        [
          matchedRoute.route.mainTable,
          ...matchedRoute.route.targetTables?.filter(
            (route) => !systemTables.includes(route?.name),
          ),
        ]?.filter(table => table?.name)?.map(async (table) => {
          const dynamicRepo = new DynamicRepository({
            context: null,
            tableName: table.name,
            tableHandlerService: this.tableHandlerService,
            queryBuilder: this.queryBuilder,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            aiConfigCacheService: this.aiConfigCacheService,
            metadataCacheService: this.metadataCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
            bootstrapScriptService: undefined,
            swaggerService: this.swaggerService,
            graphqlService: this.graphqlService,
          });

          await dynamicRepo.init();
          const name = table?.alias ?? table?.name;

          return [`${name}`, dynamicRepo];
        }),
      );

      context.$repos = Object.fromEntries(dynamicFindEntries);

      Object.values(context.$repos).forEach((repo: any) => {
        repo.context = context;
      });

      const mainTableName =
        matchedRoute.route.mainTable?.alias ?? matchedRoute.route?.mainTable?.name;
      if (context.$repos[mainTableName]) {
        context.$repos.main = context.$repos[mainTableName];
      }

      try {
        context.$helpers.$uploadFile = this.uploadFileHelper.createUploadFileHelper(context);
        context.$helpers.$updateFile = this.uploadFileHelper.createUpdateFileHelper(context);
        context.$helpers.$deleteFile = this.uploadFileHelper.createDeleteFileHelper(context);
      } catch (error) {
        this.logger.warn('Failed to initialize file helpers:', error);
      }
      
      const { route, params } = matchedRoute;

      const filteredHooks = route.hooks.filter((hook: any) => {
        const methodList = hook.methods?.map((m: any) => m.method) ?? [];

        const isGlobalAll = !hook.route && methodList.length === 0;
        const isGlobalMethod = !hook.route && methodList.includes(method);
        const isLocalAll =
          hook.route?.id === route.id && methodList.length === 0;
        const isLocalMethod =
          hook.route?.id === route.id && methodList.includes(method);

        return isGlobalAll || isGlobalMethod || isLocalAll || isLocalMethod;
      });

      req.routeData = {
        ...route,
        handler:
          route.handlers.find((handler) => handler.method?.method === method)
            ?.logic ?? null,
        params,
        hooks: filteredHooks,
        isPublished:
          route.publishedMethods?.some(
            (pubMethod: any) => pubMethod.method === req.method,
          ) || false,
        context,
        res, // Store response object for handler access
      };
    }
    next();
  }

  private generateCorrelationId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private detectClientIP(req: any): string {
    const forwardedFor = req.headers['x-forwarded-for'];
    const realIP = req.headers['x-real-ip'];
    const cfConnectingIP = req.headers['cf-connecting-ip']; // Cloudflare
    const remoteAddress = req.connection?.remoteAddress || req.socket?.remoteAddress;
    const reqIP = req.ip;

    let clientIP: string;

    if (forwardedFor) {
      clientIP = forwardedFor.split(',')[0].trim();
    }
    else if (realIP) {
      clientIP = realIP;
    }
    else if (cfConnectingIP) {
      clientIP = cfConnectingIP;
    }
    else if (reqIP && reqIP !== '::1' && reqIP !== '127.0.0.1') {
      clientIP = reqIP;
    }
    else if (remoteAddress && remoteAddress !== '::1' && remoteAddress !== '127.0.0.1') {
      clientIP = remoteAddress;
    }
    else {
      clientIP = reqIP || remoteAddress || 'unknown';
    }

    if (clientIP === '::1') {
      clientIP = '127.0.0.1';
    }

    if (clientIP?.startsWith('::ffff:')) {
      clientIP = clientIP.substring(7);
    }
    return clientIP;
  }
}
