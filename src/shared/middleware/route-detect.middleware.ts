import { Injectable, NestMiddleware } from '@nestjs/common';
import { CommonService } from '../common/services/common.service';
import { DataSourceService } from '../../core/database/data-source/data-source.service';
import { JwtService } from '@nestjs/jwt';
import { TableHandlerService } from '../../modules/table-management/services/table-handler.service';
import { DynamicRepository } from '../../modules/dynamic-api/repositories/dynamic.repository';
import { TDynamicContext } from '../interfaces/dynamic-context.interface';
import { QueryEngine } from '../../infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../infrastructure/cache/services/route-cache.service';
import { SystemProtectionService } from '../../modules/dynamic-api/services/system-protection.service';
import { BcryptService } from '../../core/auth/services/bcrypt.service';
import { ScriptErrorFactory } from '../../shared/utils/script-error-factory';
import { autoSlug } from '../utils/auto-slug.helper';
import { CacheService } from '../../infrastructure/cache/services/cache.service';

@Injectable()
export class RouteDetectMiddleware implements NestMiddleware {
  constructor(
    private commonService: CommonService,
    private dataSourceService: DataSourceService,
    private jwtService: JwtService,
    private queryEngine: QueryEngine,
    private tableHandlerService: TableHandlerService,
    private routeCacheService: RouteCacheService,
    private systemProtectionService: SystemProtectionService,
    private cacheService: CacheService,
    private bcryptService: BcryptService,
  ) {}

  async use(req: any, res: any, next: (error?: any) => void) {
    const method = req.method;

    const routes: any[] = await this.routeCacheService.getRoutesWithSWR();

    const matchedRoute = this.findMatchedRoute(routes, req.baseUrl, method);
    const systemTables = [
      'table_definition',
      'column_definition',
      'relation_definition',
    ];

      if (matchedRoute) {
        // Detect real client IP first
        const realClientIP = this.detectClientIP(req);
        
        // Create context first
      const context: TDynamicContext = {
        $body: req.body,
        $data: undefined, // Will be set by interceptor
        $statusCode: undefined, // Will be set by interceptor
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
        $user: req.user ?? undefined,
        $repos: {}, // Will be populated after repos are created
          $req: {
            ...req,
            ip: realClientIP, // Override IP with detected client IP
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
            ip: realClientIP, // Use already detected IP
          },
        },
      };
      context.$logs = (...args: any[]) => {
        context.$share.$logs.push(...args);
      };

      // Create dynamic repositories without context first to avoid circular reference
      const dynamicFindEntries = await Promise.all(
        [
          matchedRoute.route.mainTable,
          ...matchedRoute.route.targetTables?.filter(
            (route) => !systemTables.includes(route?.name),
          ),
        ]?.filter(table => table?.name)?.map(async (table) => {
          const dynamicRepo = new DynamicRepository({
            context: null, // Will be set later to avoid circular reference
            tableName: table.name,
            tableHandlerService: this.tableHandlerService,
            dataSourceService: this.dataSourceService,
            queryEngine: this.queryEngine,
            routeCacheService: this.routeCacheService,
            systemProtectionService: this.systemProtectionService,
            bootstrapScriptService: undefined, // Not available in middleware context
          });

          await dynamicRepo.init();
          const name = table?.alias ?? table?.name;

          return [`${name}`, dynamicRepo];
        }),
      );

      // Create repos object and add main alias for mainTable
      context.$repos = Object.fromEntries(dynamicFindEntries);

      // Set context for each repo after repos object is created
      Object.values(context.$repos).forEach((repo: any) => {
        repo.context = context;
      });

      // Add 'main' alias for mainTable
      const mainTableName =
        matchedRoute.route.mainTable?.alias ?? matchedRoute.route?.mainTable?.name;
      if (context.$repos[mainTableName]) {
        context.$repos.main = context.$repos[mainTableName];
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
      };
    }
    next();
  }

  private findMatchedRoute(routes: any[], reqPath: string, method: string) {
    const matchers = ['DELETE', 'PATCH'].includes(method)
      ? [(r) => r.path + '/:id', (r) => r.path]
      : [(r) => r.path];

    for (const route of routes) {
      const paths = [route.path, ...matchers.map((fn) => fn(route))].map(
        (p) => '/' + p.replace(/^\/+/, ''),
      );

      for (const routePath of paths) {
        const matched = this.commonService.isRouteMatched({
          routePath,
          reqPath,
        });
        if (matched) return { route, params: matched.params };
      }
    }

    return null;
  }

  private generateCorrelationId(): string {
    return `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  private detectClientIP(req: any): string {
    // Priority order for IP detection
    const forwardedFor = req.headers['x-forwarded-for'];
    const realIP = req.headers['x-real-ip'];
    const cfConnectingIP = req.headers['cf-connecting-ip']; // Cloudflare
    const remoteAddress = req.connection?.remoteAddress || req.socket?.remoteAddress;
    const reqIP = req.ip;

    // Debug logging
    console.log('🔍 IP Detection Debug:');
    console.log('- x-forwarded-for:', forwardedFor);
    console.log('- x-real-ip:', realIP);
    console.log('- cf-connecting-ip:', cfConnectingIP);
    console.log('- req.ip:', reqIP);
    console.log('- remoteAddress:', remoteAddress);

    let clientIP: string;

    // 1. Check X-Forwarded-For (most common with proxies/load balancers)
    if (forwardedFor) {
      // X-Forwarded-For: client, proxy1, proxy2
      clientIP = forwardedFor.split(',')[0].trim();
      console.log('✅ Using X-Forwarded-For:', clientIP);
    }
    // 2. Check X-Real-IP (nginx proxy)
    else if (realIP) {
      clientIP = realIP;
      console.log('✅ Using X-Real-IP:', clientIP);
    }
    // 3. Check CF-Connecting-IP (Cloudflare)
    else if (cfConnectingIP) {
      clientIP = cfConnectingIP;
      console.log('✅ Using CF-Connecting-IP:', clientIP);
    }
    // 4. Check req.ip (Express with trust proxy)
    else if (reqIP && reqIP !== '::1' && reqIP !== '127.0.0.1') {
      clientIP = reqIP;
      console.log('✅ Using req.ip:', clientIP);
    }
    // 5. Check remoteAddress (direct connection)
    else if (remoteAddress && remoteAddress !== '::1' && remoteAddress !== '127.0.0.1') {
      clientIP = remoteAddress;
      console.log('✅ Using remoteAddress:', clientIP);
    }
    // 6. Fallback to req.ip or remoteAddress
    else {
      clientIP = reqIP || remoteAddress || 'unknown';
      console.log('⚠️ Using fallback IP:', clientIP);
    }

    // Normalize IPv6 mapped IPv4
    if (clientIP === '::1') {
      clientIP = '127.0.0.1';
    }

    // Remove IPv6 prefix if present
    if (clientIP?.startsWith('::ffff:')) {
      clientIP = clientIP.substring(7);
    }

    console.log('🎯 Final client IP:', clientIP);
    return clientIP;
  }
}
