import {
  BadRequestException,
  forwardRef,
  Inject,
  Injectable,
} from '@nestjs/common';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { TGqlDynamicContext } from '../../../shared/utils/types/dynamic-context.type';
import { convertFieldNodesToFieldPicker } from '../utils/field-string-convertor';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { throwGqlError } from '../utils/throw-error';
import { CacheService } from '../../../infrastructure/redis/services/cache.service';
import { JwtService } from '@nestjs/jwt';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import { GLOBAL_ROUTES_KEY } from '../../../shared/utils/constant';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { RouteCacheService } from '../../../infrastructure/redis/services/route-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';

@Injectable()
export class DynamicResolver {
  constructor(
    @Inject(forwardRef(() => TableHandlerService))
    private tableHandlerService: TableHandlerService,
    private queryEngine: QueryEngine,
    private cacheService: CacheService,
    private jwtService: JwtService,
    private dataSourceService: DataSourceService,
    private handlerExecutorService: HandlerExecutorService,
    private routeCacheService: RouteCacheService,
    private systemProtectionService: SystemProtectionService,
  ) {}

  async dynamicResolver(
    tableName: string,
    args: {
      filter: any;
      page: number;
      limit: number;
      meta: 'filterCount' | 'totalCount' | '*';
      sort: string | string[];
      aggregate: any;
    },
    context: any,
    info: any,
  ) {
    const { mainTable, targetTables, user } = await this.middleware(
      tableName,
      context,
      info,
    );

    const selections = info.fieldNodes?.[0]?.selectionSet?.selections || [];
    const fullFieldPicker = convertFieldNodesToFieldPicker(selections);
    const fieldPicker = fullFieldPicker
      .filter((f) => f.startsWith('data.'))
      .map((f) => f.replace(/^data\./, ''));
    const metaPicker = fullFieldPicker
      .filter((f) => f.startsWith('meta.'))
      .map((f) => f.replace(/^meta\./, ''));

    // Create context compatible with DynamicRepository
    const handlerCtx: any = {
      $throw: ScriptErrorFactory.createThrowHandlers(),
      $helpers: {
        jwt: (payload: any, ext: string) =>
          this.jwtService.sign(payload, { expiresIn: ext }),
      },
      $args: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
        aggregate: args.aggregate,
      },
      $query: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
        aggregate: args.aggregate,
      },
      $user: user ?? undefined,
      $repos: {}, // Will be populated below
      $req: context.request,
      $body: {},
      $params: {},
      $logs: () => {},
      $share: {},
    };

    // Create dynamic repositories with context
    const dynamicFindEntries = await Promise.all(
      [mainTable, ...targetTables].map(async (table) => {
        const dynamicRepo = new DynamicRepository({
          context: handlerCtx,
          tableName: table.name,
          tableHandlerService: this.tableHandlerService,
          dataSourceService: this.dataSourceService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          systemProtectionService: this.systemProtectionService,
          // folderManagementService is optional, not needed in GraphQL
        });

        await dynamicRepo.init();

        const name =
          table.name === mainTable.name ? 'main' : (table.alias ?? table.name);

        return [name, dynamicRepo];
      }),
    );

    // Populate repos in context
    handlerCtx.$repos = Object.fromEntries(dynamicFindEntries);

    try {
      const defaultHandler = `return await $ctx.$repos.main.find();`;
      const result = await this.handlerExecutorService.run(
        defaultHandler,
        handlerCtx,
        5000,
      );

      return result;
    } catch (error) {
      throw new BadRequestException(`Script error: ${error.message}`);
    }
  }

  private async middleware(mainTableName: string, context: any, info: any) {
    if (!mainTableName) {
      throwGqlError('400', 'Missing table name');
    }

    const routes =
      (await this.cacheService.get(GLOBAL_ROUTES_KEY)) ||
      (await this.routeCacheService.loadAndCacheRoutes());

    const currentRoute = routes.find(
      (route) => route.path === '/' + mainTableName,
    );

    const accessToken =
      context.request?.headers?.get('authorization')?.split('Bearer ')[1] || '';

    const user = await this.canPass(currentRoute, accessToken);

    return {
      matchedRoute: currentRoute,
      user,
      mainTable: currentRoute.mainTable,
      targetTables: currentRoute.targetTables,
    };
  }

  private async canPass(currentRoute: any, accessToken: string) {
    if (!currentRoute?.isEnabled) {
      throwGqlError('404', 'NotFound');
    }

    const isPublished = currentRoute.publishedMethods.some(
      (item: any) => item.method === 'GQL_QUERY',
    );

    if (isPublished) {
      return { isAnonymous: true };
    }

    let decoded;
    try {
      decoded = this.jwtService.verify(accessToken);
    } catch {
      throwGqlError('401', 'Unauthorized');
    }

    const userRepo = this.dataSourceService.getRepository('user_definition');
    const user: any = await userRepo.findOne({
      where: { id: decoded.id },
      relations: ['role'],
    });

    if (!user) {
      throwGqlError('401', 'Invalid user');
    }

    const canPass =
      user.isRootAdmin ||
      currentRoute.routePermissions?.some(
        (permission: any) =>
          permission.role?.id === user.role?.id &&
          permission.methods?.includes('GQL_QUERY'),
      );

    if (!canPass) {
      throwGqlError('403', 'Not allowed');
    }

    return user;
  }
}
