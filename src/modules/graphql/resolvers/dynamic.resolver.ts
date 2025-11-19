import {
  BadRequestException,
  forwardRef,
  Inject,
  Injectable,
} from '@nestjs/common';
import { throwGqlError } from '../utils/throw-error';
import { ConfigService } from '@nestjs/config';
import { DynamicRepository } from '../../dynamic-api/repositories/dynamic.repository';
import { convertFieldNodesToFieldPicker } from '../utils/field-string-convertor';
import { TableHandlerService } from '../../table-management/services/table-handler.service';
import { QueryEngine } from '../../../infrastructure/query-engine/services/query-engine.service';
import { JwtService } from '@nestjs/jwt';
import { QueryBuilderService } from '../../../infrastructure/query-builder/query-builder.service';
import { MetadataCacheService } from '../../../infrastructure/cache/services/metadata-cache.service';
import { HandlerExecutorService } from '../../../infrastructure/handler-executor/services/handler-executor.service';
import { RouteCacheService } from '../../../infrastructure/cache/services/route-cache.service';
import { StorageConfigCacheService } from '../../../infrastructure/cache/services/storage-config-cache.service';
import { SystemProtectionService } from '../../dynamic-api/services/system-protection.service';
import { TableValidationService } from '../../dynamic-api/services/table-validation.service';
import { ScriptErrorFactory } from '../../../shared/utils/script-error-factory';

@Injectable()
export class DynamicResolver {
  constructor(
    @Inject(forwardRef(() => TableHandlerService))
    private tableHandlerService: TableHandlerService,
    private queryEngine: QueryEngine,
    private jwtService: JwtService,
    private queryBuilder: QueryBuilderService,
    private metadataCacheService: MetadataCacheService,
    private handlerExecutorService: HandlerExecutorService,
    private routeCacheService: RouteCacheService,
    private storageConfigCacheService: StorageConfigCacheService,
    private systemProtectionService: SystemProtectionService,
    private tableValidationService: TableValidationService,
    private configService: ConfigService,
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
      $user: user ?? null, // Always exists (null if no user)
      $repos: {}, // Will be populated below
      $req: context.request,
      $body: {},
      $params: {},
      $logs: () => {},
      $share: {},
    };

    const dynamicFindEntries = await Promise.all(
      [mainTable, ...targetTables].map(async (table) => {
        const dynamicRepo = new DynamicRepository({
          context: handlerCtx,
          tableName: table.name,
          tableHandlerService: this.tableHandlerService,
          queryBuilder: this.queryBuilder,
          metadataCacheService: this.metadataCacheService,
          queryEngine: this.queryEngine,
          routeCacheService: this.routeCacheService,
          storageConfigCacheService: this.storageConfigCacheService,
          systemProtectionService: this.systemProtectionService,
          tableValidationService: this.tableValidationService,
        });

        await dynamicRepo.init();

        const name =
          table.name === mainTable.name ? 'main' : (table.alias ?? table.name);

        return [name, dynamicRepo];
      }),
    );

    handlerCtx.$repos = Object.fromEntries(dynamicFindEntries);

    try {
      const defaultHandler = `return await $ctx.$repos.main.find();`;
      const result = await this.handlerExecutorService.run(
        defaultHandler,
        handlerCtx,
        this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 5000),
      );

      return result;
    } catch (error) {
      throwGqlError('SCRIPT_ERROR', error.message);
    }
  }

  async dynamicMutationResolver(
    mutationName: string,
    args: any,
    context: any,
    info: any,
  ) {
    try {
      const match = mutationName.match(/^(create|update|delete)_(.+)$/);
      if (!match) {
        throw new BadRequestException(`Invalid mutation name: ${mutationName}`);
      }

      const operation = match[1];
      const tableName = match[2];

      const { matchedRoute: currentRoute, user } = await this.middleware(tableName, context, info);

      await this.canPassMutation(currentRoute, context.req?.headers?.authorization);

      const handlerCtx = {
        $user: user ?? null,
        $repos: {},
        $req: context.request,
        $body: args.input || {},
        $params: { id: args.id },
        $logs: () => {},
        $share: {},
      };

      const dynamicRepo = new DynamicRepository({
        context: handlerCtx,
        tableName: tableName,
        queryEngine: this.queryEngine,
        queryBuilder: this.queryBuilder,
        metadataCacheService: this.metadataCacheService,
        tableHandlerService: this.tableHandlerService,
        routeCacheService: this.routeCacheService,
        storageConfigCacheService: this.storageConfigCacheService,
        systemProtectionService: this.systemProtectionService,
        tableValidationService: this.tableValidationService,
      });

      await dynamicRepo.init();

      const dynamicFindEntries = [
        ['main', dynamicRepo],
        ...(currentRoute.targetTables || []).map((table: any) => [
          table.name,
          new DynamicRepository({
            context: handlerCtx,
            tableName: table.name,
            queryEngine: this.queryEngine,
            queryBuilder: this.queryBuilder,
            metadataCacheService: this.metadataCacheService,
            tableHandlerService: this.tableHandlerService,
            routeCacheService: this.routeCacheService,
            storageConfigCacheService: this.storageConfigCacheService,
            systemProtectionService: this.systemProtectionService,
            tableValidationService: this.tableValidationService,
          }),
        ]),
      ];

      for (const [, repo] of dynamicFindEntries) {
        await (repo as DynamicRepository).init();
      }

      handlerCtx.$repos = Object.fromEntries(dynamicFindEntries);

      let defaultHandler: string;
      switch (operation) {
        case 'create':
          defaultHandler = `return await $ctx.$repos.main.create({ data: $ctx.$body });`;
          break;
        case 'update':
          defaultHandler = `return await $ctx.$repos.main.update({ id: $ctx.$params.id, data: $ctx.$body });`;
          break;
        case 'delete':
          defaultHandler = `await $ctx.$repos.main.delete({ id: $ctx.$params.id }); return \`Delete id \${$ctx.$params.id} successfully\`;`;
          break;
        default:
          throw new BadRequestException(`Unsupported operation: ${operation}`);
      }

      const result = await this.handlerExecutorService.run(
        defaultHandler,
        handlerCtx,
        this.configService.get<number>('DEFAULT_HANDLER_TIMEOUT', 5000),
      );
      
      // Extract data from result format like { data: [...] }
      if (result && result.data && Array.isArray(result.data)) {
        return result.data[0];
      }
      
      return result;
    } catch (error) {
      throwGqlError('MUTATION_ERROR', error.message);
    }
  }

  private async middleware(mainTableName: string, context: any, info: any) {
    if (!mainTableName) {
      throwGqlError('400', 'Missing table name');
    }

    // Use RouteEngine for O(log N) matching instead of O(N) linear search
    const routeEngine = this.routeCacheService.getRouteEngine();
    const operation = info.operation.operation; // 'query' or 'mutation'
    const method = operation === 'query' ? 'GQL_QUERY' : 'GQL_MUTATION';

    const matchResult = routeEngine.find(method, `/${mainTableName}`);

    if (!matchResult) {
      throwGqlError('404', 'Route not found');
    }

    const currentRoute = matchResult.route;

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

    const user = await this.queryBuilder.findOneWhere('user_definition', { id: decoded.id });

    if (!user) {
      throwGqlError('401', 'Invalid user');
    }

    // Load role if needed
    if (user.roleId) {
      user.role = await this.queryBuilder.findOneWhere('role_definition', { id: user.roleId });
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

  private async canPassMutation(currentRoute: any, accessToken: string) {
    if (!currentRoute?.isEnabled) {
      throwGqlError('404', 'NotFound');
    }

    const isPublished = currentRoute.publishedMethods.some(
      (item: any) => item.method === 'GQL_MUTATION',
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

    const user = await this.queryBuilder.findOneWhere('user_definition', { id: decoded.id });

    if (!user) {
      throwGqlError('401', 'Invalid user');
    }

    // Load role if needed
    if (user.roleId) {
      user.role = await this.queryBuilder.findOneWhere('role_definition', { id: user.roleId });
    }

    const canPass =
      user.isRootAdmin ||
      currentRoute.routePermissions?.some(
        (permission: any) =>
          permission.role?.id === user.role?.id &&
          permission.methods?.includes('GQL_MUTATION'),
      );

    if (!canPass) {
      throwGqlError('403', 'Not allowed');
    }

    return user;
  }
}
