import { BadRequestException } from '../../../domain/exceptions';
import { throwGqlError } from '../utils/throw-error';
import { convertFieldNodesToFieldPicker } from '../utils/field-string-converter';
import * as jwt from 'jsonwebtoken';
import { GraphQLError } from 'graphql';
import { QueryBuilderService } from '@enfyra/kernel';
import { getErrorMessage } from '../../../shared/utils/error.util';
import { EnvService, DynamicContextFactory } from '../../../shared/services';
import { ExecutorEngineService } from '@enfyra/kernel';
import { RepoRegistryService } from '../../../engines/cache';
import { isMetadataTable } from '../../../shared/utils/cache-events.constants';
import { loadCachedUserWithRole } from '../../../shared/utils/load-user-with-role.util';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { ApiTokenService } from '../../../domain/auth/services/api-token.service';
import {
  hasGraphqlOperationAccess,
  type GraphqlOperationName,
} from '../utils/graphql-access.util';

export class DynamicResolver {
  private readonly queryBuilderService: QueryBuilderService;
  private readonly executorEngineService: ExecutorEngineService;
  private readonly repoRegistryService: RepoRegistryService;
  private readonly runtimeRegistryService: RuntimeRegistryService;
  private readonly envService: EnvService;
  private readonly dynamicContextFactory: DynamicContextFactory;
  private readonly apiTokenService: ApiTokenService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    executorEngineService: ExecutorEngineService;
    repoRegistryService: RepoRegistryService;
    runtimeRegistryService: RuntimeRegistryService;
    envService: EnvService;
    dynamicContextFactory: DynamicContextFactory;
    apiTokenService: ApiTokenService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.executorEngineService = deps.executorEngineService;
    this.repoRegistryService = deps.repoRegistryService;
    this.runtimeRegistryService = deps.runtimeRegistryService;
    this.envService = deps.envService;
    this.apiTokenService = deps.apiTokenService;
    this.dynamicContextFactory = deps.dynamicContextFactory;
  }

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
    const { mainTable, user } = await this.middleware(
      tableName,
      'QUERY',
      context,
    );
    const selections = info.fieldNodes?.[0]?.selectionSet?.selections || [];
    const fullFieldPicker = convertFieldNodesToFieldPicker(selections);
    const fieldPicker = fullFieldPicker
      .filter((f) => f.startsWith('data.'))
      .map((f) => f.replace(/^data\./, ''));
    const metaPicker = fullFieldPicker
      .filter((f) => f.startsWith('meta.'))
      .map((f) => f.replace(/^meta\./, ''));
    const handlerCtx: any = this.dynamicContextFactory.createGraphql({
      request: context.request,
      user: user ?? null,
      body: {},
      params: {},
      args: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
        aggregate: args.aggregate,
      },
      query: {
        fields: fieldPicker.join(','),
        filter: args.filter,
        page: args.page,
        limit: args.limit,
        meta: metaPicker.join(',') as any,
        sort: args.sort,
        aggregate: args.aggregate,
      },
    });
    handlerCtx.$repos = this.repoRegistryService.createReposProxy(
      handlerCtx,
      mainTable?.name,
    );
    try {
      const defaultHandler = `return await $ctx.$repos.main.find();`;
      const result = await this.executorEngineService.run(
        defaultHandler,
        handlerCtx,
        30000,
      );
      return this.sanitizeResult(result, mainTable?.name);
    } catch (error) {
      throwGqlError('SCRIPT_ERROR', getErrorMessage(error));
    }
  }

  async dynamicMutationResolver(
    mutationName: string,
    args: any,
    context: any,
    _info: any,
  ) {
    try {
      const match = mutationName.match(/^(create|update|delete)_(.+)$/);
      if (!match) {
        throw new BadRequestException(`Invalid mutation name: ${mutationName}`);
      }
      const operation = match[1];
      const tableName = match[2];
      const graphqlOperation = operation.toUpperCase() as GraphqlOperationName;
      const { user } = await this.middleware(
        tableName,
        graphqlOperation,
        context,
      );
      const handlerCtx: any = this.dynamicContextFactory.createGraphql({
        request: context.request,
        user: user ?? null,
        body: args.input || {},
        params: { id: args.id },
      });
      handlerCtx.$repos = this.repoRegistryService.createReposProxy(
        handlerCtx,
        tableName,
      );
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
      const result = await this.executorEngineService.run(
        defaultHandler,
        handlerCtx,
        30000,
      );
      if (result && result.data && Array.isArray(result.data)) {
        return this.sanitizeResult(result.data[0], tableName);
      }
      return this.sanitizeResult(result, tableName);
    } catch (error) {
      if (error instanceof GraphQLError) throw error;
      throwGqlError('MUTATION_ERROR', getErrorMessage(error));
    }
  }

  private async middleware(
    mainTableName: string,
    operation: GraphqlOperationName,
    context: any,
  ) {
    if (!mainTableName) {
      throwGqlError('400', 'Missing table name');
    }

    if (isMetadataTable(mainTableName)) {
      throwGqlError(
        '403',
        `Metadata table "${mainTableName}" is not accessible via GraphQL. Use REST API instead.`,
      );
    }

    const definition =
      this.runtimeRegistryService.getGraphqlDefinitionForTable(mainTableName);
    if (!definition?.isEnabled) {
      throwGqlError(
        '404',
        `GraphQL is not enabled for table: ${mainTableName}`,
      );
    }

    const accessToken = this.getBearerToken(context);
    const isPublic = definition.publicOperations.includes(operation);
    const user = accessToken
      ? await this.authenticate(accessToken)
      : isPublic
        ? null
        : this.throwAuthenticationRequired();

    const access = hasGraphqlOperationAccess({
      definition,
      operation,
      user,
    });
    if (!access.allowed) {
      throwGqlError('403', 'Forbidden');
    }

    return {
      user,
      mainTable: { name: mainTableName },
    };
  }

  private getBearerToken(context: any): string {
    const authorization = context.request?.headers?.get('authorization') || '';
    const match = authorization.match(/^Bearer\s+(.+)$/i);
    return match?.[1]?.trim() || '';
  }

  private throwAuthenticationRequired(): never {
    return throwGqlError('401', 'Authentication required') as never;
  }

  private async authenticate(accessToken: string) {
    let decoded: jwt.JwtPayload;
    try {
      decoded = jwt.verify(
        accessToken,
        this.envService.get('SECRET_KEY'),
      ) as jwt.JwtPayload;
    } catch {
      throwGqlError('401', 'Unauthorized');
    }
    if (
      decoded.tokenType === 'api_token' &&
      !(await this.apiTokenService.validateAccessPayload(decoded))
    ) {
      throwGqlError('401', 'Token has been revoked');
    }
    const user = await loadCachedUserWithRole(
      this.queryBuilderService,
      decoded.id,
    );
    if (!user) {
      throwGqlError('401', 'Invalid user');
    }
    return user;
  }

  private sanitizeResult(result: any, _tableName?: string): any {
    return result;
  }
}
