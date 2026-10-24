import type { AuthenticatedRequest } from '../../../domain/auth/types/auth.types';
import type { DynamicRequestContext } from '../../../shared/types/dynamic-context.types';

export interface GraphqlResolverContext {
  request?: DynamicRequestContext;
  auth?: AuthenticatedRequest | null;
  clientIp?: string;
}

export interface GraphqlQueryArguments {
  filter?: unknown;
  page?: number;
  limit?: number;
  meta?: string;
  sort?: string | string[];
}

export interface GraphqlMutationArguments {
  id?: string | number;
  input?: unknown;
}

export interface ThrowGqlErrorOptions {
  statusCode?: number;
  details?: unknown;
}
