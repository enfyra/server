import { GraphQLError } from 'graphql';

export interface ThrowGqlErrorOptions {
  statusCode?: number;
  details?: any;
}

export function throwGqlError(
  code: string,
  message: string,
  options?: ThrowGqlErrorOptions,
): never {
  const extensions: Record<string, any> = { code };
  if (typeof options?.statusCode === 'number') {
    extensions.statusCode = options.statusCode;
  }
  if (options?.details !== undefined) {
    extensions.details = options.details;
  }

  throw new GraphQLError(message, { extensions });
}
