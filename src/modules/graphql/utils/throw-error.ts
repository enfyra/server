import { GraphQLError } from 'graphql';
import type { ThrowGqlErrorOptions } from '../types/graphql-resolver.types';

export function throwGqlError(
  code: string,
  message: string,
  options?: ThrowGqlErrorOptions,
): never {
  const extensions: Record<string, unknown> = { code };
  if (typeof options?.statusCode === 'number') {
    extensions.statusCode = options.statusCode;
  }
  if (options?.details !== undefined) {
    extensions.details = options.details;
  }

  throw new GraphQLError(message, { extensions });
}
