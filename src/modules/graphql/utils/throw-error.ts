import { GraphQLError } from 'graphql';

export function throwGqlError(
  code: string,
  message: string,
  detail?: any,
): never {
  throw new GraphQLError(message, {
    extensions: {
      code,
      detail,
    },
  });
}
