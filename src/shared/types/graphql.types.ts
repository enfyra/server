export const GRAPHQL_OPERATION_NAMES = [
  'QUERY',
  'CREATE',
  'UPDATE',
  'DELETE',
] as const;

export type GraphqlOperationName = (typeof GRAPHQL_OPERATION_NAMES)[number];
