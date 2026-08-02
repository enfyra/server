export const GRAPHQL_OPERATION_NAMES = [
  'QUERY',
  'CREATE',
  'UPDATE',
  'DELETE',
] as const;

export type GraphqlOperationName = (typeof GRAPHQL_OPERATION_NAMES)[number];

export type GraphqlPermissionGrant = {
  isEnabled: boolean;
  roleId: string | null;
  allowedUserIds: string[];
  operations: GraphqlOperationName[];
};

export type GraphqlAccessDefinition = {
  isEnabled: boolean;
  publicOperations: GraphqlOperationName[];
  permissions: GraphqlPermissionGrant[];
};

function toIdString(value: any): string | null {
  if (value === undefined || value === null) return null;
  return String(value?._id ?? value?.id ?? value);
}

export function normalizeGraphqlOperation(value: unknown): GraphqlOperationName {
  const operation = String(value ?? '').toUpperCase();
  if (!GRAPHQL_OPERATION_NAMES.includes(operation as GraphqlOperationName)) {
    throw new Error(`Unsupported GraphQL operation: ${String(value)}`);
  }
  return operation as GraphqlOperationName;
}

export function normalizeGraphqlOperationList(
  values: readonly unknown[] | null | undefined,
): GraphqlOperationName[] {
  const result: GraphqlOperationName[] = [];
  const seen = new Set<GraphqlOperationName>();
  for (const value of values ?? []) {
    const operation = normalizeGraphqlOperation(
      typeof value === 'object' && value !== null ? (value as any).name : value,
    );
    if (seen.has(operation)) continue;
    seen.add(operation);
    result.push(operation);
  }
  return result;
}

export function assertGraphqlPermissionScope(input: {
  role: unknown;
  allowedUsers: readonly unknown[] | null | undefined;
}): void {
  const hasRole = toIdString(input.role) !== null;
  const hasUsers = (input.allowedUsers ?? []).some(
    (user) => toIdString(user) !== null,
  );
  if (hasRole === hasUsers) {
    throw new Error(
      'GraphQL permission must target exactly one scope: one role or one or more allowed users',
    );
  }
}

export function assertNoPublicPermissionOverlap(input: {
  publicOperations: readonly GraphqlOperationName[];
  permissionOperations: readonly GraphqlOperationName[];
}): void {
  const publicSet = new Set(input.publicOperations);
  const overlap = input.permissionOperations.filter((operation) =>
    publicSet.has(operation),
  );
  if (overlap.length > 0) {
    throw new Error(
      `Public GraphQL operations cannot also be granted by permissions: ${overlap.join(', ')}`,
    );
  }
}

export function hasGraphqlOperationAccess(input: {
  definition: GraphqlAccessDefinition;
  operation: GraphqlOperationName;
  user: any;
}): { allowed: boolean; isPublic: boolean } {
  const { definition, operation, user } = input;
  if (!definition.isEnabled) return { allowed: false, isPublic: false };

  if (definition.publicOperations.includes(operation)) {
    return { allowed: true, isPublic: true };
  }

  if (!user || user.isAnonymous) {
    return { allowed: false, isPublic: false };
  }
  if (user.isRootAdmin === true) {
    return { allowed: true, isPublic: false };
  }

  const userId = toIdString(user);
  const roleId = toIdString(user.role);
  const allowed = definition.permissions.some((permission) => {
    if (!permission.isEnabled || !permission.operations.includes(operation)) {
      return false;
    }
    if (permission.allowedUserIds.length > 0) {
      return userId !== null && permission.allowedUserIds.includes(userId);
    }
    return permission.roleId !== null && permission.roleId === roleId;
  });

  return { allowed, isPublic: false };
}
