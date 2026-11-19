export const ROUTE_METHOD_SELECTION_FIELDS = [
  'availableMethods',
  'publicMethods',
  'skipRoleGuardMethods',
] as const;

export function hasRouteMethodSelections(body: Record<string, unknown>): boolean {
  return ROUTE_METHOD_SELECTION_FIELDS.some((field) => Object.hasOwn(body, field));
}
