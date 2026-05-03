export function isCanonicalTableRoutePath(
  path: string | undefined,
  tableName: string | undefined,
): boolean {
  if (!path || !tableName) return false;
  const normalized = path.replace(/^\/+|\/+$/g, '');
  if (!normalized || normalized.includes('/')) return false;
  return normalized === tableName;
}

export const DEFAULT_REST_HANDLER_LOGIC: Record<string, string> = {
  GET: 'return await @REPOS.main.find();',
  POST: 'return await @REPOS.main.create({ data: @BODY });',
  PATCH: 'return await @REPOS.main.update({ id: @PARAMS.id, data: @BODY });',
  DELETE: 'return await @REPOS.main.delete({ id: @PARAMS.id });',
};
