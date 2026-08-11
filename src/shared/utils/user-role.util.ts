import type {
  TAuthenticatedUser,
  TUserRoleReference,
} from '../types/authenticated-user.types';

export function toRoleId(value: TUserRoleReference | null | undefined): string | null {
  if (value === undefined || value === null) return null;
  if (typeof value === 'object') {
    const id = value._id ?? value.id;
    return id === undefined || id === null ? null : String(id);
  }
  return String(value);
}

export function getUserRoleIds(user: TAuthenticatedUser | null | undefined): Set<string> {
  if (!user || user.isAnonymous || !Array.isArray(user.roles)) return new Set();

  const roleIds = new Set<string>();
  for (const role of user.roles) {
    const roleId = toRoleId(role);
    if (roleId !== null) roleIds.add(roleId);
  }
  return roleIds;
}

export function userHasRole(
  user: TAuthenticatedUser | null | undefined,
  role: TUserRoleReference | null | undefined,
): boolean {
  const roleId = toRoleId(role);
  return roleId !== null && getUserRoleIds(user).has(roleId);
}
