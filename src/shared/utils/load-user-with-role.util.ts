import { randomUUID } from 'node:crypto';
import { ObjectId } from 'mongodb';
import type { ICache } from '../../domain/shared/interfaces/cache.interface';
import type { IQueryBuilder } from '../../domain/shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../services';

export const USER_CACHE_TTL_MS = 60_000;
const USER_AUTHORIZATION_REVISION_TTL_MS = 365 * 24 * 60 * 60 * 1_000;

type CachedUserEntry = {
  expiresAt: number;
  revision: string;
  user: any;
};

const localUserCache = new Map<string, CachedUserEntry>();

export function userCacheKey(id: unknown): string {
  return `user:${String(id)}`;
}

export function userAuthorizationRevisionKey(id: unknown): string {
  return `auth:user:${String(id)}:revision`;
}

function normalizeUserLookupId(
  queryBuilder: IQueryBuilder,
  rawId: unknown,
): unknown | null {
  if (rawId === undefined || rawId === null) return null;
  return queryBuilder.isMongoDb() ? toMongoObjectId(rawId) : toSqlId(rawId);
}

function toMongoObjectId(value: unknown): ObjectId | null {
  if (value instanceof ObjectId) return value;
  if (typeof value === 'string' && /^[0-9a-fA-F]{24}$/.test(value)) {
    return new ObjectId(value);
  }
  return null;
}

function toSqlId(value: unknown): unknown | null {
  if (typeof value === 'number') {
    return Number.isSafeInteger(value) ? value : null;
  }
  if (typeof value !== 'string') return null;
  if (/^[0-9]+$/.test(value)) return value;
  if (
    /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/.test(
      value,
    )
  ) {
    return value;
  }
  return null;
}

function normalizeRoleLookupId(
  queryBuilder: IQueryBuilder,
  rawRole: unknown,
): unknown | null {
  const roleId =
    rawRole && typeof rawRole === 'object' && !(rawRole instanceof ObjectId)
      ? (rawRole as { _id?: unknown; id?: unknown })._id ??
        (rawRole as { id?: unknown }).id
      : rawRole;
  if (roleId === undefined || roleId === null) return null;
  return queryBuilder.isMongoDb() ? toMongoObjectId(roleId) : toSqlId(roleId);
}

export async function loadUserWithRoles(
  queryBuilder: IQueryBuilder,
  rawId: unknown,
): Promise<any | null> {
  const idField = DatabaseConfigService.getPkField();
  const idValue = normalizeUserLookupId(queryBuilder, rawId);
  if (!idValue) return null;

  const user = await queryBuilder.findOne({
    table: 'enfyra_user',
    where: { [idField]: idValue },
    fields: ['*', 'roles.*'],
  });
  if (!user) return null;

  const roleIds = new Set<unknown>();
  for (const rawRole of Array.isArray(user.roles) ? user.roles : []) {
    const roleId = normalizeRoleLookupId(queryBuilder, rawRole);
    if (roleId !== null) roleIds.add(roleId);
  }

  user.roles = await Promise.all(
    Array.from(roleIds).map((roleId) =>
      queryBuilder.findOne({
        table: 'enfyra_role',
        where: { [idField]: roleId },
      }),
    ),
  );
  user.roles = user.roles.filter(Boolean);
  return user;
}

export async function loadCachedUserWithRoles(
  queryBuilder: IQueryBuilder,
  cacheService: ICache | undefined,
  rawId: unknown,
): Promise<any | null> {
  const idValue = normalizeUserLookupId(queryBuilder, rawId);
  if (!idValue) return null;

  const revision = await getOrCreateUserAuthorizationRevision(
    cacheService,
    idValue,
  );
  if (!revision) return loadUserWithRoles(queryBuilder, idValue);

  const cacheKey = userCacheKey(idValue);
  const cachedUser = localUserCache.get(cacheKey);
  if (cachedUser) {
    if (
      cachedUser.revision === revision &&
      cachedUser.expiresAt > Date.now()
    ) {
      return cachedUser.user;
    }
    localUserCache.delete(cacheKey);
  }

  const user = await loadUserWithRoles(queryBuilder, idValue);
  if (user) {
    localUserCache.set(cacheKey, {
      user,
      revision,
      expiresAt: Date.now() + USER_CACHE_TTL_MS,
    });
  }
  return user;
}

export async function primeCachedUserWithRoles(
  queryBuilder: IQueryBuilder,
  cacheService: ICache | undefined,
  rawId: unknown,
): Promise<void> {
  const idValue = normalizeUserLookupId(queryBuilder, rawId);
  if (!idValue) return;
  const revision = await getOrCreateUserAuthorizationRevision(
    cacheService,
    idValue,
  );
  if (!revision) return;

  const user = await loadUserWithRoles(queryBuilder, idValue);
  if (!user) return;
  localUserCache.set(userCacheKey(idValue), {
    user,
    revision,
    expiresAt: Date.now() + USER_CACHE_TTL_MS,
  });
}

export function primeCachedUserSnapshot(
  rawId: unknown,
  user: any,
  revision = 'test',
): void {
  if (!user || rawId === undefined || rawId === null) return;
  localUserCache.set(userCacheKey(rawId), {
    user,
    revision,
    expiresAt: Date.now() + USER_CACHE_TTL_MS,
  });
}

export async function bumpUserAuthorizationRevision(
  cacheService: ICache,
  rawId: unknown,
): Promise<void> {
  if (rawId === undefined || rawId === null) return;
  await cacheService.set(
    userAuthorizationRevisionKey(rawId),
    randomUUID(),
    USER_AUTHORIZATION_REVISION_TTL_MS,
  );
}

export function invalidateCachedUserWithRoles(rawId: unknown): void {
  if (rawId === undefined || rawId === null) return;
  localUserCache.delete(userCacheKey(rawId));
}

export function clearLocalUserCacheForTesting(): void {
  localUserCache.clear();
}

async function getOrCreateUserAuthorizationRevision(
  cacheService: ICache | undefined,
  rawId: unknown,
): Promise<string | null> {
  if (!cacheService) return null;
  try {
    const key = userAuthorizationRevisionKey(rawId);
    const current = await cacheService.get<string>(key);
    if (typeof current === 'string' && current) return current;

    const revision = randomUUID();
    await cacheService.set(
      key,
      revision,
      USER_AUTHORIZATION_REVISION_TTL_MS,
    );
    return revision;
  } catch {
    return null;
  }
}

export function withUserRequestContext(
  user: any,
  context: {
    loginProvider?: unknown;
    tokenType?: unknown;
    tokenId?: unknown;
  },
): any {
  if (!user) return user;
  return {
    ...user,
    roles: Array.isArray(user.roles)
      ? user.roles.map((role: any) =>
          role && typeof role === 'object' ? { ...role } : role,
        )
      : [],
    loginProvider: context.loginProvider ?? null,
    tokenType: context.tokenType ?? null,
    apiTokenId: context.tokenId ?? null,
  };
}
