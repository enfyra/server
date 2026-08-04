import { ObjectId } from 'mongodb';
import type { IQueryBuilder } from '../../domain/shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../services';

export const USER_CACHE_TTL_MS = 60_000;

type CachedUserEntry = {
  expiresAt: number;
  user: any;
};

const localUserCache = new Map<string, CachedUserEntry>();

export function userCacheKey(id: unknown): string {
  return `user:${String(id)}`;
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

export async function loadUserWithRole(
  queryBuilder: IQueryBuilder,
  rawId: unknown,
): Promise<any | null> {
  const isMongoDB = queryBuilder.isMongoDb();
  const idField = DatabaseConfigService.getPkField();
  const idValue = normalizeUserLookupId(queryBuilder, rawId);
  if (!idValue) return null;

  const user = await queryBuilder.findOne({
    table: 'enfyra_user',
    where: { [idField]: idValue },
  });

  if (!user) return null;

  const roleField = isMongoDB ? 'role' : 'roleId';
  const roleId = isMongoDB
    ? toMongoObjectId(user[roleField])
    : toSqlId(user[roleField]);
  if (roleId) {
    user.role = await queryBuilder.findOne({
      table: 'enfyra_role',
      where: { [idField]: roleId },
    });
  }

  return user;
}

export async function loadCachedUserWithRole(
  queryBuilder: IQueryBuilder,
  rawId: unknown,
): Promise<any | null> {
  const idValue = normalizeUserLookupId(queryBuilder, rawId);
  if (!idValue) return null;

  const cacheKey = userCacheKey(idValue);
  const cachedUser = localUserCache.get(cacheKey);
  if (cachedUser) {
    if (cachedUser.expiresAt > Date.now()) return cachedUser.user;
    localUserCache.delete(cacheKey);
  }

  const user = await loadUserWithRole(queryBuilder, idValue);
  if (user) {
    localUserCache.set(cacheKey, {
      user,
      expiresAt: Date.now() + USER_CACHE_TTL_MS,
    });
  }
  return user;
}

export async function primeCachedUserWithRole(
  queryBuilder: IQueryBuilder,
  rawId: unknown,
): Promise<void> {
  const user = await loadUserWithRole(queryBuilder, rawId);
  if (!user) return;

  const idValue = normalizeUserLookupId(queryBuilder, rawId);
  if (!idValue) return;
  localUserCache.set(userCacheKey(idValue), {
    user,
    expiresAt: Date.now() + USER_CACHE_TTL_MS,
  });
}

export function primeCachedUserSnapshot(rawId: unknown, user: any): void {
  if (!user || rawId === undefined || rawId === null) return;
  localUserCache.set(userCacheKey(rawId), {
    user,
    expiresAt: Date.now() + USER_CACHE_TTL_MS,
  });
}

export function invalidateCachedUserWithRole(rawId: unknown): void {
  if (rawId === undefined || rawId === null) return;
  localUserCache.delete(userCacheKey(rawId));
}

export function clearLocalUserCacheForTesting(): void {
  localUserCache.clear();
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
    role:
      user.role && typeof user.role === 'object' && !Array.isArray(user.role)
        ? { ...user.role }
        : user.role,
    loginProvider: context.loginProvider ?? null,
    tokenType: context.tokenType ?? null,
    apiTokenId: context.tokenId ?? null,
  };
}
