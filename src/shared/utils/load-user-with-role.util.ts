import { ObjectId } from 'mongodb';
import type { IQueryBuilder } from '../../domain/shared/interfaces/query-builder.interface';
import { DatabaseConfigService } from '../services';

export const USER_CACHE_TTL_MS = 60_000;

export function userCacheKey(id: unknown): string {
  return `user:${String(id)}`;
}

function toMongoObjectId(value: unknown): ObjectId | null {
  if (value instanceof ObjectId) return value;
  if (typeof value === 'string' && /^[0-9a-fA-F]{24}$/.test(value)) {
    return new ObjectId(value);
  }
  return null;
}

export async function loadUserWithRole(
  queryBuilder: IQueryBuilder,
  rawId: unknown,
): Promise<any | null> {
  if (rawId === undefined || rawId === null) return null;

  const isMongoDB = queryBuilder.isMongoDb();
  const idField = DatabaseConfigService.getPkField();
  const idValue = isMongoDB ? toMongoObjectId(rawId) : rawId;
  if (isMongoDB && !idValue) return null;

  const user = await queryBuilder.findOne({
    table: 'user_definition',
    where: { [idField]: idValue },
  });

  if (!user) return null;

  const roleField = isMongoDB ? 'role' : 'roleId';
  const roleId = isMongoDB ? toMongoObjectId(user[roleField]) : user[roleField];
  if (roleId) {
    user.role = await queryBuilder.findOne({
      table: 'role_definition',
      where: { [idField]: roleId },
    });
  }

  return user;
}
