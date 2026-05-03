import type { EnvService } from '../../../shared/services';
import {
  SQL_MYSQL_POOL_MAX_DEFAULT,
  SQL_MYSQL_POOL_MIN_DEFAULT,
  SQL_POSTGRES_POOL_MAX_DEFAULT,
  SQL_POSTGRES_POOL_MIN_DEFAULT,
} from '../../../shared/utils/auto-scaling.constants';

export type SqlPoolDbType = 'mysql' | 'postgres' | string;

export interface SqlPoolConfig {
  min: number;
  max: number;
}

export function resolveSqlPoolConfig(
  dbType: SqlPoolDbType,
  envService: Pick<EnvService, 'get'>,
): SqlPoolConfig {
  const isPostgres = dbType === 'postgres';
  const defaultMin = isPostgres
    ? SQL_POSTGRES_POOL_MIN_DEFAULT
    : SQL_MYSQL_POOL_MIN_DEFAULT;
  const defaultMax = isPostgres
    ? SQL_POSTGRES_POOL_MAX_DEFAULT
    : SQL_MYSQL_POOL_MAX_DEFAULT;
  const envMax = envService.get('SQL_POOL_MAX');
  const envMin = envService.get('SQL_POOL_MIN');
  const max = Math.max(1, Math.trunc(envMax ?? defaultMax));
  const min = Math.max(0, Math.min(Math.trunc(envMin ?? defaultMin), max));
  return { min, max };
}
