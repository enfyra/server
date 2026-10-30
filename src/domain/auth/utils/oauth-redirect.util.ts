import { BadRequestException, ForbiddenException } from '../../exceptions';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';
import { normalizeAuthOrigin } from './auth-origin.util';

export function parseOAuthRedirectUrl(value: unknown): URL {
  if (typeof value !== 'string' || value.length === 0) {
    throw new BadRequestException('Redirect URL is required');
  }

  try {
    const parsed = new URL(value);
    if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
      throw new BadRequestException('Invalid redirect URL protocol');
    }
    return parsed;
  } catch (error) {
    if (error instanceof BadRequestException) throw error;
    throw new BadRequestException(
      'Redirect URL must be an absolute http(s) URL',
    );
  }
}

export async function createOAuthRedirectValidator(
  queryBuilder: Pick<IQueryBuilder, 'find'>,
  appOrigins: readonly string[],
): Promise<(value: unknown) => string> {
  let allowedOrigins: string[] = [];
  try {
    const { data } = await queryBuilder.find({
      table: 'enfyra_auth_origin',
      fields: ['value', 'isEnabled'],
      limit: 0,
    });
    if (!Array.isArray(data)) throw new Error('Invalid auth origin list');
    for (const row of data) {
      if (row?.isEnabled === false) continue;
      const origin = normalizeAuthOrigin(row?.value);
      if (!origin) throw new Error('Invalid auth origin');
      allowedOrigins.push(origin);
    }
  } catch {
    allowedOrigins = [];
  }

  const origins = new Set([...appOrigins, ...allowedOrigins]);
  return (value) => {
    const parsed = parseOAuthRedirectUrl(value);
    if (!origins.has(parsed.origin)) {
      throw new ForbiddenException('Redirect origin is not allowed');
    }
    return parsed.toString();
  };
}
