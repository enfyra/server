import { BadRequestException, ForbiddenException } from '../../exceptions';
import type { IQueryBuilder } from '../../shared/interfaces/query-builder.interface';

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
      table: 'enfyra_cors_origin',
      fields: ['value', 'isEnabled'],
      limit: 0,
    });
    allowedOrigins = data
      .filter(
        (row) => row?.isEnabled !== false && typeof row?.value === 'string',
      )
      .map((row) => row.value.trim())
      .filter((origin) => origin.length > 0);
  } catch {
    allowedOrigins = [];
  }

  const origins = new Set(
    allowedOrigins.length > 0 ? allowedOrigins : appOrigins,
  );
  return (value) => {
    const parsed = parseOAuthRedirectUrl(value);
    if (!origins.has(parsed.origin)) {
      throw new ForbiddenException('Redirect origin is not allowed');
    }
    return parsed.toString();
  };
}
