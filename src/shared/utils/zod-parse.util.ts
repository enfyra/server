import { z, ZodType, ZodError } from 'zod';
import { BadRequestException } from '../../core/exceptions/custom-exceptions';

type ZodIssueLike = ZodError['issues'][number];

export function formatZodErrors(err: ZodError): string[] {
  return err.issues.map(formatIssue);
}

function formatIssue(issue: ZodIssueLike): string {
  const field = issue.path.length > 0 ? issue.path.join('.') : 'value';

  switch (issue.code) {
    case 'invalid_type': {
      const expected = (issue as any).expected;
      const msg = issue.message || '';
      if (/received undefined|received null/.test(msg)) {
        return `${field} is required`;
      }
      return `${field} must be a ${expected}`;
    }
    case 'invalid_format': {
      const format = (issue as any).format as string | undefined;
      if (format === 'regex') return `${field} must match required pattern`;
      if (format) return `${field} must be a valid ${format}`;
      return `${field} has invalid format`;
    }
    case 'too_small': {
      const min = (issue as any).minimum;
      const origin = (issue as any).origin;
      if (origin === 'string') {
        if (min === 1) return `${field} should not be empty`;
        return `${field} must be at least ${min} characters`;
      }
      if (origin === 'array') return `${field} must contain at least ${min} items`;
      if (origin === 'number') return `${field} must be at least ${min}`;
      return `${field} is too small`;
    }
    case 'too_big': {
      const max = (issue as any).maximum;
      const origin = (issue as any).origin;
      if (origin === 'string') return `${field} must be at most ${max} characters`;
      if (origin === 'array') return `${field} must contain at most ${max} items`;
      if (origin === 'number') return `${field} must be at most ${max}`;
      return `${field} is too big`;
    }
    case 'invalid_value': {
      const values = (issue as any).values as any[] | undefined;
      if (Array.isArray(values) && values.length > 0) {
        return `${field} must be one of: ${values.join(', ')}`;
      }
      return issue.message || `${field} has invalid value`;
    }
    case 'unrecognized_keys': {
      const keys = (issue as any).keys as string[];
      return keys.map((k) => `${k} is not allowed`).join('; ');
    }
    case 'invalid_union':
      return `${field} has invalid format`;
    case 'custom':
      return issue.message || `${field} is invalid`;
    default:
      return issue.message || `${field} is invalid`;
  }
}

export function parseOrBadRequest<S extends ZodType>(
  schema: S,
  input: unknown,
): z.infer<S> {
  const result = schema.safeParse(input);
  if (!result.success) {
    throw new BadRequestException(formatZodErrors(result.error));
  }
  return result.data;
}
