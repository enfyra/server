import { expect, it } from 'vitest';
import { buildZodFromMetadata } from '../../src/shared/utils/zod-from-metadata';

it.each(['bigint', 'long'])('validates exact signed 64-bit values for %s', type => {
  const schema = buildZodFromMetadata({ tableMeta: { name: 'counters', columns: [{ name: 'value', type, isNullable: false }], relations: [] }, mode: 'update', rulesForColumn: () => [], getTableMetadata: () => null } as any)!;
  for (const value of ['9007199254740993', '9223372036854775807', '-9223372036854775808', 42]) expect(schema.safeParse({ value }).success).toBe(true);
  for (const value of ['9223372036854775808', '-9223372036854775809', Number.MAX_SAFE_INTEGER + 1, 1.5, '1.5']) expect(schema.safeParse({ value }).success).toBe(false);
});
