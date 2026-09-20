import { describe, expect, it } from 'vitest';
import { generateDefaultValue } from '../../src/modules/table-management/utils/generate-default-record';

function column(type: string, extras: Record<string, any> = {}) {
  return { name: 'value', type, isNullable: false, ...extras };
}

describe('generateDefaultValue for required columns', () => {
  it.each(['date', 'datetime', 'timestamp'] as const)(
    'produces a usable default for a required %s column',
    (type) => {
      const value = generateDefaultValue(column(type));
      expect(typeof value).toBe('string');
      expect(Number.isNaN(new Date(value).getTime())).toBe(false);
    },
  );

  it('keeps a required date column date-only and a datetime column full ISO', () => {
    expect(generateDefaultValue(column('date'))).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(generateDefaultValue(column('datetime'))).toMatch(/T/);
  });

  it('returns null for a nullable column regardless of type', () => {
    expect(generateDefaultValue(column('datetime', { isNullable: true }))).toBe(
      null,
    );
  });

  it('prefers an explicit default over the generated one', () => {
    expect(
      generateDefaultValue(column('datetime', { defaultValue: '2020-01-01' })),
    ).toBe('2020-01-01');
  });
});
