import { describe, expect, it } from 'vitest';
import { generateDefaultValue } from '../../src/modules/table-management/utils/generate-default-record';
import { ensureMongoSingleRecord } from '../../src/modules/table-management/services/table-post-migration.service';

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

  it('generates a calendar day and a UTC instant for the respective types', () => {
    expect(generateDefaultValue(column('date'))).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(generateDefaultValue(column('datetime'))).toMatch(/T.*Z$/);
  });

  it('uses canonical enum option arrays for required defaults', () => {
    expect(generateDefaultValue(column('enum', { options: ['draft', 'active'] }))).toBe('draft');
  });

  it('keeps Mongo singleton calendar defaults as strings and instants as BSON dates', async () => {
    let inserted: any;
    await ensureMongoSingleRecord({ mongoService: { getDb: () => ({ collection: () => ({ countDocuments: async () => 0, insertOne: async (record: any) => { inserted = record; } }) }) } as any, tableName: 'settings', columns: [column('date'), { ...column('datetime', { defaultValue: 'now' }), name: 'startedAt' }] });
    expect(inserted.value).toMatch(/^\d{4}-\d{2}-\d{2}$/);
    expect(inserted.startedAt).toBeInstanceOf(Date);
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
