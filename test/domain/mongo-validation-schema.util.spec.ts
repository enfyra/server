import { describe, expect, it } from 'vitest';
import { buildMongoValidationSchema } from '../../src/engines/mongo/utils/mongo-validation-schema.util';

type JsonSchema = {
  properties: Record<string, { bsonType: unknown; description?: string }>;
  required?: string[];
};

describe('buildMongoValidationSchema', () => {
  it('treats an omitted isNullable as nullable to match runtime metadata', () => {
    const schema = buildMongoValidationSchema([
      { name: 'title', type: 'varchar', isNullable: false },
      { name: 'slug', type: 'varchar' },
    ]) as JsonSchema;

    expect(schema.properties.slug.bsonType).toEqual(['string', 'null']);
    expect(schema.required).toEqual(['title']);
  });

  it('keeps an explicit non-nullable column required when it has no default', () => {
    const schema = buildMongoValidationSchema([
      { name: 'title', type: 'varchar', isNullable: false },
    ]) as JsonSchema;

    expect(schema.properties.title.bsonType).toBe('string');
    expect(schema.required).toEqual(['title']);
  });

  it('does not require a non-nullable column that declares a default', () => {
    const schema = buildMongoValidationSchema([
      { name: 'published', type: 'boolean', isNullable: false, defaultValue: false },
    ]) as JsonSchema;

    expect(schema.required).toBeUndefined();
  });

  it('skips identity and timestamp columns', () => {
    const schema = buildMongoValidationSchema([
      { name: '_id', type: 'objectId' },
      { name: 'createdAt', type: 'datetime' },
      { name: 'updatedAt', type: 'datetime' },
    ]) as JsonSchema;

    expect(Object.keys(schema.properties)).toEqual([]);
  });
});
