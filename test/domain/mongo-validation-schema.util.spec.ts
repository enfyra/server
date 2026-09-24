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
      {
        name: 'published',
        type: 'boolean',
        isNullable: false,
        defaultValue: false,
      },
    ]) as JsonSchema;

    expect(schema.required).toBeUndefined();
  });

  it('maps Mongo-native types to their BSON validator contracts', () => {
    const schema = buildMongoValidationSchema([
      { name: 'count', type: 'long', isNullable: false },
      { name: 'payload', type: 'object', isNullable: false },
      { name: 'items', type: 'array', isNullable: false },
      { name: 'tags', type: 'array-select', isNullable: false },
      { name: 'enabled', type: 'bool', isNullable: false },
      { name: 'score', type: 'double', isNullable: false },
    ]) as JsonSchema;

    expect(schema.properties.count.bsonType).toBe('long');
    expect(schema.properties.payload.bsonType).toBe('object');
    expect(schema.properties.items.bsonType).toBe('array');
    expect(schema.properties.tags.bsonType).toBe('array');
    expect(schema.properties.enabled.bsonType).toBe('bool');
    expect(schema.properties.score.bsonType).toBe('double');
  });

  it('accepts any BSON shape for the permissive json contract', () => {
    const schema = buildMongoValidationSchema([
      { name: 'uniques', type: 'json', isNullable: true },
      { name: 'defaultValue', type: 'json', isNullable: false },
    ]) as JsonSchema;

    const uniques = schema.properties.uniques.bsonType as string[];
    expect(uniques).toContain('object');
    expect(uniques).toContain('array');
    expect(uniques).toContain('string');
    expect(uniques).toContain('bool');
    expect(uniques).toContain('null');
    expect(uniques).not.toContain('undefined');

    const required = schema.properties.defaultValue.bsonType as string[];
    expect(required).toContain('object');
    expect(required).toContain('array');
    expect(required).not.toContain('null');
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
