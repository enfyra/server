import { describe, expect, it } from 'vitest';
import { parseSnapshotToSchema } from '../../src/engines/knex/utils/provision/schema-parser';

describe('snapshot schema parser', () => {
  it('does not mutate snapshot relations while generating inverse metadata', () => {
    const snapshot = {
      routes: {
        name: 'routes',
        columns: [],
        relations: [
          {
            propertyName: 'methods',
            type: 'many-to-many',
            targetTable: 'methods',
            inversePropertyName: 'routes',
          },
        ],
      },
      methods: {
        name: 'methods',
        columns: [],
        relations: [],
      },
    };
    const before = structuredClone(snapshot);

    const schemas = parseSnapshotToSchema(snapshot);

    expect(snapshot).toEqual(before);
    expect(
      schemas.find((schema) => schema.tableName === 'routes')?.junctionTables,
    ).toHaveLength(1);
    expect(
      schemas.find((schema) => schema.tableName === 'methods')?.definition
        .relations,
    ).toHaveLength(1);
  });

  it('projects mandatory core ownership fields as non-null without rewriting snapshot metadata', () => {
    const snapshot = {
      enfyra_column: {
        name: 'enfyra_column',
        columns: [
          { name: 'isGenerated', type: 'boolean', isNullable: true },
          { name: 'isNullable', type: 'boolean' },
        ],
        relations: [
          {
            propertyName: 'table',
            type: 'many-to-one',
            targetTable: 'enfyra_table',
          },
        ],
      },
      enfyra_relation: {
        name: 'enfyra_relation',
        columns: [],
        relations: [
          {
            propertyName: 'sourceTable',
            type: 'many-to-one',
            targetTable: 'enfyra_table',
          },
        ],
      },
      enfyra_table: {
        name: 'enfyra_table',
        columns: [],
        relations: [],
      },
    };
    const before = structuredClone(snapshot);

    const schemas = parseSnapshotToSchema(snapshot);
    const columnSchema = schemas.find(
      (schema) => schema.tableName === 'enfyra_column',
    )!;
    const relationSchema = schemas.find(
      (schema) => schema.tableName === 'enfyra_relation',
    )!;

    expect(
      columnSchema.definition.columns.find(
        (column) => column.name === 'isGenerated',
      )?.isNullable,
    ).toBe(false);
    expect(
      columnSchema.definition.columns.find(
        (column) => column.name === 'isNullable',
      )?.isNullable,
    ).toBe(false);
    expect(
      columnSchema.definition.relations?.find(
        (relation) => relation.propertyName === 'table',
      )?.isNullable,
    ).toBe(false);
    expect(
      relationSchema.definition.relations?.find(
        (relation) => relation.propertyName === 'sourceTable',
      )?.isNullable,
    ).toBe(false);
    expect(snapshot).toEqual(before);
  });
});
