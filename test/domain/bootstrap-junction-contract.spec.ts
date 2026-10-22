import { getJunctionTableName } from '@enfyra/kernel';
import { describe, expect, it } from 'vitest';
import snapshot from '../../src/data/snapshot';
import { validateBootstrapDataFiles } from '../../src/domain/bootstrap/utils/bootstrap-data-validator.util';
import { rel } from '../../src/engines/bootstrap/definitions';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap/services/bootstrap-definition.service';
import { parseSnapshotToSchema } from '../../src/engines/knex/utils/provision/schema-parser';
import { getSqlJunctionPhysicalNames } from '../../src/modules/table-management/utils/sql-junction-naming.util';

const PHYSICAL_JUNCTION_FIELDS = [
  'junctionTableName',
  'junctionSourceColumn',
  'junctionTargetColumn',
];

describe('system-owned bootstrap junction contract', () => {
  it('does not expose physical junction authoring in the snapshot builder', () => {
    expect(rel.manyToMany('roles')).not.toHaveProperty('junction');
    for (const table of Object.values(snapshot)) {
      for (const relation of table.relations ?? []) {
        for (const field of PHYSICAL_JUNCTION_FIELDS)
          expect(relation).not.toHaveProperty(field);
      }
    }
  });

  it.each(PHYSICAL_JUNCTION_FIELDS)(
    'rejects raw snapshot authoring of %s before mutation',
    (field) => {
      const issues = validateBootstrapDataFiles({
        snapshot: {
          users: {
            name: 'users',
            columns: [],
            relations: [
              {
                propertyName: 'roles',
                type: 'many-to-many',
                targetTable: 'roles',
                [field]: 'custom',
              },
            ],
          },
          roles: { name: 'roles', columns: [] },
        },
        defaultData: {},
        dataMigration: {},
      });
      expect(issues).toContainEqual(
        expect.objectContaining({
          file: 'snapshot.ts',
          table: 'users',
          field,
          message: expect.stringMatching(/system-generated/),
        }),
      );
    },
  );

  it('uses the canonical hashed contract for every fresh SQL junction', () => {
    const target = new BootstrapDefinitionService().getSnapshot();
    const junctions = parseSnapshotToSchema(target).flatMap(
      (schema) => schema.junctionTables,
    );
    expect(junctions.length).toBeGreaterThan(0);
    for (const junction of junctions) {
      const expected = getSqlJunctionPhysicalNames({
        sourceTable: junction.sourceTable,
        propertyName: junction.sourcePropertyName,
        targetTable: junction.targetTable,
      });
      expect(junction).toMatchObject({
        tableName: expected.junctionTableName,
        sourceColumn: expected.junctionSourceColumn,
        targetColumn: expected.junctionTargetColumn,
      });
    }
  });

  it('keeps self-referencing junction columns distinct', () => {
    const [schema] = parseSnapshotToSchema({
      users: {
        name: 'users',
        columns: [],
        relations: [
          {
            propertyName: 'friends',
            type: 'many-to-many',
            targetTable: 'users',
          },
        ],
      },
    });
    expect(schema.junctionTables).toEqual([
      expect.objectContaining({
        tableName: getJunctionTableName('users', 'friends', 'users'),
        sourceColumn: 'sourceId',
        targetColumn: 'targetId',
      }),
    ]);
  });

  it('does not create a separate junction for a mapped inverse relation', () => {
    const schemas = parseSnapshotToSchema({
      users: {
        name: 'users',
        columns: [],
        relations: [
          {
            propertyName: 'roles',
            type: 'many-to-many',
            targetTable: 'roles',
            inversePropertyName: 'users',
          },
        ],
      },
      roles: {
        name: 'roles',
        columns: [],
        relations: [
          {
            propertyName: 'users',
            type: 'many-to-many',
            targetTable: 'users',
            mappedBy: 'roles',
          },
        ],
      },
    });
    expect(schemas.flatMap((schema) => schema.junctionTables)).toHaveLength(1);
    expect(schemas[1].junctionTables).toEqual([]);
  });

  it('shares the kernel hash implementation including unambiguous tuple encoding', () => {
    const input = { sourceTable: 'a:b', propertyName: 'c', targetTable: 'd' };
    expect(getSqlJunctionPhysicalNames(input).junctionTableName).toBe(
      getJunctionTableName(
        input.sourceTable,
        input.propertyName,
        input.targetTable,
      ),
    );
  });
});
