import { describe, expect, it } from 'vitest';
import snapshot from '../../src/data/snapshot';
import {
  SnapshotDefinition,
  col,
  rel,
} from '../../src/engines/bootstrap/definitions';
import { buildExpectedRelations } from '../../src/engines/bootstrap/utils/metadata-comparison.util';

describe('SnapshotDefinition', () => {
  it('defines the complete current system target', () => {
    const tables = Object.values(snapshot);

    expect(tables).toHaveLength(43);
    expect(
      tables.reduce((total, table) => total + table.columns.length, 0),
    ).toBe(349);
    expect(
      tables.reduce(
        (total, table) => total + (table.relations?.length ?? 0),
        0,
      ),
    ).toBe(74);
  });

  it('builds table, column, relation, unique, and index contracts', () => {
    const definition = new SnapshotDefinition();

    definition
      .table('posts', {
        description: 'Posts',
        system: true,
        singleRecord: false,
      })
      .columns({
        id: col.int().primary().generated().notNull().system(),
        title: col.varchar().notNull().default('draft').description('Title'),
      })
      .relations({
        owner: rel
          .manyToOne('users')
          .notNull()
          .system()
          .inverse('posts')
          .onDelete('CASCADE'),
      })
      .uniques([['title']])
      .indexes([['owner']]);

    expect(definition.build()).toEqual({
      posts: {
        name: 'posts',
        description: 'Posts',
        isSystem: true,
        isSingleRecord: false,
        columns: [
          {
            name: 'id',
            type: 'int',
            isPrimary: true,
            isGenerated: true,
            isNullable: false,
            isSystem: true,
          },
          {
            name: 'title',
            type: 'varchar',
            isNullable: false,
            defaultValue: 'draft',
            description: 'Title',
          },
        ],
        relations: [
          {
            propertyName: 'owner',
            type: 'many-to-one',
            targetTable: 'users',
            isNullable: false,
            isSystem: true,
            inversePropertyName: 'posts',
            onDelete: 'CASCADE',
          },
        ],
        uniques: [['title']],
        indexes: [['owner']],
      },
    });
  });

  it('keeps menu permission ownership one-way from the permission record to role', () => {
    const relations = buildExpectedRelations(snapshot);

    expect(relations.get('enfyra_role.menuPermissions')).toBeUndefined();
    const roleRelation = relations.get('enfyra_menu_permission.role');
    expect(roleRelation).toMatchObject({
      propertyName: 'role',
      targetTable: 'enfyra_role',
      type: 'many-to-one',
    });
    expect(roleRelation).not.toHaveProperty('inversePropertyName');
  });

  it('rejects duplicate table declarations', () => {
    const definition = new SnapshotDefinition();
    definition.table('posts');

    expect(() => definition.table('posts')).toThrow(
      /Snapshot table posts is already defined/,
    );
  });
});
