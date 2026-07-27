import { describe, expect, it } from 'vitest';
import { applyDataMigrationMetadataTargets } from '../../src/engines/bootstrap/utils/data-migration-target.util';

describe('data migration metadata targets', () => {
  it('projects exact table, column, and relation targets without mutating the snapshot', () => {
    const snapshot = {
      posts: {
        name: 'posts',
        validateBody: true,
        columns: [
          { name: 'status', type: 'enum', options: ['draft'] },
          { name: 'title', type: 'text' },
        ],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTable: 'authors',
          },
        ],
      },
    };
    const before = structuredClone(snapshot);

    const target = applyDataMigrationMetadataTargets(snapshot, {
      enfyra_table: {
        _unique: { name: { _eq: 'posts' } },
        validateBody: false,
      },
      enfyra_column: [
        {
          _unique: {
            _and: [
              { table: { name: { _eq: 'posts' } } },
              { name: { _eq: 'status' } },
            ],
          },
          options: ['draft', 'published'],
        },
        {
          _unique: {
            table: { name: { _eq: 'missing' } },
            name: { _eq: 'title' },
          },
          description: 'ignored',
        },
      ],
      enfyra_relation: {
        _unique: {
          sourceTable: { name: { _eq: 'posts' } },
          propertyName: { _eq: 'author' },
        },
        description: 'Intentional relation target',
      },
    });

    expect(target.posts.validateBody).toBe(false);
    expect(target.posts.columns[0].options).toEqual(['draft', 'published']);
    expect(target.posts.relations[0].description).toBe(
      'Intentional relation target',
    );
    expect(snapshot).toEqual(before);
  });

  it('ignores non-exact selectors instead of guessing a metadata target', () => {
    const snapshot = {
      posts: {
        name: 'posts',
        columns: [{ name: 'title', type: 'text' }],
        relations: [],
      },
    };

    const target = applyDataMigrationMetadataTargets(snapshot, {
      enfyra_table: {
        _unique: { name: { _in: ['posts'] } },
        validateBody: false,
      },
      enfyra_column: {
        _unique: {
          _and: [
            { table: { name: { _eq: 'posts' } } },
            { name: { _in: ['title'] } },
          ],
        },
        description: 'ignored',
      },
    });

    expect(target).toEqual(snapshot);
  });
});
