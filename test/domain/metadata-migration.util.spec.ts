import { describe, expect, it } from 'vitest';
import {
  buildColumnMetadataUpdate,
  buildRelationMetadataUpdate,
  buildTableMetadataUpdate,
  hasColumnMetadataChanges,
  hasRelationMetadataChanges,
  validateSnapshotMigrationDefinition,
} from '../../src/engines/bootstrap/utils/metadata-migration.util';

describe('metadata migration update contract', () => {
  it('copies every declared target column property into the metadata update', () => {
    const migration = {
      from: {
        name: 'status',
        type: 'varchar',
        isNullable: true,
        defaultValue: null,
      },
      to: {
        name: 'state',
        type: 'enum',
        options: ['draft', 'published'],
        isPrimary: false,
        isGenerated: false,
        isNullable: false,
        isSystem: true,
        isUpdatable: false,
        isPublished: false,
        isEncrypted: true,
        defaultValue: 'draft',
        description: 'Publication state',
        placeholder: 'Choose a state',
      },
    };

    expect(hasColumnMetadataChanges(migration)).toBe(true);
    expect(buildColumnMetadataUpdate(migration)).toEqual(migration.to);
  });

  it('copies logical relation properties without overwriting persisted physical mappings', () => {
    const migration = {
      from: {
        propertyName: 'owner',
        isNullable: true,
        onDelete: 'SET NULL',
      },
      to: {
        propertyName: 'author',
        type: 'many-to-one',
        isNullable: false,
        isSystem: true,
        isUpdatable: false,
        isPublished: false,
        onDelete: 'CASCADE',
        description: 'Owning author',
        foreignKeyColumn: 'author_id',
        referencedColumn: 'id',
        constraintName: 'fk_posts_author',
      },
    };

    expect(hasRelationMetadataChanges(migration)).toBe(true);
    expect(buildRelationMetadataUpdate(migration)).toEqual({
      propertyName: 'author',
      type: 'many-to-one',
      isNullable: false,
      isSystem: true,
      isUpdatable: false,
      isPublished: false,
      onDelete: 'CASCADE',
      description: 'Owning author',
    });
  });

  it('copies every declared target table property into the metadata update', () => {
    const migration = {
      from: {
        indexes: [['legacy']],
        uniques: [['legacy']],
      },
      to: {
        isSystem: true,
        isSingleRecord: false,
        indexes: [['title']],
        uniques: [['slug']],
        alias: 'Posts',
        description: 'Post records',
        metadata: { source: 'snapshot' },
        validateBody: true,
      },
    };

    expect(buildTableMetadataUpdate(migration)).toEqual(migration.to);
  });

  it('fails fast when snapshot migration targets are invalid', () => {
    expect(() =>
      validateSnapshotMigrationDefinition(
        {
          current: { name: 'current', columns: [], relations: [] },
        },
        {
          tables: [],
          tablesToRename: [{ from: 'legacy', to: 'missing' }],
        },
      ),
    ).toThrow(/target missing does not exist in snapshot\.ts/);
  });

  it('rejects derived relation physical fields in migration declarations', () => {
    expect(() =>
      validateSnapshotMigrationDefinition(
        {
          posts: {
            name: 'posts',
            columns: [],
            relations: [
              {
                propertyName: 'tags',
                type: 'many-to-many',
                targetTable: 'tags',
                junctionTableName: 'j_target',
                junctionSourceColumn: 'sourceId',
                junctionTargetColumn: 'targetId',
              },
            ],
          },
          tags: { name: 'tags', columns: [], relations: [] },
        },
        {
          tables: [
            {
              _unique: { name: { _eq: 'posts' } },
              relationsToModify: [
                {
                  from: {
                    propertyName: 'tags',
                    junctionTableName: 'j_legacy',
                  },
                  to: {
                    propertyName: 'tags',
                    junctionTableName: 'j_target',
                  },
                },
              ],
            },
          ],
        },
      ),
    ).toThrow(/must not declare derived physical fields: junctionTableName/);
  });
});
