import { describe, expect, it } from 'vitest';
import {
  validateSnapshotMigrationCoverage,
  validateSnapshotTargetState,
} from '../../src/engines/bootstrap/utils/metadata-migration.util';
import type {
  SchemaMigrationDef,
  SnapshotMigrationMetadataState,
} from '../../src/shared/types/schema-migration.types';

type DatabaseKind = 'postgres' | 'mysql' | 'mongodb';

const snapshot = {
  authors: {
    name: 'authors',
    isSystem: true,
    columns: [
      {
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: true,
      },
    ],
    relations: [],
  },
  posts: {
    name: 'posts',
    isSystem: true,
    indexes: [['title']],
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
        type: 'text',
        isNullable: false,
        isSystem: true,
      },
      {
        name: 'summary',
        type: 'text',
        isSystem: true,
      },
    ],
    relations: [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTable: 'authors',
        isNullable: false,
        isSystem: true,
        inversePropertyName: 'posts',
        onDelete: 'CASCADE',
      },
    ],
  },
};

function metadataState(database: DatabaseKind): SnapshotMigrationMetadataState {
  const json = (value: any) =>
    database === 'mongodb' ? value : JSON.stringify(value);
  return {
    tables: [
      { id: 'authors-id', name: 'authors', isSystem: true },
      {
        id: 'posts-id',
        name: 'posts',
        isSystem: true,
        indexes: json([['legacyTitle']]),
      },
    ],
    columns: [
      {
        id: 'authors-pk',
        tableName: 'authors',
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: true,
      },
      {
        id: 'posts-pk',
        tableName: 'posts',
        name: 'id',
        type: 'int',
        isPrimary: true,
        isGenerated: true,
        isNullable: false,
        isSystem: true,
      },
      {
        id: 'title-id',
        tableName: 'posts',
        name: 'legacyTitle',
        type: 'varchar',
        isNullable: false,
        isSystem: true,
      },
      {
        id: 'obsolete-id',
        tableName: 'posts',
        name: 'obsolete',
        type: 'varchar',
        isSystem: true,
      },
    ],
    relations: [
      {
        id: 'author-relation',
        sourceTableName: 'posts',
        targetTableName: 'authors',
        targetTable: 'authors',
        propertyName: 'writer',
        type: 'many-to-one',
        isNullable: false,
        isSystem: true,
        inversePropertyName: 'writtenPosts',
        onDelete: 'SET NULL',
      },
    ],
  };
}

function completeMigration(): SchemaMigrationDef {
  return {
    tables: [
      {
        _unique: { name: { _eq: 'posts' } },
        tableToModify: {
          from: { indexes: [['legacyTitle']] },
          to: { indexes: [['title']] },
        },
        columnsToModify: [
          {
            from: { name: 'legacyTitle', type: 'varchar' },
            to: {
              name: 'title',
              type: 'text',
              isNullable: false,
              isSystem: true,
            },
          },
        ],
        columnsToRemove: ['obsolete'],
        relationsToModify: [
          {
            from: {
              propertyName: 'writer',
              inversePropertyName: 'writtenPosts',
              onDelete: 'SET NULL',
            },
            to: {
              propertyName: 'author',
              type: 'many-to-one',
              targetTable: 'authors',
              isNullable: false,
              isSystem: true,
              inversePropertyName: 'posts',
              onDelete: 'CASCADE',
            },
          },
        ],
      },
    ],
  };
}

function convergedState(
  database: DatabaseKind,
): SnapshotMigrationMetadataState {
  const state = metadataState(database);
  state.tables[1].indexes =
    database === 'mongodb' ? [['title']] : JSON.stringify([['title']]);
  state.columns = state.columns.filter((column) => column.name !== 'obsolete');
  state.columns[2] = {
    ...state.columns[2],
    name: 'title',
    type: 'text',
  };
  state.columns.push({
    id: 'summary-id',
    tableName: 'posts',
    name: 'summary',
    type: 'text',
    isSystem: true,
  });
  state.relations = [
    {
      id: 'author-relation',
      sourceTableName: 'posts',
      targetTableName: 'authors',
      targetTable: 'authors',
      propertyName: 'author',
      type: 'many-to-one',
      mappedBy: null,
      isNullable: false,
      isSystem: true,
      inversePropertyName: 'posts',
      onDelete: 'CASCADE',
    },
    {
      id: 'posts-relation',
      sourceTableName: 'authors',
      targetTableName: 'posts',
      targetTable: 'posts',
      propertyName: 'posts',
      type: 'one-to-many',
      mappedBy: 'author',
      isNullable: false,
      isSystem: true,
      inversePropertyName: 'author',
      onDelete: 'SET NULL',
    },
  ];
  return state;
}

const tableFieldCases: Array<[string, any, any]> = [
  ['isSystem', true, false],
  ['isSingleRecord', false, true],
  ['uniques', [], [['legacyTitle']]],
  ['indexes', [['title']], [['legacyTitle']]],
  ['alias', null, 'Legacy posts'],
  ['description', null, 'Legacy description'],
  ['metadata', null, { source: 'legacy' }],
  ['validateBody', true, false],
];

const columnFieldCases: Array<[string, any, any]> = [
  ['name', 'title', 'legacyTitle'],
  ['type', 'text', 'varchar'],
  ['isPrimary', false, true],
  ['isGenerated', false, true],
  ['isNullable', false, true],
  ['isSystem', true, false],
  ['isUpdatable', true, false],
  ['isPublished', true, false],
  ['isEncrypted', false, true],
  ['defaultValue', null, 'legacy'],
  ['options', null, ['legacy']],
  ['description', null, 'Legacy description'],
  ['placeholder', null, 'Legacy placeholder'],
];

const relationFieldCases: Array<[string, any, any]> = [
  ['propertyName', 'author', 'writer'],
  ['type', 'many-to-one', 'one-to-one'],
  ['targetTable', 'authors', 'legacy_authors'],
  ['mappedBy', null, 'legacyOwner'],
  ['inversePropertyName', 'posts', 'writtenPosts'],
  ['isNullable', false, true],
  ['isSystem', true, false],
  ['isUpdatable', true, false],
  ['isPublished', true, false],
  ['onDelete', 'CASCADE', 'SET NULL'],
  ['description', null, 'Legacy description'],
];

function persistedValue(
  database: DatabaseKind,
  field: string,
  value: any,
): any {
  if (
    database !== 'mongodb' &&
    ['uniques', 'indexes', 'metadata', 'defaultValue', 'options'].includes(
      field,
    )
  ) {
    return JSON.stringify(value);
  }
  return value;
}

describe.each<DatabaseKind>(['postgres', 'mysql', 'mongodb'])(
  'snapshot migration contract on %s',
  (database) => {
    it('allows additive snapshot fields without a migration declaration', () => {
      const state = metadataState(database);
      state.tables[1].indexes =
        database === 'mongodb' ? [['title']] : JSON.stringify([['title']]);
      state.columns = state.columns.filter(
        (column) => column.name !== 'obsolete',
      );
      state.columns[2] = {
        ...state.columns[2],
        name: 'title',
        type: 'text',
      };
      state.relations[0] = {
        ...state.relations[0],
        propertyName: 'author',
        inversePropertyName: 'posts',
        onDelete: 'CASCADE',
      };

      expect(() =>
        validateSnapshotMigrationCoverage(snapshot, null, state),
      ).not.toThrow();
    });

    it('rejects undeclared updates and removals', () => {
      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          null,
          metadataState(database),
        ),
      ).toThrow(/missing non-additive declarations/);
    });

    it('accepts comprehensive table, column, relation, and removal declarations', () => {
      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          completeMigration(),
          metadataState(database),
        ),
      ).not.toThrow();
    });

    it('rejects a partial update declaration', () => {
      const migration = completeMigration();
      delete migration.tables[0].columnsToModify?.[0].to.type;

      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          migration,
          metadataState(database),
        ),
      ).toThrow(/does not fully declare target fields: type/);
    });

    it('rejects a declaration whose target disagrees with snapshot.ts', () => {
      const migration = completeMigration();
      migration.tables[0].relationsToModify![0].to.onDelete = 'RESTRICT';

      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          migration,
          metadataState(database),
        ),
      ).toThrow(/does not fully declare target fields: onDelete/);
    });

    it('rejects a column rename whose target is absent from snapshot.ts', () => {
      const migration = completeMigration();
      migration.tables[0].columnsToModify![0].to.name = 'headline';

      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          migration,
          metadataState(database),
        ),
      ).toThrow(/column posts\.headline does not exist in snapshot\.ts/);
    });

    it('rejects a relation rename whose target is absent from snapshot.ts', () => {
      const migration = completeMigration();
      migration.tables[0].relationsToModify![0].to.propertyName = 'editor';

      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          migration,
          metadataState(database),
        ),
      ).toThrow(/relation posts\.editor does not exist in snapshot\.ts/);
    });

    it('rejects a migration whose declared source does not match the old state', () => {
      const migration = completeMigration();
      migration.tables[0].columnsToModify![0].from.type = 'int';

      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          migration,
          metadataState(database),
        ),
      ).toThrow(/source does not match current fields: type/);
    });

    it('allows an explicitly removed relation to be recreated under the same property name', () => {
      const state = convergedState(database);
      state.relations[0] = {
        ...state.relations[0],
        type: 'one-to-one',
      };
      state.relations = state.relations.slice(0, 1);
      const migration: SchemaMigrationDef = {
        tables: [
          {
            _unique: { name: { _eq: 'posts' } },
            relationsToRemove: ['author'],
          },
        ],
      };

      expect(() =>
        validateSnapshotMigrationCoverage(snapshot, migration, state),
      ).not.toThrow();
    });

    it('requires tablesToDrop only for removed system tables', () => {
      const state = convergedState(database);
      state.tables.push(
        { id: 'legacy-system', name: 'legacy_system', isSystem: true },
        { id: 'custom-table', name: 'custom_table', isSystem: false },
      );

      expect(() =>
        validateSnapshotMigrationCoverage(snapshot, null, state),
      ).toThrow(/legacy_system is removed without tablesToDrop/);
      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          { tables: [], tablesToDrop: ['legacy_system'] },
          state,
        ),
      ).not.toThrow();
    });

    it('is idempotent when the database already matches the target', () => {
      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          completeMigration(),
          convergedState(database),
        ),
      ).not.toThrow();
    });

    it('accepts either the snapshot value or an intentional data-migration metadata target', () => {
      const dataTargetSnapshot = structuredClone(snapshot);
      dataTargetSnapshot.posts.validateBody = false;
      dataTargetSnapshot.posts.columns[1].description =
        'Intentional data target';
      const snapshotState = convergedState(database);
      const dataTargetState = convergedState(database);
      dataTargetState.tables[1].validateBody = false;
      dataTargetState.columns[2].description = 'Intentional data target';

      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          completeMigration(),
          snapshotState,
          dataTargetSnapshot,
        ),
      ).not.toThrow();
      expect(() =>
        validateSnapshotMigrationCoverage(
          snapshot,
          completeMigration(),
          dataTargetState,
          dataTargetSnapshot,
        ),
      ).not.toThrow();
      expect(() =>
        validateSnapshotTargetState(
          snapshot,
          snapshotState,
          completeMigration(),
          dataTargetSnapshot,
        ),
      ).not.toThrow();
      expect(() =>
        validateSnapshotTargetState(
          snapshot,
          dataTargetState,
          completeMigration(),
          dataTargetSnapshot,
        ),
      ).not.toThrow();
    });

    it('requires healing to converge completely to the new snapshot state', () => {
      expect(() =>
        validateSnapshotTargetState(snapshot, metadataState(database)),
      ).toThrow(/did not converge/);
      expect(() =>
        validateSnapshotTargetState(snapshot, convergedState(database)),
      ).not.toThrow();
    });

    it('preserves undeclared user metadata beside system snapshot metadata', () => {
      const state = convergedState(database);
      state.tables.push({
        id: 'custom-table',
        name: 'custom_table',
        isSystem: false,
      });
      state.columns.push({
        id: 'custom-column',
        tableName: 'posts',
        name: 'custom_column',
        type: 'text',
        isSystem: false,
      });
      state.relations.push({
        id: 'custom-relation',
        sourceTableName: 'posts',
        targetTableName: 'authors',
        targetTable: 'authors',
        propertyName: 'custom_author',
        type: 'many-to-one',
        isSystem: false,
      });

      expect(() =>
        validateSnapshotMigrationCoverage(snapshot, completeMigration(), state),
      ).not.toThrow();
      expect(() =>
        validateSnapshotTargetState(snapshot, state, completeMigration()),
      ).not.toThrow();
    });

    it('still requires explicitly removed metadata to be absent', () => {
      const state = convergedState(database);
      state.columns.push({
        id: 'obsolete-user-column',
        tableName: 'posts',
        name: 'obsolete',
        type: 'text',
        isSystem: false,
      });

      expect(() =>
        validateSnapshotTargetState(snapshot, state, completeMigration()),
      ).toThrow(/column posts\.obsolete still exists/);
    });

    it('rejects duplicate metadata records instead of hiding them in maps', () => {
      const state = convergedState(database);
      state.columns.push({ ...state.columns[2], id: 'duplicate-title' });
      state.relations.push({
        ...state.relations[0],
        id: 'duplicate-author',
      });

      expect(() => validateSnapshotTargetState(snapshot, state)).toThrow(
        /duplicate column posts\.title/,
      );
      expect(() => validateSnapshotTargetState(snapshot, state)).toThrow(
        /duplicate relation posts\.author/,
      );
    });

    it('requires explicit physical relation mapping and metadata updates', () => {
      const physicalSnapshot = structuredClone(snapshot);
      Object.assign(physicalSnapshot.posts.relations[0], {
        foreignKeyColumn: 'author_id',
        referencedColumn: 'author_key',
        constraintName: 'fk_posts_author',
        metadata: { source: 'snapshot' },
      });
      const state = convergedState(database);
      Object.assign(state.relations[0], {
        foreignKeyColumn: 'legacy_author_id',
        referencedColumn: 'id',
        constraintName: 'fk_posts_legacy_author',
        metadata:
          database === 'mongodb'
            ? { source: 'legacy' }
            : JSON.stringify({ source: 'legacy' }),
      });

      expect(() =>
        validateSnapshotMigrationCoverage(physicalSnapshot, null, state),
      ).toThrow(
        /updates foreignKeyColumn, referencedColumn, constraintName, metadata without migration/,
      );

      const migration: SchemaMigrationDef = {
        tables: [
          {
            _unique: { name: { _eq: 'posts' } },
            relationsToModify: [
              {
                from: {
                  propertyName: 'author',
                  foreignKeyColumn: 'legacy_author_id',
                  referencedColumn: 'id',
                  constraintName: 'fk_posts_legacy_author',
                  metadata: { source: 'legacy' },
                },
                to: {
                  propertyName: 'author',
                  foreignKeyColumn: 'author_id',
                  referencedColumn: 'author_key',
                  constraintName: 'fk_posts_author',
                  metadata: { source: 'snapshot' },
                },
              },
            ],
          },
        ],
      };
      expect(() =>
        validateSnapshotMigrationCoverage(physicalSnapshot, migration, state),
      ).not.toThrow();

      expect(() =>
        validateSnapshotTargetState(physicalSnapshot, state),
      ).toThrow(
        /differs on foreignKeyColumn, referencedColumn, constraintName, metadata/,
      );
    });
  },
);

describe.each<DatabaseKind>(['postgres', 'mysql', 'mongodb'])(
  'comprehensive update field matrix on %s',
  (database) => {
    it.each(tableFieldCases)(
      'requires and accepts a declared table %s update',
      (field, target, current) => {
        const state = convergedState(database);
        state.tables[1][field] = persistedValue(database, field, current);

        expect(() =>
          validateSnapshotMigrationCoverage(snapshot, null, state),
        ).toThrow(new RegExp(`table posts updates .*${field}`));
        expect(() =>
          validateSnapshotMigrationCoverage(
            snapshot,
            {
              tables: [
                {
                  _unique: { name: { _eq: 'posts' } },
                  tableToModify: {
                    from: { [field]: current },
                    to: { [field]: target },
                  },
                },
              ],
            },
            state,
          ),
        ).not.toThrow();
      },
    );

    it.each(columnFieldCases)(
      'requires and accepts a declared column %s update',
      (field, target, current) => {
        const state = convergedState(database);
        const column = state.columns.find(
          (entry) => entry.tableName === 'posts' && entry.name === 'title',
        )!;
        column[field] = persistedValue(database, field, current);

        expect(() =>
          validateSnapshotMigrationCoverage(snapshot, null, state),
        ).toThrow(
          field === 'name'
            ? /column posts\.legacyTitle is removed without migration/
            : new RegExp(`column posts\\..* updates .*${field}`),
        );
        expect(() =>
          validateSnapshotMigrationCoverage(
            snapshot,
            {
              tables: [
                {
                  _unique: { name: { _eq: 'posts' } },
                  columnsToModify: [
                    {
                      from: {
                        name: field === 'name' ? current : 'title',
                        [field]: current,
                      },
                      to: {
                        name: 'title',
                        [field]: target,
                      },
                    },
                  ],
                },
              ],
            },
            state,
          ),
        ).not.toThrow();
      },
    );

    it.each(relationFieldCases)(
      'requires and accepts a declared relation %s update',
      (field, target, current) => {
        const state = convergedState(database);
        const relation = state.relations.find(
          (entry) =>
            entry.sourceTableName === 'posts' &&
            entry.propertyName === 'author',
        )!;
        relation[field] = persistedValue(database, field, current);
        if (field === 'targetTable') {
          relation.targetTableName = current;
        }

        expect(() =>
          validateSnapshotMigrationCoverage(snapshot, null, state),
        ).toThrow(
          field === 'propertyName'
            ? /relation posts\.writer is removed without migration/
            : new RegExp(`relation posts\\..* updates .*${field}`),
        );
        expect(() =>
          validateSnapshotMigrationCoverage(
            snapshot,
            {
              tables: [
                {
                  _unique: { name: { _eq: 'posts' } },
                  relationsToModify: [
                    {
                      from: {
                        propertyName:
                          field === 'propertyName' ? current : 'author',
                        [field]: current,
                      },
                      to: {
                        propertyName: 'author',
                        [field]: target,
                      },
                    },
                  ],
                },
              ],
            },
            state,
          ),
        ).not.toThrow();
      },
    );
  },
);

describe('snapshot migration declaration validation', () => {
  it('rejects duplicate table migration blocks', () => {
    const migration = completeMigration();
    migration.tables.push({
      _unique: { name: { _eq: 'posts' } },
      columnsToRemove: ['obsolete'],
    });

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate table migration posts/);
  });

  it('rejects migration blocks for tables absent from the target snapshot', () => {
    const migration = completeMigration();
    migration.tables.push({
      _unique: { name: { _eq: 'legacy_posts' } },
      columnsToRemove: ['obsolete'],
    });

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/table migration legacy_posts does not exist in snapshot\.ts/);
  });

  it('rejects tablesToDrop entries that still exist in the target snapshot', () => {
    const migration = completeMigration();
    migration.tablesToDrop = ['authors'];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/tablesToDrop authors still exists in snapshot\.ts/);
  });

  it('rejects stale table targets even when the database already converged', () => {
    const migration = completeMigration();
    migration.tables[0].tableToModify!.to.indexes = [['headline']];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        convergedState('postgres'),
      ),
    ).toThrow(/table posts migration target disagrees with snapshot\.ts/);
  });

  it('rejects duplicate destructive declarations', () => {
    const migration = completeMigration();
    migration.tablesToDrop = ['legacy_posts', 'legacy_posts'];
    migration.physicalTablesToDrop = ['legacy_files', 'legacy_files'];
    migration.tables[0].columnsToRemove = ['obsolete', 'obsolete'];
    migration.tables[0].relationsToRemove = ['legacyAuthor', 'legacyAuthor'];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate tablesToDrop legacy_posts/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate physicalTablesToDrop legacy_files/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate column removal posts\.obsolete/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate relation removal posts\.legacyAuthor/);
  });

  it('rejects removing a column that remains in the target snapshot', () => {
    const migration = completeMigration();
    migration.tables[0].columnsToRemove = ['obsolete', 'title'];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/column removal posts\.title still exists in snapshot\.ts/);
  });

  it('rejects a column declared for both update and removal', () => {
    const migration = completeMigration();
    migration.tables[0].columnsToRemove = ['obsolete', 'legacyTitle'];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(
      /column posts\.legacyTitle is declared for both modification and removal/,
    );
  });

  it('rejects a relation declared for both update and removal', () => {
    const migration = completeMigration();
    migration.tables[0].relationsToRemove = ['writer'];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(
      /relation posts\.writer is declared for both modification and removal/,
    );
  });

  it('rejects target relations that reference a missing table', () => {
    const invalidSnapshot = structuredClone(snapshot);
    invalidSnapshot.posts.relations[0].targetTable = 'missing_authors';

    expect(() =>
      validateSnapshotMigrationCoverage(
        invalidSnapshot,
        completeMigration(),
        metadataState('postgres'),
      ),
    ).toThrow(/relation posts\.author targets missing table missing_authors/);
  });

  it('rejects invalid metadata table rename graphs', () => {
    const migration = completeMigration();
    migration.tablesToRename = [
      { from: 'legacy_posts', to: 'posts' },
      { from: 'legacy_posts', to: 'authors' },
      { from: 'legacy_articles', to: 'posts' },
      { from: 'authors', to: 'authors' },
      { from: 'missing_source', to: 'missing_target' },
    ];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate tablesToRename source legacy_posts/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate tablesToRename target posts/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/tablesToRename authors cannot rename a table to itself/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/tablesToRename source authors still exists in snapshot\.ts/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(
      /tablesToRename target missing_target does not exist in snapshot\.ts/,
    );
  });

  it('rejects invalid physical rename and drop overlap', () => {
    const migration = completeMigration();
    migration.physicalTablesToRename = [
      { from: 'legacy_files', to: 'files' },
      { from: 'legacy_files', to: 'assets' },
      { from: 'legacy_assets', to: 'files' },
      { from: 'same', to: 'same' },
    ];
    migration.physicalTablesToDrop = ['legacy_files', 'files'];

    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate physicalTablesToRename source legacy_files/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/duplicate physicalTablesToRename target files/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/physicalTablesToRename same cannot rename a table to itself/);
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(
      /physical table legacy_files is declared for both rename and drop/,
    );
    expect(() =>
      validateSnapshotMigrationCoverage(
        snapshot,
        migration,
        metadataState('postgres'),
      ),
    ).toThrow(/physical table files is declared for both rename and drop/);
  });
});

describe('snapshot target definition validation', () => {
  it('rejects duplicate snapshot columns before maps can hide them', () => {
    const invalidSnapshot = structuredClone(snapshot);
    invalidSnapshot.posts.columns.push({
      ...invalidSnapshot.posts.columns[1],
    });

    expect(() =>
      validateSnapshotTargetState(invalidSnapshot, convergedState('postgres')),
    ).toThrow(/duplicate snapshot column posts\.title/);
  });

  it('rejects duplicate snapshot relations before maps can hide them', () => {
    const invalidSnapshot = structuredClone(snapshot);
    invalidSnapshot.posts.relations.push({
      ...invalidSnapshot.posts.relations[0],
    });

    expect(() =>
      validateSnapshotTargetState(invalidSnapshot, convergedState('postgres')),
    ).toThrow(/duplicate snapshot relation posts\.author/);
  });

  it('rejects snapshot table names that disagree with their object key', () => {
    const invalidSnapshot = structuredClone(snapshot);
    invalidSnapshot.posts.name = 'articles';

    expect(() =>
      validateSnapshotTargetState(invalidSnapshot, convergedState('postgres')),
    ).toThrow(/snapshot table key posts disagrees with name articles/);
  });

  it('rejects target relations that reference a missing table', () => {
    const invalidSnapshot = structuredClone(snapshot);
    invalidSnapshot.posts.relations[0].targetTable = 'missing_authors';

    expect(() =>
      validateSnapshotTargetState(invalidSnapshot, convergedState('postgres')),
    ).toThrow(/relation posts\.author targets missing table missing_authors/);
  });
});

describe('snapshot generated inverse relation contract', () => {
  it('treats one-to-many snapshot declarations as inverse metadata', () => {
    const relationSnapshot = {
      teams: {
        name: 'teams',
        isSystem: true,
        columns: [],
        relations: [
          {
            propertyName: 'members',
            type: 'one-to-many',
            targetTable: 'users',
            inversePropertyName: 'team',
            isSystem: true,
          },
        ],
      },
      users: {
        name: 'users',
        isSystem: true,
        columns: [],
        relations: [
          {
            propertyName: 'team',
            type: 'many-to-one',
            targetTable: 'teams',
            inversePropertyName: 'members',
            isSystem: true,
            onDelete: 'CASCADE',
            description: 'Owning team',
          },
        ],
      },
    };
    const state: SnapshotMigrationMetadataState = {
      tables: [
        { id: 'teams-id', name: 'teams', isSystem: true },
        { id: 'users-id', name: 'users', isSystem: true },
      ],
      columns: [],
      relations: [
        {
          sourceTableName: 'teams',
          targetTableName: 'users',
          targetTable: 'users',
          propertyName: 'members',
          type: 'one-to-many',
          mappedBy: 'team',
          inversePropertyName: 'team',
          isSystem: true,
        },
        {
          sourceTableName: 'users',
          targetTableName: 'teams',
          targetTable: 'teams',
          propertyName: 'team',
          type: 'many-to-one',
          mappedBy: null,
          inversePropertyName: 'members',
          isSystem: true,
          onDelete: 'CASCADE',
          description: 'Owning team',
        },
      ],
    };

    expect(() =>
      validateSnapshotTargetState(relationSnapshot, state),
    ).not.toThrow();
  });
});
