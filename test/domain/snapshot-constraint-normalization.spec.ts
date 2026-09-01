import { describe, expect, it } from 'vitest';
import { MetadataMigrationService } from '../../src/engines/bootstrap/services/metadata-migration.service';
import {
  validateSnapshotMigrationCoverage,
  validateSnapshotTargetState,
} from '../../src/engines/bootstrap/utils/metadata-migration.util';

describe.each(['postgresql', 'mysql', 'mongodb'] as const)(
  'auto-managed snapshot index comparison (%s)',
  (backend) => {
    const snapshot = {
      enfyra_user: {
        name: 'enfyra_user',
        isSystem: true,
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
            isGenerated: true,
            isNullable: false,
            isSystem: true,
          },
          {
            name: 'email',
            type: 'varchar',
            isNullable: false,
            isSystem: true,
          },
        ],
        relations: [],
        uniques: [['email']],
      },
    };

    function convergedState() {
      const persist = (value: unknown) =>
        backend === 'mongodb' ? value : JSON.stringify(value);
      return {
        tables: [
          {
            id: 'user-table',
            name: 'enfyra_user',
            isSystem: true,
            uniques: persist([['email']]),
            indexes: persist([['createdAt'], ['updatedAt']]),
          },
        ],
        columns: snapshot.enfyra_user.columns.map((column, index) => ({
          ...column,
          id: `column-${index}`,
          tableName: 'enfyra_user',
        })),
        relations: [],
      };
    }

    it('accepts generated timestamp index metadata without a migration', () => {
      const state = convergedState();

      expect(() =>
        validateSnapshotMigrationCoverage(snapshot, null, state),
      ).not.toThrow();
      expect(() =>
        validateSnapshotTargetState(snapshot, state, null),
      ).not.toThrow();
    });

    it('still rejects a non-canonical system index without a migration', () => {
      const state = convergedState();
      state.tables[0].indexes =
        backend === 'mongodb'
          ? [['createdAt'], ['updatedAt'], ['email']]
          : JSON.stringify([['createdAt'], ['updatedAt'], ['email']]);

      expect(() =>
        validateSnapshotMigrationCoverage(snapshot, null, state),
      ).toThrow(/updates indexes without migration/);
    });
  },
);

describe('metadata migration auto-index guard', () => {
  it('normalizes generated timestamp indexes before live coverage validation', async () => {
    const snapshot = {
      enfyra_user: {
        name: 'enfyra_user',
        isSystem: true,
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
            isGenerated: true,
            isNullable: false,
            isSystem: true,
          },
        ],
        relations: [],
      },
    };
    const rows: Record<string, any[]> = {
      enfyra_table: [
        {
          id: 1,
          name: 'enfyra_user',
          isSystem: true,
          indexes: JSON.stringify([['createdAt'], ['updatedAt']]),
        },
      ],
      enfyra_column: snapshot.enfyra_user.columns.map((column, index) => ({
        ...column,
        id: index + 1,
        tableId: 1,
      })),
      enfyra_relation: [],
    };
    const knex = Object.assign(
      (tableName: string) => ({ select: async () => rows[tableName] ?? [] }),
      {
        client: { config: { client: 'pg' } },
        schema: { hasTable: async () => true },
      },
    );
    const service = new MetadataMigrationService({
      queryBuilderService: {
        isMongoDb: () => false,
        getKnex: () => knex,
      } as any,
      systemCoreTableResolver: {
        getNames: async () => ({
          table: 'enfyra_table',
          column: 'enfyra_column',
          relation: 'enfyra_relation',
        }),
      } as any,
      bootstrapDefinitionService: {
        getSnapshot: () => snapshot,
        getDataTargetSnapshot: () => snapshot,
        getMigration: () => null,
      } as any,
    });

    await expect(
      service.prepareMigrationExecutionPlan(),
    ).resolves.toMatchObject({
      mode: 'upgrade',
      database: 'postgresql',
    });
  });
});
