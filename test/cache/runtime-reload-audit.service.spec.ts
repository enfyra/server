import { describe, expect, it, vi } from 'vitest';
import { RuntimeReloadAuditService } from '../../src/engines/cache';

function createSqlQueryBuilder(hasTableResults: boolean[]) {
  const rows: any[] = [];
  const knex = vi.fn((tableName: string) => ({
    where(filter: Record<string, any>) {
      return {
        first: vi.fn(async () =>
          rows.find((row) =>
            Object.entries(filter).every(([key, value]) => row[key] === value),
          ),
        ),
        update: vi.fn(async (patch: Record<string, any>) => {
          for (const row of rows) {
            if (
              Object.entries(filter).every(([key, value]) => row[key] === value)
            ) {
              Object.assign(row, patch);
            }
          }
        }),
      };
    },
    whereIn(column: string, values: any[]) {
      return {
        update: vi.fn(async (patch: Record<string, any>) => {
          for (const row of rows) {
            if (values.includes(row[column])) {
              Object.assign(row, patch);
            }
          }
        }),
      };
    },
    insert: vi.fn(async (row: Record<string, any>) => {
      rows.push({ ...row, __tableName: tableName });
    }),
  })) as any;
  knex.schema = {
    hasTable: vi.fn(async () => hasTableResults.shift() ?? true),
  };

  return {
    rows,
    queryBuilderService: {
      isMongoDb: () => false,
      getKnex: () => knex,
    } as any,
    hasTable: knex.schema.hasTable,
  };
}

describe('RuntimeReloadAuditService', () => {
  it('rechecks availability after the audit table appears and records terminal status', async () => {
    const { rows, queryBuilderService, hasTable } = createSqlQueryBuilder([
      false,
      true,
    ]);
    const service = new RuntimeReloadAuditService({ queryBuilderService });

    await expect(
      service.markBuilding({
        reloadId: 'reload-1',
        flow: 'route',
        table: 'enfyra_route',
        scope: 'full',
        action: 'reload',
        chain: ['route'],
      }),
    ).resolves.toBe(false);

    await expect(
      service.markBuilding({
        reloadId: 'reload-1',
        flow: 'route',
        table: 'enfyra_route',
        scope: 'full',
        action: 'reload',
        chain: ['route'],
      }),
    ).resolves.toBe(true);

    await service.markActivated({
      reloadId: 'reload-1',
      durationMs: 12,
      steps: [{ name: 'route', durationMs: 12, status: 'success' }],
    });

    expect(hasTable).toHaveBeenCalledTimes(2);
    expect(rows).toHaveLength(1);
    expect(rows[0]).toEqual(
      expect.objectContaining({
        reloadId: 'reload-1',
        status: 'activated',
        durationMs: 12,
        errorMessage: null,
      }),
    );
    expect(JSON.parse(rows[0].steps)).toEqual([
      { name: 'route', durationMs: 12, status: 'success' },
    ]);
  });

  it('marks interrupted reload rows as failed', async () => {
    const { rows, queryBuilderService } = createSqlQueryBuilder([true]);
    rows.push(
      { reloadId: 'reload-1', status: 'building', errorMessage: null },
      { reloadId: 'reload-2', status: 'pending', errorMessage: null },
      { reloadId: 'reload-3', status: 'activated', errorMessage: null },
    );
    const service = new RuntimeReloadAuditService({ queryBuilderService });

    await service.markInterruptedReloadsFailed('boot repair');

    expect(rows[0]).toEqual(
      expect.objectContaining({
        status: 'failed',
        errorMessage: 'boot repair',
      }),
    );
    expect(rows[1]).toEqual(
      expect.objectContaining({
        status: 'failed',
        errorMessage: 'boot repair',
      }),
    );
    expect(rows[2]).toEqual(
      expect.objectContaining({
        status: 'activated',
        errorMessage: null,
      }),
    );
  });
});
