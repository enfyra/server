import type { IQueryBuilder } from '../../src/domain/shared/interfaces/query-builder.interface';
import { getSqlJunctionMetadata } from '../../src/domain/bootstrap/utils/sql-junction-metadata.util';
import { describe, expect, it, vi } from 'vitest';

function fixture(relation: Record<string, unknown> | undefined) {
  const builder = {
    leftJoin: vi.fn().mockReturnThis(),
    select: vi.fn().mockReturnThis(),
    where: vi.fn().mockReturnThis(),
    first: vi.fn().mockResolvedValue(relation),
  };
  const knex = Object.assign(
    vi.fn(() => builder),
    {
      schema: {
        hasTable: vi.fn().mockResolvedValue(true),
        hasColumn: vi.fn().mockResolvedValue(true),
        createTable: vi.fn(),
      },
    },
  );
  return { knex, query: { getKnex: () => knex } as unknown as IQueryBuilder };
}

const input = {
  sourceTable: 'users',
  propertyName: 'roles',
  targetTable: 'roles',
};

describe('bootstrap junction metadata reader', () => {
  it('consumes the persisted mapping without choosing a different storage or writing schema', async () => {
    const { knex, query } = fixture({
      junctionTableName: 'persisted_junction',
      junctionSourceColumn: 'memberId',
      junctionTargetColumn: 'roleId',
    });
    await expect(getSqlJunctionMetadata(query, input)).resolves.toEqual({
      junctionTable: 'persisted_junction',
      sourceColumn: 'memberId',
      targetColumn: 'roleId',
    });
    expect(knex).toHaveBeenCalledTimes(1);
    expect(knex.schema.hasTable).not.toHaveBeenCalled();
    expect(knex.schema.createTable).not.toHaveBeenCalled();
  });

  it.each([undefined, { junctionTableName: 'persisted_junction' }])(
    'rejects absent or incomplete metadata instead of inventing a second mapping',
    async (relation) => {
      const { knex, query } = fixture(relation);
      await expect(getSqlJunctionMetadata(query, input)).rejects.toThrow(
        /Junction metadata.*users.roles/,
      );
      expect(knex.schema.createTable).not.toHaveBeenCalled();
    },
  );
});
