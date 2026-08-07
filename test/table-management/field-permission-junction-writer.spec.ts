import { describe, expect, it } from 'vitest';
import { SqlTableMetadataWriterService } from '../../src/modules/table-management';

describe('SqlTableMetadataWriterService field-permission junctions', () => {
  it('uses the physical hashed junction table when deleting nested permissions', async () => {
    const calls: string[] = [];
    const runner: any = (table: string) => {
      calls.push(table);
      const builder: any = {
        where: () => builder,
        whereIn: () => builder,
        select: async () =>
          table === 'enfyra_field_permission' ? [{ id: 1 }, { id: 2 }] : [],
        delete: async () => 1,
        update: async () => 1,
        insert: async () => [{ id: 3 }],
      };
      return builder;
    };

    await (
      new SqlTableMetadataWriterService() as any
    ).writeNestedFieldPermissions(runner, {
      permissions: [],
      subjectFk: 'columnId',
      subjectFkValue: 10,
    });

    expect(calls).toContain('j_33f4340124d4');
    expect(calls).not.toContain(
      'enfyra_field_permission_allowedUsers_enfyra_user',
    );
  });

  it('skips optional junction cleanup when no physical table exists', async () => {
    const calls: string[] = [];
    const runner: any = (table: string) => {
      calls.push(table);
      const builder: any = {
        where: () => builder,
        whereIn: () => builder,
        select: async () =>
          table === 'enfyra_field_permission' ? [{ id: 1 }, { id: 2 }] : [],
        delete: async () => 1,
      };
      return builder;
    };
    runner.schema = { hasTable: async () => false };

    await (
      new SqlTableMetadataWriterService() as any
    ).writeNestedFieldPermissions(runner, {
      permissions: [],
      subjectFk: 'columnId',
      subjectFkValue: 10,
    });

    expect(calls).toEqual(['enfyra_field_permission']);
  });
});
