import { describe, expect, it } from 'vitest';
import { SqlTableMetadataWriterService } from '../../src/modules/table-management/services/sql-table-metadata-writer.service';

function createQueryRunner() {
  const inserts: Record<string, any[]> = {};
  let nextRelationId = 700;

  const runner: any = (table: string) => {
    const state: any = { table, whereValue: null, whereInValue: null };
    const resolveRows = () => {
      if (table === 'relation_definition') {
        if (state.whereValue?.sourceTableId === 1) return [];
        if (state.whereValue?.sourceTableId === 2) return [];
      }
      if (table === 'table_definition' && state.whereInValue?.column === 'id') {
        return [{ id: 2, name: 'users' }];
      }
      return [];
    };
    const builder: any = {
      where(value: any) {
        state.whereValue = value;
        return builder;
      },
      whereIn(column: string, values: any[]) {
        state.whereInValue = { column, values };
        return builder;
      },
      select() {
        return builder;
      },
      first() {
        if (
          table === 'relation_definition' &&
          state.whereValue?.sourceTableId === 2 &&
          state.whereValue?.propertyName === 'posts'
        ) {
          return Promise.resolve(null);
        }
        if (
          table === 'relation_definition' &&
          state.whereValue?.id === 701
        ) {
          return Promise.resolve(inserts.relation_definition?.[0] ?? null);
        }
        return Promise.resolve(null);
      },
      update(data: any) {
        inserts[table] = inserts[table] || [];
        inserts[table].push({ __update: true, ...data });
        return Promise.resolve(1);
      },
      delete() {
        return Promise.resolve(1);
      },
      insert(data: any) {
        const rows = Array.isArray(data) ? data : [data];
        inserts[table] = inserts[table] || [];
        for (const row of rows) {
          inserts[table].push({ ...row });
        }
        if (table === 'relation_definition') {
          return Promise.resolve([nextRelationId++]);
        }
        return Promise.resolve([1]);
      },
      then(resolve: any, reject: any) {
        return Promise.resolve(resolveRows()).then(resolve, reject);
      },
    };
    return builder;
  };

  return { runner, inserts };
}

describe('SqlTableMetadataWriterService relation onDelete metadata', () => {
  it('persists onDelete for owning and inverse relations', async () => {
    const { runner, inserts } = createQueryRunner();
    const service = new SqlTableMetadataWriterService();

    await service.writeTableMetadataUpdates(
      runner,
      1,
      {
        name: 'posts',
        columns: [],
        relations: [
          {
            propertyName: 'author',
            type: 'many-to-one',
            targetTable: { id: 2 },
            inversePropertyName: 'posts',
            isNullable: false,
            onDelete: 'CASCADE',
          },
        ],
      },
      { id: 1, name: 'posts', uniques: '[]', indexes: '[]' },
      new Set<string>(),
    );

    const relationRows = inserts.relation_definition || [];
    const owning = relationRows.find((row) => row.propertyName === 'author');
    const inverse = relationRows.find((row) => row.propertyName === 'posts');

    expect(owning?.onDelete).toBe('CASCADE');
    expect(inverse?.onDelete).toBe('CASCADE');
  });
});
