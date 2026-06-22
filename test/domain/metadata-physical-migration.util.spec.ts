import { MetadataPhysicalMigrationHelper } from '../../src/engines/bootstrap/utils/metadata-physical-migration.util';

function makePhysicalSql(rows: any[]) {
  const tables = { enfyra_file: rows };
  const schemas = { enfyra_file: ['id', 'isPublished', 'isPublic'] };
  const knex = {
    schema: {
      hasTable: jest.fn(async (table: string) => table in tables),
      hasColumn: jest.fn(
        async (table: string, column: string) =>
          (schemas as any)[table]?.includes(column) ?? false,
      ),
      alterTable: jest.fn(async (table: string, callback: any) => {
        const builder = {
          dropColumn: jest.fn((column: string) => {
            (schemas as any)[table] = (schemas as any)[table].filter(
              (name: string) => name !== column,
            );
            (tables as any)[table] = (tables as any)[table].map((row: any) => {
              const next = { ...row };
              delete next[column];
              return next;
            });
          }),
          renameColumn: jest.fn((oldName: string, newName: string) => {
            (schemas as any)[table] = (schemas as any)[table].map(
              (name: string) => (name === oldName ? newName : name),
            );
            (tables as any)[table] = (tables as any)[table].map((row: any) => {
              const next = { ...row, [newName]: row[oldName] };
              delete next[oldName];
              return next;
            });
          }),
        };
        callback(builder);
      }),
    },
    raw: jest.fn(async (sql: string, params: string[]) => {
      if (sql.startsWith('UPDATE')) {
        const [table, newName, oldName] = params;
        (tables as any)[table] = (tables as any)[table].map((row: any) =>
          row[newName] === null || row[newName] === undefined
            ? { ...row, [newName]: row[oldName] }
            : row,
        );
        return;
      }
      const [table, oldName, newName] = params;
      const count = (tables as any)[table].filter(
        (row: any) =>
          row[oldName] !== null &&
          row[oldName] !== undefined &&
          row[newName] !== null &&
          row[newName] !== undefined &&
          row[oldName] !== row[newName],
      ).length;
      return [{ count }];
    }),
  };
  return { knex, tables, schemas };
}

function makePhysicalMongo(documents: any[]) {
  const collections = { enfyra_file: documents };
  const db = {
    collection: jest.fn((name: string) => ({
      updateMany: jest.fn(async (filter: any, update: any) => {
        (collections as any)[name] = (collections as any)[name].map(
          (doc: any) => {
            const hasOld = doc.isPublished !== undefined;
            const hasNew = doc.isPublic !== undefined;
            if (filter.isPublished?.$exists && !hasOld) return doc;
            if (filter.isPublic?.$exists === false && hasNew) return doc;
            if (Array.isArray(update)) {
              return { ...doc, isPublic: doc.isPublished };
            }
            if (update.$unset?.isPublished !== undefined) {
              const next = { ...doc };
              delete next.isPublished;
              return next;
            }
            return doc;
          },
        );
      }),
      countDocuments: jest.fn(
        async () =>
          (collections as any)[name].filter(
            (doc: any) =>
              doc.isPublished !== undefined &&
              doc.isPublic !== undefined &&
              doc.isPublished !== doc.isPublic,
          ).length,
      ),
    })),
  };
  return { db, collections };
}

describe('MetadataPhysicalMigrationHelper conflict-safe field rename', () => {
  it('preserves SQL legacy column when old and new values conflict', async () => {
    const sql = makePhysicalSql([
      { id: 1, isPublished: true, isPublic: false },
    ]);
    const helper = new MetadataPhysicalMigrationHelper({
      queryBuilderService: {
        getKnex: jest.fn(() => sql.knex),
      } as any,
      verbose: jest.fn(),
    });

    await helper.renameSqlPhysicalColumnIfNeeded(
      'enfyra_file',
      'isPublished',
      'isPublic',
    );

    expect(sql.tables.enfyra_file).toEqual([
      { id: 1, isPublished: true, isPublic: false },
    ]);
    expect(sql.schemas.enfyra_file).toContain('isPublished');
  });

  it('drops SQL legacy column when target values are safely backfilled', async () => {
    const sql = makePhysicalSql([{ id: 1, isPublished: true, isPublic: null }]);
    const helper = new MetadataPhysicalMigrationHelper({
      queryBuilderService: {
        getKnex: jest.fn(() => sql.knex),
      } as any,
      verbose: jest.fn(),
    });

    await helper.renameSqlPhysicalColumnIfNeeded(
      'enfyra_file',
      'isPublished',
      'isPublic',
    );

    expect(sql.tables.enfyra_file).toEqual([{ id: 1, isPublic: true }]);
    expect(sql.schemas.enfyra_file).not.toContain('isPublished');
  });

  it('preserves Mongo legacy field when old and new values conflict', async () => {
    const mongo = makePhysicalMongo([
      { _id: '1', isPublished: true, isPublic: false },
    ]);
    const helper = new MetadataPhysicalMigrationHelper({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      verbose: jest.fn(),
    });

    await helper.renameMongoDocumentFieldIfNeeded(
      'enfyra_file',
      'isPublished',
      'isPublic',
    );

    expect(mongo.collections.enfyra_file).toEqual([
      { _id: '1', isPublished: true, isPublic: false },
    ]);
  });

  it('unsets Mongo legacy field when target values are safely backfilled', async () => {
    const mongo = makePhysicalMongo([{ _id: '1', isPublished: true }]);
    const helper = new MetadataPhysicalMigrationHelper({
      queryBuilderService: {
        isMongoDb: jest.fn(() => true),
        getMongoDb: jest.fn(() => mongo.db),
      } as any,
      verbose: jest.fn(),
    });

    await helper.renameMongoDocumentFieldIfNeeded(
      'enfyra_file',
      'isPublished',
      'isPublic',
    );

    expect(mongo.collections.enfyra_file).toEqual([
      { _id: '1', isPublic: true },
    ]);
  });
});
