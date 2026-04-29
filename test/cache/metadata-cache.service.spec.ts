import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { MetadataCacheService } from '../../src/engines/cache';
import { DatabaseConfigService } from '../../src/shared/services';

function makeKnex(rowsByTable: Record<string, any[]>) {
  return {
    table: (name: string) => ({
      select: async () => rowsByTable[name] ?? [],
    }),
  };
}

function makeService(rowsByTable: Record<string, any[]>) {
  return new MetadataCacheService({
    databaseConfigService: { getDbType: () => 'postgres' } as any,
    lazyRef: {
      knexService: {
        getKnex: () => makeKnex(rowsByTable),
      },
    } as any,
  });
}

describe('MetadataCacheService', () => {
  beforeEach(() => {
    DatabaseConfigService.overrideForTesting('postgres');
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  it('builds SQL metadata from stored metadata without physical schema introspection', async () => {
    const service = makeService({
      table_definition: [
        { id: 1, name: 'authors', indexes: '[]', uniques: '[]' },
        { id: 2, name: 'posts', indexes: '[]', uniques: '[]' },
      ],
      column_definition: [
        {
          id: 10,
          tableId: 1,
          name: 'id',
          type: 'uuid',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
          isSystem: true,
          isUpdatable: false,
          isPublished: true,
        },
        {
          id: 20,
          tableId: 2,
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
          isSystem: true,
          isUpdatable: false,
          isPublished: true,
        },
        {
          id: 21,
          tableId: 2,
          name: 'title',
          type: 'varchar',
          isPrimary: false,
          isGenerated: false,
          isNullable: false,
          isSystem: false,
          isUpdatable: true,
          isPublished: true,
        },
      ],
      relation_definition: [
        {
          id: 30,
          sourceTableId: 2,
          targetTableId: 1,
          propertyName: 'author',
          type: 'many-to-one',
          isNullable: false,
          isSystem: false,
          isUpdatable: true,
          isPublished: true,
          onDelete: 'CASCADE',
        },
      ],
    });

    const metadata = await service.getMetadata();
    const posts = metadata.tables.get('posts');
    const authors = metadata.tables.get('authors');

    expect(posts).toBeTruthy();
    expect(authors).toBeTruthy();
    expect(posts.relations[0]).toMatchObject({
      propertyName: 'author',
      targetTableName: 'authors',
      foreignKeyColumn: 'authorId',
      isInverse: false,
    });
    expect(posts.columns).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: 'authorId',
          type: 'uuid',
          isForeignKey: true,
          relationPropertyName: 'author',
          isNullable: false,
        }),
        expect.objectContaining({
          name: 'createdAt',
          type: 'datetime',
          isSystem: true,
          isUpdatable: false,
        }),
        expect.objectContaining({
          name: 'updatedAt',
          type: 'datetime',
          isSystem: true,
          isUpdatable: false,
        }),
      ]),
    );
    expect(authors.columns.map((column: any) => column.name)).toEqual(
      expect.arrayContaining(['createdAt', 'updatedAt']),
    );
  });

  it('marks explicit FK and timestamp metadata instead of duplicating it', async () => {
    const service = makeService({
      table_definition: [
        { id: 1, name: 'authors', indexes: '[]', uniques: '[]' },
        { id: 2, name: 'posts', indexes: '[]', uniques: '[]' },
      ],
      column_definition: [
        {
          id: 10,
          tableId: 1,
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
        },
        {
          id: 20,
          tableId: 2,
          name: 'id',
          type: 'int',
          isPrimary: true,
          isGenerated: true,
          isNullable: false,
        },
        {
          id: 21,
          tableId: 2,
          name: 'authorId',
          type: 'int',
          isNullable: true,
          isUpdatable: true,
          isSystem: false,
        },
        {
          id: 22,
          tableId: 2,
          name: 'createdAt',
          type: 'datetime',
          isNullable: true,
          isUpdatable: true,
          isSystem: false,
        },
      ],
      relation_definition: [
        {
          id: 30,
          sourceTableId: 2,
          targetTableId: 1,
          propertyName: 'author',
          type: 'many-to-one',
          isNullable: true,
          isUpdatable: false,
        },
      ],
    });

    const metadata = await service.getMetadata();
    const posts = metadata.tables.get('posts');
    const authorIdColumns = posts.columns.filter(
      (column: any) => column.name === 'authorId',
    );
    const createdAtColumns = posts.columns.filter(
      (column: any) => column.name === 'createdAt',
    );

    expect(authorIdColumns).toHaveLength(1);
    expect(authorIdColumns[0]).toMatchObject({
      isForeignKey: true,
      relationPropertyName: 'author',
      isUpdatable: false,
    });
    expect(createdAtColumns).toHaveLength(1);
    expect(createdAtColumns[0]).toMatchObject({
      isSystem: true,
      isUpdatable: false,
    });
    expect(
      posts.columns.filter((column: any) => column.name === 'updatedAt'),
    ).toHaveLength(1);
  });
});
