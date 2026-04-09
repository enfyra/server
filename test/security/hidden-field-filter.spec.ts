import knex from 'knex';
import { buildRelationSubquery } from '../../src/infrastructure/query-builder/utils/sql/relation-filter.util';

const db = knex({ client: 'sqlite3', connection: ':memory:', useNullAsDefault: true });

function makeTableMeta(
  name: string,
  columns: Array<{ name: string; type?: string; isHidden?: boolean; isPrimary?: boolean }>,
  relations: any[] = [],
) {
  return {
    name,
    columns: columns.map((c) => ({
      type: 'varchar',
      isPrimary: false,
      isNullable: true,
      ...c,
    })),
    relations,
  };
}

describe('buildRelationSubquery – hidden field filter stripping', () => {
  const userMeta = makeTableMeta('user_definition', [
    { name: 'id', type: 'int', isPrimary: true },
    { name: 'email' },
    { name: 'password', isHidden: true },
    { name: 'role' },
  ]);

  const postMeta = makeTableMeta(
    'post',
    [
      { name: 'id', type: 'int', isPrimary: true },
      { name: 'title' },
      { name: 'userId', type: 'int' },
    ],
    [
      {
        propertyName: 'author',
        type: 'many-to-one',
        targetTableName: 'user_definition',
        foreignKeyColumn: 'userId',
      },
    ],
  );

  const getMetadata = async (tableName: string) => {
    if (tableName === 'user_definition') return userMeta as any;
    if (tableName === 'post') return postMeta as any;
    return null;
  };

  it('strips hidden column from direct filter on relation', async () => {
    const sql = await buildRelationSubquery(
      db,
      'post',
      'author',
      { password: { _eq: 'secret' }, role: { _eq: 'admin' } },
      postMeta as any,
      'mysql',
      getMetadata,
    );
    expect(sql).not.toBeNull();
    expect(sql).not.toContain('password');
    expect(sql).toContain('role');
  });

  it('allows non-hidden fields in relation filter', async () => {
    const sql = await buildRelationSubquery(
      db,
      'post',
      'author',
      { email: { _eq: 'test@example.com' } },
      postMeta as any,
      'mysql',
      getMetadata,
    );
    expect(sql).not.toBeNull();
    expect(sql).toContain('email');
  });

  it('strips hidden field nested inside _and', async () => {
    const sql = await buildRelationSubquery(
      db,
      'post',
      'author',
      {
        _and: [
          { password: { _eq: 'hacked' } },
          { role: { _eq: 'admin' } },
        ],
      },
      postMeta as any,
      'mysql',
      getMetadata,
    );
    expect(sql).not.toBeNull();
    expect(sql).not.toContain('password');
  });

  it('strips hidden field nested inside _or', async () => {
    const sql = await buildRelationSubquery(
      db,
      'post',
      'author',
      {
        _or: [
          { password: { _eq: 'hacked' } },
          { role: { _eq: 'user' } },
        ],
      },
      postMeta as any,
      'mysql',
      getMetadata,
    );
    expect(sql).not.toBeNull();
    expect(sql).not.toContain('password');
  });

  it('produces a valid SQL string even after stripping all fields', async () => {
    const sql = await buildRelationSubquery(
      db,
      'post',
      'author',
      { password: { _eq: 'only-hidden' } },
      postMeta as any,
      'mysql',
      getMetadata,
    );
    expect(typeof sql).toBe('string');
    expect(sql!.toLowerCase()).toContain('select');
  });

  it('does not strip when target metadata is unavailable', async () => {
    const noMeta = async (_: string) => null;
    const sql = await buildRelationSubquery(
      db,
      'post',
      'author',
      { password: { _eq: 'keep' } },
      postMeta as any,
      'mysql',
      noMeta,
    );
    expect(sql).not.toBeNull();
    expect(sql).toContain('password');
  });

  describe('one-to-many relation', () => {
    const commentMeta = makeTableMeta(
      'comment',
      [
        { name: 'id', type: 'int', isPrimary: true },
        { name: 'body' },
        { name: 'internalNote', isHidden: true },
        { name: 'postId', type: 'int' },
      ],
    );

    const postWithCommentsMeta = makeTableMeta(
      'post',
      [{ name: 'id', type: 'int', isPrimary: true }, { name: 'title' }],
      [
        {
          propertyName: 'comments',
          type: 'one-to-many',
          targetTableName: 'comment',
          foreignKeyColumn: 'postId',
        },
      ],
    );

    const getMetaWithComments = async (tableName: string) => {
      if (tableName === 'comment') return commentMeta as any;
      if (tableName === 'post') return postWithCommentsMeta as any;
      return null;
    };

    it('strips hidden column in one-to-many filter', async () => {
      const sql = await buildRelationSubquery(
        db,
        'post',
        'comments',
        { internalNote: { _eq: 'secret' }, body: { _eq: 'hello' } },
        postWithCommentsMeta as any,
        'mysql',
        getMetaWithComments,
      );
      expect(sql).not.toBeNull();
      expect(sql).not.toContain('internalNote');
      expect(sql).toContain('body');
    });
  });
});
