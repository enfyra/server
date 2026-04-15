import { generateGraphQLTypeDefsFromTables } from '../../src/modules/graphql/utils/generate-type-defs';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

function makeTable(name: string, columns: any[], relations: any[] = []): any {
  return { name, columns, relations };
}

function makeColumn(name: string, type = 'varchar', opts: any = {}): any {
  return { name, type, isNullable: true, isPrimary: false, ...opts };
}

describe('generateGraphQLTypeDefsFromTables – security filter', () => {
  beforeAll(() => {
    DatabaseConfigService.overrideForTesting('mysql');
  });

  afterAll(() => {
    DatabaseConfigService.resetForTesting();
  });

  describe('queryableTableNames allowlist', () => {
    it('emits only tables present in queryableTableNames', () => {
      const tables = [
        makeTable('user_definition', [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('email')]),
        makeTable('secret_config', [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('apiKey')]),
      ];
      const schema = generateGraphQLTypeDefsFromTables(tables, new Set(['user_definition']));
      expect(schema).toContain('type user_definition');
      expect(schema).not.toContain('type secret_config');
    });

    it('emits all tables when queryableTableNames is undefined', () => {
      const tables = [
        makeTable('user_definition', [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('email')]),
        makeTable('post', [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('title')]),
      ];
      const schema = generateGraphQLTypeDefsFromTables(tables, undefined);
      expect(schema).toContain('type user_definition');
      expect(schema).toContain('type post');
    });

    it('produces empty query/mutation blocks when set is empty', () => {
      const tables = [
        makeTable('user_definition', [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('email')]),
      ];
      const schema = generateGraphQLTypeDefsFromTables(tables, new Set([]));
      expect(schema).not.toContain('type user_definition');
      expect(schema).not.toContain('user_definition(');
    });
  });

  describe('unpublished column exclusion', () => {
    it('excludes unpublished columns from output type', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('email'),
        makeColumn('password', 'varchar', { isPublished: false }),
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('user_definition', columns)],
        new Set(['user_definition']),
      );
      expect(schema).toContain('email');
      expect(schema).not.toContain('password');
    });

    it('excludes unpublished columns from input types', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('email'),
        makeColumn('passwordHash', 'varchar', { isPublished: false }),
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('user_definition', columns)],
        new Set(['user_definition']),
      );
      expect(schema).toContain('input user_definitionInput');
      expect(schema).not.toContain('passwordHash');
    });

    it('excludes unpublished columns from update input types', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('name'),
        makeColumn('secret', 'varchar', { isPublished: false }),
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('user_definition', columns)],
        new Set(['user_definition']),
      );
      expect(schema).toContain('input user_definitionUpdateInput');
      expect(schema).not.toContain('secret');
    });

    it('keeps published columns intact', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('username'),
        makeColumn('bio'),
        makeColumn('token', 'varchar', { isPublished: false }),
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('user_definition', columns)],
        new Set(['user_definition']),
      );
      expect(schema).toContain('username');
      expect(schema).toContain('bio');
      expect(schema).not.toContain('token');
    });

    it('does not emit a type when all non-primary columns are unpublished', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('secret', 'varchar', { isPublished: false }),
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('internal_table', columns)],
        new Set(['internal_table']),
      );
      expect(schema).not.toContain('internal_table(');
    });
  });

  describe('unpublished relation exclusion', () => {
    it('excludes unpublished relations from output type', () => {
      const columns = [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('title')];
      const relations = [
        { propertyName: 'author', targetTableName: 'user_definition', type: 'many-to-one', isPublished: false },
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [
          makeTable('post', columns, relations),
          makeTable('user_definition', [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('email')]),
        ],
        new Set(['post', 'user_definition']),
      );
      expect(schema).toContain('type post');
      expect(schema).not.toContain('author: user_definition');
    });
  });

  describe('stub types for non-GQL relation targets', () => {
    it('generates stub type for relation pointing to non-queryable table', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('title'),
      ];
      const relations = [
        { propertyName: 'author', targetTableName: 'admin_user', type: 'many-to-one' },
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('post', columns, relations)],
        new Set(['post']),
      );
      expect(schema).toContain('type admin_user');
      expect(schema).toContain('type admin_user {\n  id: ID\n}');
      expect(schema).not.toContain('admin_user(');
    });

    it('does not duplicate stub type if it appears in multiple relations', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('title'),
      ];
      const relations = [
        { propertyName: 'author', targetTableName: 'admin_user', type: 'many-to-one' },
        { propertyName: 'editor', targetTableName: 'admin_user', type: 'many-to-one' },
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('post', columns, relations)],
        new Set(['post']),
      );
      const count = (schema.match(/type admin_user/g) || []).length;
      expect(count).toBe(1);
    });

    it('does not create stub when relation target is queryable', () => {
      const userColumns = [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('name')];
      const postColumns = [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('title')];
      const relations = [
        { propertyName: 'author', targetTableName: 'user_definition', type: 'many-to-one' },
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [
          makeTable('post', postColumns, relations),
          makeTable('user_definition', userColumns),
        ],
        new Set(['post', 'user_definition']),
      );
      const typeMatches = (schema.match(/^type user_definition\s*\{/gm) || []).length;
      expect(typeMatches).toBe(1);
    });

    it('stub type has only id field', () => {
      const columns = [makeColumn('id', 'uuid', { isPrimary: true }), makeColumn('body')];
      const relations = [
        { propertyName: 'owner', targetTableName: 'hidden_table', type: 'many-to-one' },
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('comment', columns, relations)],
        new Set(['comment']),
      );
      const stubMatch = schema.match(/type hidden_table \{([^}]+)\}/);
      expect(stubMatch).toBeTruthy();
      const stubBody = stubMatch![1].trim();
      expect(stubBody).toBe('id: ID');
    });
  });

  describe('invalid identifiers', () => {
    it('skips columns with invalid GraphQL identifier names', () => {
      const columns = [
        makeColumn('id', 'uuid', { isPrimary: true }),
        makeColumn('valid_name'),
        { name: '123invalid', type: 'varchar', isNullable: true },
        { name: 'has-hyphen', type: 'varchar', isNullable: true },
      ];
      const schema = generateGraphQLTypeDefsFromTables(
        [makeTable('my_table', columns)],
        new Set(['my_table']),
      );
      expect(schema).toContain('valid_name');
      expect(schema).not.toContain('123invalid');
      expect(schema).not.toContain('has-hyphen');
    });
  });
});
