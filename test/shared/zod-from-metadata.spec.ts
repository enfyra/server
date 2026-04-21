import { describe, it, expect } from 'vitest';
import { buildZodFromMetadata } from '../../src/shared/utils/zod-from-metadata';
import { TColumnRule } from '../../src/infrastructure/cache/services/column-rule-cache.service';

function makeMeta(overrides: any = {}) {
  return {
    name: 't',
    columns: [],
    relations: [],
    validateBody: true,
    ...overrides,
  };
}

function col(name: string, type: string, extras: any = {}) {
  return { id: `col_${name}`, name, type, ...extras };
}

const noRules = () => [];

function build(
  meta: any,
  mode: 'create' | 'update' = 'create',
  getTableMetadata: any = () => null,
  rulesForColumn: (id: any) => TColumnRule[] = noRules,
) {
  return buildZodFromMetadata({
    tableMeta: meta,
    mode,
    rulesForColumn,
    getTableMetadata,
  });
}

describe('buildZodFromMetadata — column types', () => {
  it('int column → z.number().int()', () => {
    const s = build(
      makeMeta({ columns: [col('age', 'int', { isNullable: false })] }),
    );
    expect(s.safeParse({ age: 1 }).success).toBe(true);
    expect(s.safeParse({ age: 'x' }).success).toBe(false);
    expect(s.safeParse({ age: 1.5 }).success).toBe(false);
  });

  it('float column accepts decimals', () => {
    const s = build(
      makeMeta({ columns: [col('price', 'float', { isNullable: false })] }),
    );
    expect(s.safeParse({ price: 1.5 }).success).toBe(true);
  });

  it('boolean column', () => {
    const s = build(
      makeMeta({ columns: [col('active', 'boolean', { isNullable: false })] }),
    );
    expect(s.safeParse({ active: true }).success).toBe(true);
    expect(s.safeParse({ active: 'yes' }).success).toBe(false);
  });

  it('varchar with options.length applies max', () => {
    const s = build(
      makeMeta({
        columns: [
          col('name', 'varchar', { isNullable: false, options: { length: 5 } }),
        ],
      }),
    );
    expect(s.safeParse({ name: 'abc' }).success).toBe(true);
    expect(s.safeParse({ name: 'abcdef' }).success).toBe(false);
  });

  it('enum column validates options', () => {
    const s = build(
      makeMeta({
        columns: [
          col('status', 'enum', { isNullable: false, options: ['a', 'b'] }),
        ],
      }),
    );
    expect(s.safeParse({ status: 'a' }).success).toBe(true);
    expect(s.safeParse({ status: 'c' }).success).toBe(false);
  });

  it('array-select accepts string array', () => {
    const s = build(
      makeMeta({
        columns: [col('tags', 'array-select', { isNullable: false })],
      }),
    );
    expect(s.safeParse({ tags: ['a', 'b'] }).success).toBe(true);
    expect(s.safeParse({ tags: [1] }).success).toBe(false);
  });

  it('simple-json accepts anything', () => {
    const s = build(
      makeMeta({
        columns: [col('meta', 'simple-json', { isNullable: false })],
      }),
    );
    expect(s.safeParse({ meta: { x: 1 } }).success).toBe(true);
    expect(s.safeParse({ meta: [1, 2] }).success).toBe(true);
  });

  it('date accepts string or Date', () => {
    const s = build(
      makeMeta({ columns: [col('d', 'date', { isNullable: false })] }),
    );
    expect(s.safeParse({ d: '2024-01-01' }).success).toBe(true);
    expect(s.safeParse({ d: new Date() }).success).toBe(true);
    expect(s.safeParse({ d: 123 }).success).toBe(false);
  });
});

describe('buildZodFromMetadata — flags', () => {
  it('isGenerated column skipped from validation (id accepted as auto-managed pass-through)', () => {
    const s = build(
      makeMeta({
        columns: [col('id', 'int', { isGenerated: true, isPrimary: true })],
      }),
    );
    expect(s.safeParse({}).success).toBe(true);
    // id is auto-managed → accepted as optional any (admin UIs echo it back)
    expect(s.safeParse({ id: 1 }).success).toBe(true);
  });

  it('auto-managed columns (id/createdAt/updatedAt) skipped', () => {
    const s = build(
      makeMeta({
        columns: [
          col('id', 'int'),
          col('createdAt', 'date'),
          col('updatedAt', 'date'),
        ],
      }),
    );
    expect(s.safeParse({}).success).toBe(true);
  });

  it('isNullable=false required on create', () => {
    const s = build(
      makeMeta({ columns: [col('name', 'varchar', { isNullable: false })] }),
    );
    expect(s.safeParse({}).success).toBe(false);
    expect(s.safeParse({ name: 'x' }).success).toBe(true);
  });

  it('isNullable=true optional on create', () => {
    const s = build(
      makeMeta({ columns: [col('bio', 'text', { isNullable: true })] }),
    );
    expect(s.safeParse({}).success).toBe(true);
    expect(s.safeParse({ bio: null }).success).toBe(true);
  });

  it('defaultValue present → optional on create', () => {
    const s = build(
      makeMeta({
        columns: [
          col('active', 'boolean', { isNullable: false, defaultValue: true }),
        ],
      }),
    );
    expect(s.safeParse({}).success).toBe(true);
  });

  it('update mode → all fields optional', () => {
    const s = build(
      makeMeta({ columns: [col('name', 'varchar', { isNullable: false })] }),
      'update',
    );
    expect(s.safeParse({}).success).toBe(true);
    expect(s.safeParse({ name: 'x' }).success).toBe(true);
  });

  it('isUpdatable=false stripped on update', () => {
    const s = build(
      makeMeta({
        columns: [
          col('code', 'varchar', { isNullable: true, isUpdatable: false }),
        ],
      }),
      'update',
    );
    expect(s.safeParse({}).success).toBe(true);
    expect(s.safeParse({ code: 'x' }).success).toBe(false); // strict
  });

  it('strict mode rejects unknown keys', () => {
    const s = build(
      makeMeta({ columns: [col('name', 'varchar', { isNullable: false })] }),
    );
    expect(s.safeParse({ name: 'x', extra: 1 }).success).toBe(false);
  });
});

describe('buildZodFromMetadata — column rules', () => {
  const rule = (ruleType: string, value: any): TColumnRule => ({
    id: 'r',
    ruleType: ruleType as any,
    value,
    message: null,
    isEnabled: true,
    columnId: 'col_age',
  });

  it('min rule on number', () => {
    const s = build(
      makeMeta({ columns: [col('age', 'int', { isNullable: false })] }),
      'create',
      () => null,
      () => [rule('min', { v: 18 })],
    );
    expect(s.safeParse({ age: 20 }).success).toBe(true);
    expect(s.safeParse({ age: 10 }).success).toBe(false);
  });

  it('max rule on number', () => {
    const s = build(
      makeMeta({ columns: [col('age', 'int', { isNullable: false })] }),
      'create',
      () => null,
      () => [rule('max', { v: 100 })],
    );
    expect(s.safeParse({ age: 50 }).success).toBe(true);
    expect(s.safeParse({ age: 101 }).success).toBe(false);
  });

  it('minLength/maxLength on string', () => {
    const s = build(
      makeMeta({ columns: [col('age', 'varchar', { isNullable: false })] }),
      'create',
      () => null,
      () => [rule('minLength', { v: 3 }), rule('maxLength', { v: 5 })],
    );
    expect(s.safeParse({ age: 'abcd' }).success).toBe(true);
    expect(s.safeParse({ age: 'ab' }).success).toBe(false);
    expect(s.safeParse({ age: 'abcdef' }).success).toBe(false);
  });

  it('pattern rule', () => {
    const s = build(
      makeMeta({ columns: [col('age', 'varchar', { isNullable: false })] }),
      'create',
      () => null,
      () => [rule('pattern', { v: '^[a-z]+$' })],
    );
    expect(s.safeParse({ age: 'abc' }).success).toBe(true);
    expect(s.safeParse({ age: 'ABC' }).success).toBe(false);
  });

  it('format email rule', () => {
    const s = build(
      makeMeta({ columns: [col('age', 'varchar', { isNullable: false })] }),
      'create',
      () => null,
      () => [rule('format', { v: 'email' })],
    );
    expect(s.safeParse({ age: 'a@b.com' }).success).toBe(true);
    expect(s.safeParse({ age: 'nope' }).success).toBe(false);
  });

  it('disabled rule skipped', () => {
    const disabled = { ...rule('min', { v: 100 }), isEnabled: false };
    const s = build(
      makeMeta({ columns: [col('age', 'int', { isNullable: false })] }),
      'create',
      () => null,
      () => [disabled],
    );
    expect(s.safeParse({ age: 5 }).success).toBe(true);
  });

  it('minItems/maxItems on array-select', () => {
    const s = build(
      makeMeta({
        columns: [col('age', 'array-select', { isNullable: false })],
      }),
      'create',
      () => null,
      () => [rule('minItems', { v: 2 }), rule('maxItems', { v: 3 })],
    );
    expect(s.safeParse({ age: ['a', 'b'] }).success).toBe(true);
    expect(s.safeParse({ age: ['a'] }).success).toBe(false);
    expect(s.safeParse({ age: ['a', 'b', 'c', 'd'] }).success).toBe(false);
  });
});

describe('buildZodFromMetadata — relations', () => {
  const targetMeta = makeMeta({
    name: 'author',
    validateBody: true,
    columns: [
      col('id', 'int', { isPrimary: true, isGenerated: true }),
      col('name', 'varchar', { isNullable: false }),
    ],
  });

  it('m2o accepts scalar id', () => {
    const s = build(
      makeMeta({
        relations: [
          {
            type: 'many-to-one',
            propertyName: 'author',
            targetTable: 'author',
          },
        ],
      }),
      'create',
      () => targetMeta,
    );
    expect(s.safeParse({ author: 5 }).success).toBe(true);
    expect(s.safeParse({ author: 'abc' }).success).toBe(true);
  });

  it('m2o accepts {id} object', () => {
    const s = build(
      makeMeta({
        relations: [
          {
            type: 'many-to-one',
            propertyName: 'author',
            targetTable: 'author',
          },
        ],
      }),
      'create',
      () => targetMeta,
    );
    expect(s.safeParse({ author: { id: 5 } }).success).toBe(true);
  });

  it('m2o with cascade accepts nested create', () => {
    const s = build(
      makeMeta({
        relations: [
          {
            type: 'many-to-one',
            propertyName: 'author',
            targetTable: 'author',
          },
        ],
      }),
      'create',
      () => targetMeta,
    );
    expect(s.safeParse({ author: { name: 'Alice' } }).success).toBe(true);
    // Nested cascaded objects use passthrough — unknown keys accepted,
    // required fields still enforced. `{nope: 1}` misses `name` → fail.
    expect(s.safeParse({ author: { nope: 1 } }).success).toBe(false);
  });

  it('m2o cascade disabled when target.validateBody=false', () => {
    const nonValidatedTarget = { ...targetMeta, validateBody: false };
    const s = build(
      makeMeta({
        relations: [
          {
            type: 'many-to-one',
            propertyName: 'author',
            targetTable: 'author',
          },
        ],
      }),
      'create',
      () => nonValidatedTarget,
    );
    expect(s.safeParse({ author: 5 }).success).toBe(true);
    expect(s.safeParse({ author: { name: 'Alice' } }).success).toBe(true);
    expect(s.safeParse({ author: { nope: 1 } }).success).toBe(true);
  });

  it('o2o inverse skipped (permissive)', () => {
    const s = build(
      makeMeta({
        relations: [
          {
            type: 'one-to-one',
            propertyName: 'profile',
            targetTable: 'profile',
            mappedBy: 'x',
          },
        ],
      }),
      'create',
      () => targetMeta,
    );
    expect(s.safeParse({ profile: anything() }).success).toBe(true);
  });

  function anything() {
    return { anything: 1 };
  }

  it('m2m accepts array of ids or {id} objects', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'many-to-many', propertyName: 'tags', targetTable: 'tag' },
        ],
      }),
      'create',
      () => null,
    );
    expect(s.safeParse({ tags: [1, 2, 3] }).success).toBe(true);
    expect(s.safeParse({ tags: [{ id: 1 }] }).success).toBe(true);
  });

  it('o2m accepts array with cascade', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'one-to-many', propertyName: 'posts', targetTable: 'author' },
        ],
      }),
      'create',
      () => targetMeta,
    );
    expect(s.safeParse({ posts: [{ name: 'post1' }] }).success).toBe(true);
    expect(s.safeParse({ posts: [5] }).success).toBe(true);
  });
});

describe('buildZodFromMetadata — cascade create vs update semantics', () => {
  const authorMeta = {
    name: 'author',
    validateBody: true,
    columns: [
      col('id', 'int', { isPrimary: true, isGenerated: true }),
      col('name', 'varchar', { isNullable: false }),
      col('bio', 'text', { isNullable: true }),
    ],
    relations: [],
  };
  const postMeta = {
    name: 'post',
    validateBody: true,
    columns: [
      col('id', 'int', { isPrimary: true, isGenerated: true }),
      col('title', 'varchar', { isNullable: false }),
      col('body', 'text', { isNullable: true }),
    ],
    relations: [
      { type: 'many-to-one', propertyName: 'author', targetTable: 'author' },
      { type: 'one-to-many', propertyName: 'comments', targetTable: 'comment' },
      { type: 'many-to-many', propertyName: 'tags', targetTable: 'tag' },
    ],
  };
  const commentMeta = {
    name: 'comment',
    validateBody: true,
    columns: [
      col('id', 'int', { isPrimary: true, isGenerated: true }),
      col('text', 'varchar', { isNullable: false }),
    ],
    relations: [],
  };
  const getMeta = (name: string) =>
    name === 'author' ? authorMeta : name === 'comment' ? commentMeta : null;

  describe('POST (create) — parent requires all required', () => {
    it('parent missing required title → fail', () => {
      const s = build(postMeta, 'create', getMeta);
      const r = s.safeParse({ author: 1 });
      expect(r.success).toBe(false);
    });

    it('parent with title + connect child by id → pass', () => {
      const s = build(postMeta, 'create', getMeta);
      expect(s.safeParse({ title: 'x', author: 5 }).success).toBe(true);
    });

    it('parent with title + nested create child fully required → pass', () => {
      const s = build(postMeta, 'create', getMeta);
      expect(
        s.safeParse({ title: 'x', author: { name: 'Alice' } }).success,
      ).toBe(true);
    });

    it('parent with title + nested create child missing required → fail', () => {
      const s = build(postMeta, 'create', getMeta);
      const r = s.safeParse({ title: 'x', author: { bio: 'no name' } });
      expect(r.success).toBe(false);
      if (!r.success) {
        const paths = r.error.issues.map((i) => i.path.join('.'));
        expect(paths.some((p) => p.startsWith('author'))).toBe(true);
      }
    });

    it('o2m array of nested-create — each item requires all required', () => {
      const s = build(postMeta, 'create', getMeta);
      const r = s.safeParse({
        title: 'x',
        comments: [{ text: 'ok' }, { wrong: 'missing text' }],
      });
      expect(r.success).toBe(false);
      if (!r.success) {
        const paths = r.error.issues.map((i) => i.path.join('.'));
        expect(paths.some((p) => p.startsWith('comments.1'))).toBe(true);
      }
    });

    it('o2m array of connect-by-id → pass', () => {
      const s = build(postMeta, 'create', getMeta);
      expect(
        s.safeParse({ title: 'x', comments: [{ id: 1 }, { id: 2 }] }).success,
      ).toBe(true);
    });

    it('m2m array of scalar ids → pass', () => {
      const s = build(postMeta, 'create', getMeta);
      expect(s.safeParse({ title: 'x', tags: [1, 2, 3] }).success).toBe(true);
    });
  });

  describe('PATCH (update) — parent fields all optional, nested-create still strict', () => {
    it('parent with empty body → pass (no required enforced)', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(s.safeParse({}).success).toBe(true);
    });

    it('parent PATCH only one field → pass', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(s.safeParse({ title: 'new title' }).success).toBe(true);
    });

    it('parent PATCH connect child by id → pass', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(s.safeParse({ author: 5 }).success).toBe(true);
    });

    it('parent PATCH connect child with {id, extras} → pass (passthrough)', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(s.safeParse({ author: { id: 5, name: 'updated' } }).success).toBe(
        true,
      );
    });

    it('parent PATCH with nested-create child (no id) missing required → fail', () => {
      const s = build(postMeta, 'update', getMeta);
      const r = s.safeParse({ author: { bio: 'no name' } });
      expect(r.success).toBe(false);
    });

    it('parent PATCH with nested-create child fully required → pass', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(s.safeParse({ author: { name: 'New Author' } }).success).toBe(
        true,
      );
    });

    it('parent PATCH wrong type on field → fail', () => {
      const s = build(postMeta, 'update', getMeta);
      const r = s.safeParse({ title: 123 });
      expect(r.success).toBe(false);
    });

    it('parent PATCH o2m array mixed connect + nested-create → pass when both valid', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(
        s.safeParse({
          comments: [{ id: 1 }, { text: 'new' }],
        }).success,
      ).toBe(true);
    });

    it('parent PATCH o2m array nested-create missing required → fail', () => {
      const s = build(postMeta, 'update', getMeta);
      const r = s.safeParse({
        comments: [{ id: 1 }, { wrong: 'x' }],
      });
      expect(r.success).toBe(false);
    });

    it('parent PATCH m2m connect-only array → pass', () => {
      const s = build(postMeta, 'update', getMeta);
      expect(s.safeParse({ tags: [1, 2, 3] }).success).toBe(true);
    });

    it('parent PATCH strict rejects unknown keys', () => {
      const s = build(postMeta, 'update', getMeta);
      const r = s.safeParse({ bogus: 'x' });
      expect(r.success).toBe(false);
    });
  });

  describe('PATCH nested-create still runs create rules', () => {
    const ruleMap = new Map<string, any[]>();
    ruleMap.set('col_name', [
      {
        id: 'r1',
        ruleType: 'minLength',
        value: { v: 3 },
        message: null,
        isEnabled: true,
        columnId: 'col_name',
      },
    ]);
    const rulesFor = (id: any) => ruleMap.get(String(id)) ?? [];

    it('nested-create inherits column rules of target table', () => {
      const s = build(postMeta, 'update', getMeta, rulesFor);
      const r = s.safeParse({ author: { name: 'ab' } });
      expect(r.success).toBe(false);
      if (!r.success) {
        const msgs = r.error.issues.map((i) => i.message);
        expect(msgs.join('|')).toMatch(/>=?\s*3/);
      }
    });
  });
});

describe('buildZodFromMetadata — cycle detection + depth cap', () => {
  const author: any = {
    name: 'author',
    validateBody: true,
    columns: [],
    relations: [],
  };
  const post: any = {
    name: 'post',
    validateBody: true,
    columns: [],
    relations: [],
  };
  author.relations = [
    {
      type: 'one-to-many',
      propertyName: 'posts',
      targetTable: 'post',
      mappedBy: 'author',
    },
  ];
  post.relations = [
    {
      type: 'many-to-one',
      propertyName: 'author',
      targetTable: 'author',
      foreignKeyColumn: 'authorId',
    },
  ];

  const getTableMetadata = (name: string) =>
    name === 'author' ? author : name === 'post' ? post : null;

  it('cycle does not infinite-loop; cascaded nested objects use passthrough', () => {
    const s = build(author, 'create', getTableMetadata);
    // Nested post without any field → pass
    expect(s.safeParse({ posts: [{}] }).success).toBe(true);
    // Nested with back-ref → also pass (passthrough ignores extras; server strips)
    expect(s.safeParse({ posts: [{ author: 5 }] }).success).toBe(true);
  });

  it('deep cascade: nested o2m on both sides works without back-ref', () => {
    const s = build(author, 'create', getTableMetadata);
    // Just the nested o2m empty → pass
    expect(s.safeParse({ posts: [] }).success).toBe(true);
    expect(s.safeParse({ posts: [{}] }).success).toBe(true);
  });

  it('self-referencing m2o (parent → same table) does not infinite-loop', () => {
    const selfRef: any = {
      name: 'category',
      validateBody: true,
      columns: [
        col('id', 'int', { isPrimary: true, isGenerated: true }),
        col('name', 'varchar', { isNullable: false }),
      ],
      relations: [
        {
          type: 'many-to-one',
          propertyName: 'parent',
          targetTable: 'category',
        },
      ],
    };
    const s = build(selfRef, 'create', () => selfRef);
    expect(s.safeParse({ name: 'x', parent: 5 }).success).toBe(true);
    expect(
      s.safeParse({ name: 'x', parent: { name: 'new-parent' } }).success,
    ).toBe(true);
  });
});

describe('buildZodFromMetadata — rule edge cases', () => {
  const mkRule = (
    ruleType: string,
    value: any,
    overrides: any = {},
  ): TColumnRule => ({
    id: 'r',
    ruleType: ruleType as any,
    value,
    message: null,
    isEnabled: true,
    columnId: 'col_f',
    ...overrides,
  });

  it('invalid regex in pattern rule → silently skipped (no crash)', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'varchar', { isNullable: false })] }),
      'create',
      () => null,
      () => [mkRule('pattern', { v: '[unclosed' })],
    );
    expect(s.safeParse({ f: 'any' }).success).toBe(true);
  });

  it('min rule on varchar (type mismatch) → silently skipped', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'varchar', { isNullable: false })] }),
      'create',
      () => null,
      () => [mkRule('min', { v: 100 })],
    );
    expect(s.safeParse({ f: 'short' }).success).toBe(true);
  });

  it('format uuid on int (type mismatch) → silently skipped', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'int', { isNullable: false })] }),
      'create',
      () => null,
      () => [mkRule('format', { v: 'uuid' })],
    );
    expect(s.safeParse({ f: 42 }).success).toBe(true);
  });

  it('unknown ruleType → no-op', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'varchar', { isNullable: false })] }),
      'create',
      () => null,
      () => [mkRule('nonexistent_rule_type', { v: 1 })],
    );
    expect(s.safeParse({ f: 'x' }).success).toBe(true);
  });

  it('duplicate same ruleType applies both (last effective)', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'int', { isNullable: false })] }),
      'create',
      () => null,
      () => [mkRule('min', { v: 5 }), mkRule('min', { v: 10 })],
    );
    expect(s.safeParse({ f: 8 }).success).toBe(false);
    expect(s.safeParse({ f: 10 }).success).toBe(true);
  });
});

describe('buildZodFromMetadata — null/undefined handling for nullable', () => {
  it('nullable column accepts both undefined (absent) and null (explicit)', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'varchar', { isNullable: true })] }),
    );
    expect(s.safeParse({}).success).toBe(true);
    expect(s.safeParse({ f: null }).success).toBe(true);
    expect(s.safeParse({ f: 'x' }).success).toBe(true);
  });

  it('non-nullable column rejects both undefined and null on create', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'varchar', { isNullable: false })] }),
    );
    expect(s.safeParse({}).success).toBe(false);
    expect(s.safeParse({ f: null }).success).toBe(false);
    expect(s.safeParse({ f: 'x' }).success).toBe(true);
  });

  it('non-nullable column on update → absent key pass, null still rejected', () => {
    const s = build(
      makeMeta({ columns: [col('f', 'varchar', { isNullable: false })] }),
      'update',
    );
    expect(s.safeParse({}).success).toBe(true);
    expect(s.safeParse({ f: null }).success).toBe(false);
  });
});

describe('buildZodFromMetadata — array relation edge cases', () => {
  const target = makeMeta({
    name: 'tag',
    validateBody: true,
    columns: [
      col('id', 'int', { isPrimary: true, isGenerated: true }),
      col('name', 'varchar', { isNullable: false }),
    ],
  });

  it('m2m with empty array → pass (optional, no minItems)', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'many-to-many', propertyName: 'tags', targetTable: 'tag' },
        ],
      }),
      'create',
      () => target,
    );
    expect(s.safeParse({ tags: [] }).success).toBe(true);
  });

  it('m2m with null item → fail', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'many-to-many', propertyName: 'tags', targetTable: 'tag' },
        ],
      }),
      'create',
      () => target,
    );
    expect(s.safeParse({ tags: [1, null, 2] }).success).toBe(false);
  });

  it('o2m with null item → fail', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'one-to-many', propertyName: 'items', targetTable: 'tag' },
        ],
      }),
      'create',
      () => target,
    );
    expect(s.safeParse({ items: [{ name: 'x' }, null] }).success).toBe(false);
  });

  it('array-select with mixed types → fail', () => {
    const s = build(
      makeMeta({
        columns: [col('f', 'array-select', { isNullable: false })],
      }),
    );
    expect(s.safeParse({ f: ['a', 1, 'b'] }).success).toBe(false);
  });

  it('m2m with duplicate ids → pass (no uniqueness enforced)', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'many-to-many', propertyName: 'tags', targetTable: 'tag' },
        ],
      }),
      'create',
      () => target,
    );
    expect(s.safeParse({ tags: [1, 1, 1] }).success).toBe(true);
  });

  it('o2m not array (null value) → fail', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'one-to-many', propertyName: 'items', targetTable: 'tag' },
        ],
      }),
      'create',
      () => target,
    );
    expect(s.safeParse({ items: null }).success).toBe(false);
  });
});

describe('buildZodFromMetadata — cascade target validateBody=false', () => {
  const looseTarget = {
    name: 'loose',
    validateBody: false,
    columns: [
      { id: 'cx', name: 'id', type: 'int', isPrimary: true, isGenerated: true },
      { id: 'cy', name: 'name', type: 'varchar', isNullable: false },
    ],
    relations: [],
  };

  it('m2o cascade OFF when target validateBody=false → nested create shape not enforced', () => {
    const s = build(
      makeMeta({
        relations: [
          { type: 'many-to-one', propertyName: 'other', targetTable: 'loose' },
        ],
      }),
      'create',
      () => looseTarget,
    );
    // Nested object without required 'name' should pass (no cascade into target schema)
    expect(s.safeParse({ other: { whatever: 1 } }).success).toBe(true);
    expect(s.safeParse({ other: 5 }).success).toBe(true);
  });
});
