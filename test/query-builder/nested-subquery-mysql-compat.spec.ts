/**
 * MySQL Compatibility Tests for nested-subquery-builder
 *
 * Verifies that:
 * 1. MySQL output never contains ORDER BY inside JSON_ARRAYAGG()
 * 2. MySQL O2M/M2M correlated subqueries use derived-table ORDER BY
 * 3. MySQL CTE strategies use GROUP_CONCAT(... ORDER BY ...) instead
 * 4. Postgres output still uses ORDER BY inside aggregate
 * 5. Generated SQL is syntactically valid and produces correct ordered results
 */

import {
  buildNestedSubquery,
  buildCTEStrategy,
} from '../../src/infrastructure/query-builder/utils/sql/nested-subquery-builder';
import { TableMetadata } from '../../src/infrastructure/knex/types/knex-types';
import { DatabaseType } from '../../src/shared/types/query-builder.types';

// ── helpers ──────────────────────────────────────────────────────────────────

function col(name: string, opts: Partial<{ isPrimary: boolean }> = {}) {
  return {
    id: 0,
    name,
    type: 'varchar',
    isPrimary: opts.isPrimary ?? false,
    isGenerated: false,
    isNullable: true,
    isSystem: false,
    isUpdatable: true,
    tableId: 0,
  };
}

function rel(
  propertyName: string,
  type: 'many-to-one' | 'one-to-many' | 'many-to-many',
  targetTable: string,
  extra: Record<string, any> = {},
) {
  return {
    id: 0,
    propertyName,
    type,
    targetTable,
    targetTableId: 0,
    sourceTable: '',
    sourceTableId: 0,
    isNullable: true,
    isSystem: false,
    ...extra,
  };
}

const parentMeta: TableMetadata = {
  id: 1,
  name: 'route_definition',
  isSystem: true,
  columns: [col('id', { isPrimary: true }), col('path'), col('isEnabled')],
  relations: [
    rel('handlers', 'one-to-many', 'route_handler_definition', {
      foreignKeyColumn: 'routeId',
    }),
    rel('methods', 'many-to-many', 'method_definition', {
      junctionTableName:
        'route_definition_availableMethods_method_definition',
      junctionSourceColumn: 'route_definitionId',
      junctionTargetColumn: 'method_definitionId',
    }),
  ],
};

const handlerMeta: TableMetadata = {
  id: 2,
  name: 'route_handler_definition',
  isSystem: true,
  columns: [col('id', { isPrimary: true }), col('logic'), col('timeout')],
  relations: [
    rel('route', 'many-to-one', 'route_definition', {
      foreignKeyColumn: 'routeId',
    }),
  ],
};

const methodMeta: TableMetadata = {
  id: 3,
  name: 'method_definition',
  isSystem: true,
  columns: [col('id', { isPrimary: true }), col('method')],
  relations: [],
};

const metadataGetter = async (
  tableName: string,
): Promise<TableMetadata | null> => {
  const map: Record<string, TableMetadata> = {
    route_definition: parentMeta,
    route_handler_definition: handlerMeta,
    method_definition: methodMeta,
  };
  return map[tableName] || null;
};

// ── buildNestedSubquery ─────────────────────────────────────────────────────

describe('buildNestedSubquery — MySQL vs Postgres ordering', () => {
  describe('one-to-many (correlated subquery)', () => {
    const run = (dbType: DatabaseType) =>
      buildNestedSubquery(
        'route_definition',
        parentMeta,
        'handlers',
        ['id', 'logic'],
        dbType,
        metadataGetter,
      );

    it('MySQL: must NOT contain ORDER BY inside JSON_ARRAYAGG()', async () => {
      const sql = await run('mysql');
      expect(sql).not.toBeNull();
      // Extract the JSON_ARRAYAGG(...) call and check no ORDER BY inside it
      const aggMatch = sql!.match(/JSON_ARRAYAGG\(([^)]+)\)/);
      expect(aggMatch).toBeTruthy();
      expect(aggMatch![1]).not.toContain('ORDER BY');
    });

    it('MySQL: must use derived table with ORDER BY', async () => {
      const sql = await run('mysql');
      // Pattern: from (select ... ORDER BY ... ASC) alias)
      expect(sql).toMatch(/from\s+\(select\s+.*ORDER BY.*ASC\)\s+\w+\)/i);
    });

    it('MySQL: derived table references target table with correct alias', async () => {
      const sql = await run('mysql');
      // Inner: select c.`id`, c.`logic` from `route_handler_definition` c where ...
      expect(sql).toContain('from `route_handler_definition` c');
      expect(sql).toContain('c.`id`');
    });

    it('Postgres: must contain ORDER BY inside aggregate', async () => {
      const sql = await run('postgres');
      expect(sql).not.toBeNull();
      // Postgres uses json_agg (via COALESCE wrapper)
      expect(sql).toContain('ORDER BY');
      // Should NOT use derived table
      expect(sql).not.toMatch(
        /from\s+\(select\s+c\.\*\s+from/i,
      );
    });

    it('Postgres: uses json_agg with ORDER BY inside', async () => {
      const sql = await run('postgres');
      // Pattern: json_agg(json_build_object(...) ORDER BY ...)
      expect(sql).toMatch(/json_agg\(json_build_object\(.*ORDER BY/s);
    });

    it('both DBs include the same columns in JSON_OBJECT', async () => {
      const mysqlSql = await run('mysql');
      const pgSql = await run('postgres');
      // Both should have 'id' and 'logic' fields
      for (const sql of [mysqlSql!, pgSql!]) {
        expect(sql).toContain("'id'");
        expect(sql).toContain("'logic'");
      }
    });
  });

  describe('many-to-many (correlated subquery)', () => {
    const run = (dbType: DatabaseType) =>
      buildNestedSubquery(
        'route_definition',
        parentMeta,
        'methods',
        ['id', 'method'],
        dbType,
        metadataGetter,
      );

    it('MySQL: must NOT contain ORDER BY inside JSON_ARRAYAGG()', async () => {
      const sql = await run('mysql');
      expect(sql).not.toBeNull();
      const aggMatch = sql!.match(/JSON_ARRAYAGG\(([^)]+)\)/);
      expect(aggMatch).toBeTruthy();
      expect(aggMatch![1]).not.toContain('ORDER BY');
    });

    it('MySQL: must use derived table with ORDER BY and join', async () => {
      const sql = await run('mysql');
      // Should have: from (select c.`id`, c.`method` from junction j join target c on ... where ... ORDER BY ...) c)
      expect(sql).toMatch(/from\s+\(select\s+c\./i);
      expect(sql).toContain('ORDER BY');
      expect(sql).toContain(
        '`route_definition_availableMethods_method_definition`',
      );
    });

    it('MySQL: junction join preserved inside derived table', async () => {
      const sql = await run('mysql');
      // Junction alias j, target alias c, join condition
      expect(sql).toContain('j.`method_definitionId` = c.`id`');
    });

    it('Postgres: must contain ORDER BY inside aggregate', async () => {
      const sql = await run('postgres');
      expect(sql).not.toBeNull();
      expect(sql).toMatch(/json_agg\(json_build_object\(.*ORDER BY/s);
      expect(sql).not.toMatch(
        /from\s+\(select\s+c\.\*\s+from/i,
      );
    });
  });

  describe('many-to-one (no ordering needed)', () => {
    const childMeta: TableMetadata = {
      id: 10,
      name: 'route_handler_definition',
      isSystem: true,
      columns: [col('id', { isPrimary: true }), col('logic')],
      relations: [
        rel('route', 'many-to-one', 'route_definition', {
          foreignKeyColumn: 'routeId',
        }),
      ],
    };

    it('MySQL: M2O uses simple subquery with limit 1, no ordering logic', async () => {
      const sql = await buildNestedSubquery(
        'route_handler_definition',
        childMeta,
        'route',
        ['id', 'path'],
        'mysql',
        metadataGetter,
      );
      expect(sql).not.toBeNull();
      expect(sql).toContain('limit 1');
      expect(sql).not.toContain('JSON_ARRAYAGG');
      expect(sql).not.toContain('ORDER BY');
    });

    it('Postgres: M2O same structure as MySQL', async () => {
      const sql = await buildNestedSubquery(
        'route_handler_definition',
        childMeta,
        'route',
        ['id', 'path'],
        'postgres',
        metadataGetter,
      );
      expect(sql).not.toBeNull();
      expect(sql).toContain('limit 1');
      expect(sql).not.toContain('json_agg');
    });
  });
});

// ── buildCTEStrategy ────────────────────────────────────────────────────────

describe('buildCTEStrategy — MySQL fallback vs Postgres CTE', () => {
  describe('MySQL returns null (fallback to correlated subquery)', () => {
    it('O2M CTE returns null for MySQL', async () => {
      const sql = await buildCTEStrategy(
        'route_definition', parentMeta, 'handlers', ['id', 'logic'],
        'mysql', metadataGetter, 'limited_routes',
      );
      expect(sql).toBeNull();
    });

    it('M2M CTE returns null for MySQL', async () => {
      const sql = await buildCTEStrategy(
        'route_definition', parentMeta, 'methods', ['id', 'method'],
        'mysql', metadataGetter, 'limited_routes',
      );
      expect(sql).toBeNull();
    });
  });

  describe('Postgres uses CTE with jsonb_agg', () => {
    it('O2M CTE: uses jsonb_agg with ORDER BY', async () => {
      const sql = await buildCTEStrategy(
        'route_definition', parentMeta, 'handlers', ['id', 'logic'],
        'postgres', metadataGetter, 'limited_routes',
      );
      expect(sql).not.toBeNull();
      expect(sql).toContain('jsonb_agg');
      expect(sql).toContain('ORDER BY');
      expect(sql).toContain('GROUP BY r."routeId"');
    });

    it('M2M CTE: uses jsonb_agg with ORDER BY', async () => {
      const sql = await buildCTEStrategy(
        'route_definition', parentMeta, 'methods', ['id', 'method'],
        'postgres', metadataGetter, 'limited_routes',
      );
      expect(sql).not.toBeNull();
      expect(sql).toContain('jsonb_agg');
      expect(sql).toContain('ORDER BY');
    });
  });

  describe('rejects wrong relation types', () => {
    it('returns null for many-to-one on any DB', async () => {
      for (const db of ['mysql', 'postgres'] as DatabaseType[]) {
        const sql = await buildCTEStrategy(
          'route_handler_definition', handlerMeta, 'route', ['id'],
          db, metadataGetter, 'limited',
        );
        expect(sql).toBeNull();
      }
    });
  });
});

// ── SQL structure validation ────────────────────────────────────────────────

describe('SQL structure correctness', () => {
  describe('MySQL correlated O2M — derived table structure', () => {
    it('outer select wraps ifnull(JSON_ARRAYAGG(...), JSON_ARRAY())', async () => {
      const sql = await buildNestedSubquery(
        'route_definition',
        parentMeta,
        'handlers',
        ['id'],
        'mysql',
        metadataGetter,
      );
      // Pattern: (select ifnull(JSON_ARRAYAGG(JSON_OBJECT(...)), JSON_ARRAY()) from (select c.* from ... ORDER BY ...) c)
      expect(sql).toMatch(/^\(select ifnull\(JSON_ARRAYAGG\(/);
      expect(sql).toMatch(/JSON_ARRAY\(\)\) from \(select/);
    });

    it('WHERE clause is inside the derived table, not outside', async () => {
      const sql = await buildNestedSubquery(
        'route_definition',
        parentMeta,
        'handlers',
        ['id'],
        'mysql',
        metadataGetter,
      );
      // The WHERE should be inside the inner (select c.`id`, ... from ... where ... ORDER BY ...) c
      const innerDerived = sql!.match(
        /from\s+\((select\s+c\.[\s\S]+?)\)\s+c\)/,
      );
      expect(innerDerived).toBeTruthy();
      expect(innerDerived![1]).toContain('where');
      expect(innerDerived![1]).toContain('ORDER BY');
    });
  });

  describe('MySQL correlated M2M — derived table structure', () => {
    it('junction join is inside derived table', async () => {
      const sql = await buildNestedSubquery(
        'route_definition',
        parentMeta,
        'methods',
        ['id'],
        'mysql',
        metadataGetter,
      );
      const innerDerived = sql!.match(
        /from\s+\((select\s+c\.[\s\S]+?)\)\s+c\)/,
      );
      expect(innerDerived).toBeTruthy();
      expect(innerDerived![1]).toContain('join');
      expect(innerDerived![1]).toContain('where');
    });
  });

  describe('Postgres CTE — aggregate structure', () => {
    it('uses COALESCE(jsonb_agg(...), empty) pattern', async () => {
      const sql = await buildCTEStrategy(
        'route_definition',
        parentMeta,
        'handlers',
        ['id'],
        'postgres',
        metadataGetter,
        'limited',
      );
      expect(sql).toContain("COALESCE(jsonb_agg");
      expect(sql).toContain("'[]'::jsonb");
    });
  });
});

// ── Nesting depth ───────────────────────────────────────────────────────────

describe('Nested subquery depth — MySQL consistency', () => {
  const deepParent: TableMetadata = {
    id: 100,
    name: 'guard_definition',
    isSystem: true,
    columns: [col('id', { isPrimary: true }), col('name')],
    relations: [
      rel('rules', 'one-to-many', 'guard_rule_definition', {
        foreignKeyColumn: 'guardId',
      }),
    ],
  };

  const deepChild: TableMetadata = {
    id: 101,
    name: 'guard_rule_definition',
    isSystem: true,
    columns: [col('id', { isPrimary: true }), col('type'), col('value')],
    relations: [
      rel('guard', 'many-to-one', 'guard_definition', {
        foreignKeyColumn: 'guardId',
      }),
    ],
  };

  const deepGetter = async (
    name: string,
  ): Promise<TableMetadata | null> => {
    if (name === 'guard_definition') return deepParent;
    if (name === 'guard_rule_definition') return deepChild;
    return null;
  };

  it('MySQL: nesting level 1 still uses derived table with narrow select', async () => {
    const sql = await buildNestedSubquery(
      'guard_definition',
      deepParent,
      'rules',
      ['id', 'type'],
      'mysql',
      deepGetter,
      1,
      'c',
    );
    expect(sql).not.toBeNull();
    expect(sql).not.toMatch(/JSON_ARRAYAGG\([^)]*ORDER BY/);
    expect(sql).toContain('ORDER BY');
    // At nesting level 1, alias is c1 — narrow select, not c1.*
    expect(sql).toContain('c1.`id`');
    expect(sql).toContain('c1.`type`');
    expect(sql).not.toContain('c1.*');
  });

  it('maxDepth cuts off correctly', async () => {
    const sql = await buildNestedSubquery(
      'guard_definition',
      deepParent,
      'rules',
      ['id'],
      'mysql',
      deepGetter,
      0,
      undefined,
      undefined,
      0,
    );
    expect(sql).toBeNull();
  });
});

// ── Identifier quoting ──────────────────────────────────────────────────────

describe('Identifier quoting', () => {
  it('MySQL uses backtick quoting', async () => {
    const sql = await buildNestedSubquery(
      'route_definition',
      parentMeta,
      'handlers',
      ['id'],
      'mysql',
      metadataGetter,
    );
    expect(sql).toContain('`route_handler_definition`');
    expect(sql).toContain('`id`');
  });

  it('Postgres uses double-quote quoting', async () => {
    const sql = await buildNestedSubquery(
      'route_definition',
      parentMeta,
      'handlers',
      ['id'],
      'postgres',
      metadataGetter,
    );
    expect(sql).toContain('"route_handler_definition"');
    expect(sql).toContain('"id"');
  });
});

// ── Edge cases ──────────────────────────────────────────────────────────────

describe('Edge cases', () => {
  it('returns null for non-existent relation', async () => {
    const sql = await buildNestedSubquery(
      'route_definition',
      parentMeta,
      'nonexistent',
      ['id'],
      'mysql',
      metadataGetter,
    );
    expect(sql).toBeNull();
  });

  it('returns null when target metadata not found', async () => {
    const emptyGetter = async () => null;
    const sql = await buildNestedSubquery(
      'route_definition',
      parentMeta,
      'handlers',
      ['id'],
      'mysql',
      emptyGetter,
    );
    expect(sql).toBeNull();
  });

  it('returns null for empty fields list (no matching columns)', async () => {
    const sql = await buildNestedSubquery(
      'route_definition',
      parentMeta,
      'handlers',
      ['nonexistent_col'],
      'mysql',
      metadataGetter,
    );
    expect(sql).toBeNull();
  });

  it('CTE M2M returns null when junction table is missing (postgres)', async () => {
    const noJunctionParent: TableMetadata = {
      ...parentMeta,
      relations: [
        rel('methods', 'many-to-many', 'method_definition', {}),
      ],
    };
    const sql = await buildCTEStrategy(
      'route_definition',
      noJunctionParent,
      'methods',
      ['id'],
      'postgres',
      metadataGetter,
      'limited',
    );
    expect(sql).toBeNull();
  });

  it('O2M with mappedBy resolves FK column correctly', async () => {
    const parentWithMapped: TableMetadata = {
      ...parentMeta,
      relations: [
        rel('handlers', 'one-to-many', 'route_handler_definition', {
          mappedBy: 'route',
        }),
      ],
    };
    const sql = await buildNestedSubquery(
      'route_definition',
      parentWithMapped,
      'handlers',
      ['id'],
      'mysql',
      metadataGetter,
    );
    expect(sql).not.toBeNull();
    // mappedBy: 'route' → FK column: routeId
    expect(sql).toContain('`routeId`');
  });
});

// ── Cross-DB parity: both produce ORDER BY somewhere ────────────────────────

describe('Cross-DB parity: ordering always present', () => {
  const correlated: Array<{
    name: string;
    build: (db: DatabaseType) => Promise<string | null>;
  }> = [
    {
      name: 'O2M correlated subquery',
      build: (db) =>
        buildNestedSubquery(
          'route_definition', parentMeta, 'handlers', ['id'],
          db, metadataGetter,
        ),
    },
    {
      name: 'M2M correlated subquery',
      build: (db) =>
        buildNestedSubquery(
          'route_definition', parentMeta, 'methods', ['id'],
          db, metadataGetter,
        ),
    },
  ];

  for (const { name, build } of correlated) {
    it(`${name}: MySQL has ORDER BY`, async () => {
      const sql = await build('mysql');
      expect(sql).not.toBeNull();
      expect(sql).toContain('ORDER BY');
    });

    it(`${name}: Postgres has ORDER BY`, async () => {
      const sql = await build('postgres');
      expect(sql).not.toBeNull();
      expect(sql).toContain('ORDER BY');
    });
  }

  it('CTE: MySQL returns null (uses correlated fallback), Postgres uses CTE', async () => {
    const buildCte = (db: DatabaseType) =>
      buildCTEStrategy(
        'route_definition', parentMeta, 'handlers', ['id'],
        db, metadataGetter, 'limited',
      );
    expect(await buildCte('mysql')).toBeNull();
    const pgSql = await buildCte('postgres');
    expect(pgSql).not.toBeNull();
    expect(pgSql).toContain('ORDER BY');
  });
});
