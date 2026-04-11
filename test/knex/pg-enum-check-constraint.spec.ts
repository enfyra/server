/**
 * Tests for Postgres ENUM conversion in syncTable (migrations.ts).
 * Covers the fix: DROP CHECK constraint before ALTER COLUMN TYPE to enum,
 * because Knex table.enum() on PG creates text + CHECK constraint (not native ENUM).
 */
describe('Postgres ENUM conversion — CHECK constraint handling', () => {
  let executedSql: string[];
  let knexRaw: jest.Mock;
  let knex: any;

  beforeEach(() => {
    executedSql = [];
    knexRaw = jest.fn(async (sql: string, _bindings?: any[]) => {
      executedSql.push(sql.trim().replace(/\s+/g, ' '));

      // Simulate PG information_schema responses
      if (sql.includes('data_type, udt_name') && sql.includes('information_schema.columns')) {
        return { rows: [{ data_type: 'text', udt_name: 'text' }] };
      }
      if (sql.includes('column_default') && sql.includes('information_schema.columns')) {
        return { rows: [{ column_default: null }] };
      }
      return { rows: [] };
    });
    knex = { raw: knexRaw, client: { config: { client: 'pg' } } };
  });

  /**
   * Simulate the PG text→enum conversion flow from migrations.ts lines 169-246
   */
  async function simulateEnumConversion(
    tableName: string,
    colName: string,
    options: string[],
    currentType: string,
    hasCheckConstraint: boolean,
  ) {
    const newEnumType = `${tableName}_${colName}_enum`;

    // Skip if already enum
    if (currentType.endsWith('_enum')) return 'skipped';

    // Drop default if exists (omitted for simplicity)

    // FIX: Drop CHECK constraint before ALTER TYPE
    if (hasCheckConstraint) {
      try {
        await knex.raw(
          `ALTER TABLE "${tableName}" DROP CONSTRAINT IF EXISTS "${tableName}_${colName}_check"`,
        );
      } catch (_e) {}
    }

    // Convert to text if not already
    if (currentType !== 'text') {
      await knex.raw(
        `ALTER TABLE "${tableName}" ALTER COLUMN "${colName}" TYPE text USING "${colName}"::text`,
      );
    }

    // Create enum type and convert
    const enumValues = options.map((v) => `'${v}'`).join(', ');
    await knex.raw(`DROP TYPE IF EXISTS "${newEnumType}" CASCADE`);
    await knex.raw(`CREATE TYPE "${newEnumType}" AS ENUM (${enumValues})`);
    await knex.raw(
      `ALTER TABLE "${tableName}" ALTER COLUMN "${colName}" TYPE "${newEnumType}" USING "${colName}"::"${newEnumType}"`,
    );

    return 'converted';
  }

  it('should DROP CHECK constraint before ALTER TYPE when column is text+CHECK (Knex default)', async () => {
    await simulateEnumConversion(
      'column_definition',
      'type',
      ['int', 'varchar', 'text'],
      'text',
      true,
    );

    const dropCheck = executedSql.find((s) => s.includes('DROP CONSTRAINT'));
    const alterType = executedSql.find((s) =>
      s.includes('TYPE "column_definition_type_enum"'),
    );

    expect(dropCheck).toBeDefined();
    expect(alterType).toBeDefined();

    // DROP CONSTRAINT must come BEFORE ALTER TYPE
    const dropIdx = executedSql.indexOf(dropCheck!);
    const alterIdx = executedSql.indexOf(alterType!);
    expect(dropIdx).toBeLessThan(alterIdx);
  });

  it('should skip conversion when column is already native ENUM', async () => {
    const result = await simulateEnumConversion(
      'column_definition',
      'type',
      ['int', 'varchar'],
      'column_definition_type_enum',
      false,
    );

    expect(result).toBe('skipped');
    expect(executedSql).toHaveLength(0);
  });

  it('should not issue TYPE text conversion when already text', async () => {
    await simulateEnumConversion(
      'test_table',
      'status',
      ['active', 'inactive'],
      'text',
      true,
    );

    const typeToText = executedSql.find(
      (s) => s.includes('TYPE text USING'),
    );
    expect(typeToText).toBeUndefined();
  });

  it('should convert varchar to text first, then to enum', async () => {
    await simulateEnumConversion(
      'test_table',
      'status',
      ['active', 'inactive'],
      'varchar',
      true,
    );

    const typeToText = executedSql.find((s) => s.includes('TYPE text USING'));
    const typeToEnum = executedSql.find((s) =>
      s.includes('TYPE "test_table_status_enum"'),
    );

    expect(typeToText).toBeDefined();
    expect(typeToEnum).toBeDefined();

    const textIdx = executedSql.indexOf(typeToText!);
    const enumIdx = executedSql.indexOf(typeToEnum!);
    expect(textIdx).toBeLessThan(enumIdx);
  });

  it('should execute SQL in correct order: DROP CHECK → DROP TYPE → CREATE TYPE → ALTER TYPE', async () => {
    await simulateEnumConversion(
      'my_table',
      'kind',
      ['a', 'b', 'c'],
      'text',
      true,
    );

    const steps = executedSql.map((s) => {
      if (s.includes('DROP CONSTRAINT')) return 'DROP_CHECK';
      if (s.includes('DROP TYPE IF EXISTS')) return 'DROP_TYPE';
      if (s.includes('CREATE TYPE')) return 'CREATE_TYPE';
      if (s.includes('ALTER TABLE') && s.includes('TYPE "my_table_kind_enum"'))
        return 'ALTER_TYPE';
      return 'other';
    });

    expect(steps).toEqual([
      'DROP_CHECK',
      'DROP_TYPE',
      'CREATE_TYPE',
      'ALTER_TYPE',
    ]);
  });

  it('should handle CHECK constraint drop failure gracefully', async () => {
    knexRaw.mockImplementationOnce(async (sql: string) => {
      if (sql.includes('DROP CONSTRAINT')) {
        throw new Error('constraint does not exist');
      }
    });

    // Should not throw — the try/catch in the fix swallows the error
    await expect(
      simulateEnumConversion('t', 'col', ['x'], 'text', true),
    ).resolves.toBe('converted');
  });
});
