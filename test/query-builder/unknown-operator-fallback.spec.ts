/**
 * Unknown operator handling in MongoQueryExecutor filter parsing.
 *
 * When the filter DSL contains an unrecognized operator (e.g. _foo),
 * the executor should silently skip it via `continue` instead of
 * producing an invalid where condition.
 */

describe('MongoQueryExecutor — unknown operator fallback', () => {
  /**
   * Mirrors the operator-mapping loop from MongoQueryExecutor.execute().
   * Returns the WhereCondition[] that would be built from a given filter value.
   */
  function parseOperators(
    field: string,
    value: Record<string, any>,
  ): Array<{ field: string; operator: string; value: any }> {
    const conditions: Array<{ field: string; operator: string; value: any }> =
      [];

    for (const [op, val] of Object.entries(value)) {
      let operator: string;
      if (op === '_eq') operator = '=';
      else if (op === '_neq') operator = '!=';
      else if (op === '_in') operator = 'in';
      else if (op === '_not_in' || op === '_nin') operator = 'not in';
      else if (op === '_gt') operator = '>';
      else if (op === '_gte') operator = '>=';
      else if (op === '_lt') operator = '<';
      else if (op === '_lte') operator = '<=';
      else if (op === '_contains') operator = '_contains';
      else if (op === '_starts_with') operator = '_starts_with';
      else if (op === '_ends_with') operator = '_ends_with';
      else if (op === '_between') operator = '_between';
      else if (op === '_is_null') operator = '_is_null';
      else if (op === '_is_not_null') operator = '_is_not_null';
      else continue;

      conditions.push({ field, operator, value: val });
    }

    return conditions;
  }

  it('skips unknown operator _foo without producing a condition', () => {
    const result = parseOperators('status', { _foo: 'bar' });
    expect(result).toHaveLength(0);
  });

  it('skips unknown operator _regex', () => {
    const result = parseOperators('name', { _regex: '^A' });
    expect(result).toHaveLength(0);
  });

  it('skips multiple unknown operators', () => {
    const result = parseOperators('field', {
      _custom1: 1,
      _custom2: 2,
      _xyz: 'a',
    });
    expect(result).toHaveLength(0);
  });

  it('processes known operators alongside unknown ones', () => {
    const result = parseOperators('age', {
      _gt: 18,
      _foo: 'ignored',
      _lte: 65,
      _bar: 'also_ignored',
    });
    expect(result).toHaveLength(2);
    expect(result[0]).toEqual({ field: 'age', operator: '>', value: 18 });
    expect(result[1]).toEqual({ field: 'age', operator: '<=', value: 65 });
  });

  it('handles all recognized operators correctly', () => {
    const ops: Record<string, string> = {
      _eq: '=',
      _neq: '!=',
      _in: 'in',
      _not_in: 'not in',
      _nin: 'not in',
      _gt: '>',
      _gte: '>=',
      _lt: '<',
      _lte: '<=',
      _contains: '_contains',
      _starts_with: '_starts_with',
      _ends_with: '_ends_with',
      _between: '_between',
      _is_null: '_is_null',
      _is_not_null: '_is_not_null',
    };

    for (const [dslOp, expectedOp] of Object.entries(ops)) {
      const result = parseOperators('f', { [dslOp]: 'v' });
      expect(result).toHaveLength(1);
      expect(result[0].operator).toBe(expectedOp);
    }
  });

  it('returns empty array when value object is empty', () => {
    const result = parseOperators('field', {});
    expect(result).toHaveLength(0);
  });

  it('does not produce conditions with raw operator text', () => {
    // Before the fix, unknown ops produced `operator = op.replace('_', ' ')`
    // which would create invalid conditions like { operator: ' foo' }.
    // Now they are skipped entirely.
    const result = parseOperators('x', {
      _foo: 1,
      _some_operator: 2,
      _: 3,
    });
    expect(result).toHaveLength(0);
    // Ensure no condition has a space-prefixed or underscore-prefixed operator
    for (const c of result) {
      expect(c.operator).not.toMatch(/^ /);
    }
  });
});
