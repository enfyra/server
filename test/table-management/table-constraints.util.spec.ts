import { describe, expect, it } from 'vitest';
import { normalizeTableConstraints } from '../../src/modules/table-management/utils/table-constraints.util';

describe('normalizeTableConstraints', () => {
  it('canonicalizes constraints and removes indexes covered by an exact unique', () => {
    const constraints = normalizeTableConstraints({
      uniques: JSON.stringify([['modelId'], ['legacy']]),
      indexes: [{ value: ['modelId'] }, ['legacy']],
      columns: [
        { name: 'modelName', isUnique: true },
        { name: 'legacy', isUnique: false },
      ],
      renames: new Map([['modelId', 'modelName']]),
      allowedFields: new Set(['modelName', 'legacy']),
    });

    expect(constraints).toEqual({
      uniques: [['modelName']],
      indexes: [['legacy']],
    });
  });
});
