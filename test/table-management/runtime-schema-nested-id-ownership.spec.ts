import { describe, expect, it } from 'vitest';
import { assertRuntimeNestedMetadataIdsOwned } from '../../src/modules/table-management/utils/runtime-schema-normalization.util';

describe('runtime nested metadata id ownership', () => {
  it('rejects a permission id moved from one column to another', () => {
    const before = {
      name: 'posts',
      columns: [
        { id: 1, name: 'title', fieldPermissions: [{ id: 10, action: 'read' }] },
        { id: 2, name: 'secret', fieldPermissions: [] },
      ],
      relations: [],
    };
    const after = {
      ...before,
      columns: [
        { id: 1, name: 'title', fieldPermissions: [] },
        { id: 2, name: 'secret', fieldPermissions: [{ id: 10, action: 'deny' }] },
      ],
      relations: [],
    };

    expect(() => assertRuntimeNestedMetadataIdsOwned('update', before, after)).toThrow(/owned by column/);
  });

  it('rejects a nested permission id reused twice in one aggregate', () => {
    const before = {
      name: 'posts',
      columns: [{ id: 1, name: 'title', fieldPermissions: [{ id: 10 }] }],
      relations: [],
    };
    const after = {
      ...before,
      columns: [{
        id: 1,
        name: 'title',
        fieldPermissions: [{ id: 10 }, { id: 10 }],
      }],
      relations: [],
    };

    expect(() => assertRuntimeNestedMetadataIdsOwned('update', before, after)).toThrow(/more than once/);
  });
});
