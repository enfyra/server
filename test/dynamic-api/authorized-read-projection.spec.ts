import { describe, expect, it, vi } from 'vitest';
import { DynamicReadAuthorizationService } from '../../src/modules/dynamic-api/services/dynamic-read-authorization.service';
import { DynamicRepositoryReadService } from '../../src/modules/dynamic-api/services/dynamic-repository-read.service';

function fixture(options: { conditional?: boolean; root?: boolean; secure?: boolean } = {}) {
  const columns = [
    { name: 'id', type: 'int', isPrimary: true, isPublished: true },
    { name: 'salary', type: 'int', isPublished: true },
    { name: 'ownerId', type: 'varchar', isPublished: false },
    { name: 'title', type: 'varchar', isPublished: true },
  ];
  const employee = { name: 'employees', columns, relations: [{ propertyName: 'manager', targetTableName: 'managers', type: 'many-to-one', isPublished: true }] };
  const manager = { name: 'managers', columns: [{ name: 'id', type: 'int', isPrimary: true }, { name: 'name', type: 'varchar', isPublished: true }], relations: [] };
  const team = { name: 'teams', columns: [{ name: 'id', type: 'int', isPrimary: true }], relations: [{ propertyName: 'employees', targetTableName: 'employees', type: 'one-to-many', isPublished: true }] };
  const tables = new Map([['employees', employee], ['teams', team], ['managers', manager]]);
  const rows = [
    { id: 1, salary: 100, ownerId: 'reader', title: 'one' },
    { id: 2, salary: 200, ownerId: 'other', title: 'two' },
  ];
  const rules = [{ tableName: 'employees', columnName: 'salary', action: 'read', effect: 'deny', isEnabled: true, allowedUserIds: [], roleId: null, condition: options.conditional ? { ownerId: { _eq: '@USER.id' } } : null }];
  const registry = {
    lookupTableByName: (name: string) => tables.get(name),
    requireMetadata: () => ({ tables, tablesList: [...tables.values()] }),
    getFieldPermissionPoliciesFor: () => [{ rules }],
    getMaxQueryDepth: () => 10,
  };
  const select = (row: Record<string, unknown>, fields: string | string[] | undefined) => {
    const keys = fields === undefined || fields === '*' ? Object.keys(row) : Array.isArray(fields) ? fields : fields.split(',');
    return Object.fromEntries(keys.filter(key => key in row).map(key => [key, row[key]]));
  };
  const find = vi.fn(async (query: any) => ({
    data: query.table === 'teams'
      ? [{ ...select({ id: 1 }, query.fields), ...(query.deep?.employees ? { employees: rows.map(row => select(row, query.deep.employees.fields)) } : {}) }]
      : rows.map(row => select(row, query.fields)),
    meta: { filterCount: 2 },
  }));
  const reader = (tableName = 'employees') => new DynamicRepositoryReadService({
    context: { $user: { id: 'reader', isRootAdmin: options.root === true }, $query: {} } as any,
    enforceFieldPermission: options.secure !== false,
    queryBuilderService: { find, getPkField: () => 'id' } as any,
    readAuthorizationService: new DynamicReadAuthorizationService({ runtimeRegistryService: registry as any }),
    runtimeRegistryService: registry as any,
    tableName,
  });
  return { reader, find, rules };
}

describe('authorized read projection', () => {
  it.each(['salary', ['salary']])('does not widen a denied-only projection %j', async fields => {
    const { reader, find } = fixture();
    const result = await reader().find({ fields });
    expect(result).toEqual({ data: [{}, {}], meta: { filterCount: 2 } });
    expect(find.mock.calls[0][0].fields).toEqual([]);
  });

  it('retains allowed fields while removing denied fields', async () => {
    const { reader } = fixture();
    expect((await reader().find({ fields: 'id,salary' })).data).toEqual([{ id: 1 }, { id: 2 }]);
  });

  it.each([undefined, '', []])('preserves caller default projection semantics for %j', async fields => {
    const { reader } = fixture();
    expect((await reader().find({ fields })).data).toEqual([{ id: 1, title: 'one' }, { id: 2, title: 'two' }]);
  });

  it('evaluates published conditional deny with an unrequested unpublished dependency', async () => {
    const { reader, find } = fixture({ conditional: true });
    const result = await reader().find({ fields: 'salary' });
    expect(result.data).toEqual([{}, { salary: 200 }]);
    expect(find.mock.calls[0][0].fields).toContain('ownerId');
  });

  it('evaluates all field conditions against the same unmodified record', async () => {
    const { reader, rules } = fixture({ conditional: true });
    rules.unshift({ ...rules[0], columnName: 'ownerId', condition: null });
    expect((await reader().find({ fields: 'ownerId,salary' })).data).toEqual([{}, { salary: 200 }]);
  });

  it('preserves denied-only projection inside a relation', async () => {
    const { reader } = fixture();
    const result = await reader('teams').find({ fields: 'id', deep: { employees: { fields: ['salary'] } } });
    expect(result.data).toEqual([{ id: 1, employees: [{}, {}] }]);
  });

  it('loads conditional dependencies inside deep projections without exposing them', async () => {
    const { reader } = fixture({ conditional: true });
    const result = await reader('teams').find({ fields: 'id', deep: { employees: { fields: 'salary' } } });
    expect(result.data).toEqual([{ id: 1, employees: [{}, { salary: 200 }] }]);
  });

  it('does not return unexpected fields supplied by a deny-all backend result', async () => {
    const { reader, find } = fixture();
    find.mockImplementation(async () => ({ data: [{ id: 1, salary: 100, title: 'one' }], meta: { filterCount: 2 } }));
    expect((await reader().find({ fields: 'salary' })).data).toEqual([{}]);
  });

  it('does not expose published condition-only dependencies', async () => {
    const { reader, rules } = fixture({ conditional: true });
    rules[0].condition = { title: { _eq: 'one' } } as any;
    expect((await reader().find({ fields: 'salary' })).data).toEqual([{}, { salary: 200 }]);
  });

  it('retains default fields during conditional evaluation', async () => {
    const { reader } = fixture({ conditional: true });
    expect((await reader().find()).data).toEqual([{ id: 1, title: 'one' }, { id: 2, salary: 200, title: 'two' }]);
  });

  it('does not widen a requested relation identity with conditional dependencies', async () => {
    const { reader, rules, find } = fixture({ conditional: true });
    rules[0].condition = { manager: { name: { _eq: 'one' } } } as any;
    find.mockImplementation(async () => ({ data: [{ salary: 100, manager: { id: 7, name: 'one' } }, { salary: 200, manager: { id: 8, name: 'two' } }], meta: { filterCount: 2 } }));
    expect((await reader().find({ fields: 'salary,manager' })).data).toEqual([{ manager: { id: 7 } }, { salary: 200, manager: { id: 8 } }]);
  });

  it('does not fetch dependencies for disabled or unrelated conditional rules', async () => {
    const { reader, rules, find } = fixture({ conditional: true });
    rules[0].isEnabled = false;
    expect((await reader().find({ fields: 'salary' })).data).toEqual([{ salary: 100 }, { salary: 200 }]);
    expect(find.mock.calls[0][0].fields).toBe('salary');
  });

  it('preserves privileged root and trusted-repository reads', async () => {
    for (const options of [{ root: true }, { secure: false }]) {
      const { reader } = fixture(options);
      expect((await reader().find({ fields: 'salary' })).data).toEqual([{ salary: 100 }, { salary: 200 }]);
    }
  });
});
