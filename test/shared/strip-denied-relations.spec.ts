const TABLE_META: Record<string, any> = {
  main_table: {
    name: 'main_table',
    columns: [
      { name: 'id', isPrimary: true },
      { name: 'name' },
      { name: 'secret', isPublished: false },
    ],
    relations: [
      { propertyName: 'author', targetTable: 'user_table', isPublished: true },
      { propertyName: 'category', targetTable: 'category_table', isPublished: true },
    ],
  },
  user_table: {
    name: 'user_table',
    columns: [
      { name: 'id', isPrimary: true },
      { name: 'email' },
      { name: 'salary', isPublished: false },
    ],
    relations: [
      { propertyName: 'company', targetTable: 'company_table', isPublished: true },
    ],
  },
  company_table: {
    name: 'company_table',
    columns: [{ name: 'id', isPrimary: true }, { name: 'name' }],
    relations: [],
  },
};

function makeRelDenyPolicy(tableName: string, relName: string) {
  return {
    unconditionalAllowedColumns: new Set<string>(),
    unconditionalAllowedRelations: new Set<string>(),
    unconditionalDeniedColumns: new Set<string>(),
    unconditionalDeniedRelations: new Set([relName]),
    rules: [
      {
        id: 1,
        isEnabled: true,
        action: 'read' as const,
        effect: 'deny' as const,
        tableName,
        roleId: null,
        allowedUserIds: [],
        columnName: null,
        relationPropertyName: relName,
        condition: null,
      },
    ],
  };
}

function makeColDenyPolicy(tableName: string, colName: string) {
  return {
    unconditionalAllowedColumns: new Set<string>(),
    unconditionalAllowedRelations: new Set<string>(),
    unconditionalDeniedColumns: new Set([colName]),
    unconditionalDeniedRelations: new Set<string>(),
    rules: [
      {
        id: 1,
        isEnabled: true,
        action: 'read' as const,
        effect: 'deny' as const,
        tableName,
        roleId: null,
        allowedUserIds: [],
        columnName: colName,
        relationPropertyName: null,
        condition: null,
      },
    ],
  };
}

function makeConditionalAllowPolicy(tableName: string, colName: string) {
  return {
    unconditionalAllowedColumns: new Set<string>(),
    unconditionalAllowedRelations: new Set<string>(),
    unconditionalDeniedColumns: new Set<string>(),
    unconditionalDeniedRelations: new Set<string>(),
    rules: [
      {
        id: 1,
        isEnabled: true,
        action: 'read' as const,
        effect: 'allow' as const,
        tableName,
        roleId: null,
        allowedUserIds: [],
        columnName: colName,
        relationPropertyName: null,
        condition: { owner: { _eq: '@USER.id' } },
      },
    ],
  };
}

function makeRepo({
  enforceFieldPermission = true,
  isRootAdmin = false,
  tableMetaMap = TABLE_META,
  policiesMap = {} as Record<string, any[]>,
} = {}) {
  const { DynamicRepository } = require('../../src/modules/dynamic-api/repositories/dynamic.repository');
  const repo = Object.create(DynamicRepository.prototype);
  repo.enforceFieldPermission = enforceFieldPermission;
  repo.tableName = 'main_table';
  repo.context = { $user: { id: 1, role: { id: '2' }, isRootAdmin } };
  repo.metadataCacheService = {
    lookupTableByName: jest.fn().mockImplementation(async (name: string) => tableMetaMap[name] ?? null),
  };
  repo.fieldPermissionCacheService = {
    getPoliciesFor: jest.fn().mockImplementation(
      async (_user: any, tableName: string, action: string) =>
        policiesMap[`${tableName}:${action}`] ?? [],
    ),
    ensureLoaded: jest.fn().mockResolvedValue(undefined),
  };
  return repo;
}

async function strip(repo: any, tableName: string, fields: any, deep: any) {
  return (repo as any).stripDeniedFields(tableName, fields, deep);
}

describe('stripDeniedFields — bypass conditions', () => {
  it('returns as-is when enforceFieldPermission is false', async () => {
    const repo = makeRepo({ enforceFieldPermission: false });
    const result = await strip(repo, 'main_table', ['id', 'author.name'], { author: {} });
    expect(result.fields).toEqual(['id', 'author.name']);
    expect(result.deep).toEqual({ author: {} });
  });

  it('returns as-is when user is rootAdmin', async () => {
    const repo = makeRepo({ isRootAdmin: true });
    const result = await strip(repo, 'main_table', ['id', 'author.name'], { author: {} });
    expect(result.fields).toEqual(['id', 'author.name']);
    expect(result.deep).toEqual({ author: {} });
  });

  it('does not call metadataCacheService when enforceFieldPermission is false', async () => {
    const repo = makeRepo({ enforceFieldPermission: false });
    await strip(repo, 'main_table', ['id'], {});
    expect(repo.metadataCacheService.lookupTableByName).not.toHaveBeenCalled();
  });

  it('does not call getPoliciesFor when rootAdmin', async () => {
    const repo = makeRepo({ isRootAdmin: true });
    await strip(repo, 'main_table', ['id'], {});
    expect(repo.fieldPermissionCacheService.getPoliciesFor).not.toHaveBeenCalled();
  });
});

describe('stripDeniedFields — relations', () => {
  it('strips denied relation from fields string', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeRelDenyPolicy('main_table', 'author')] },
    });
    const result = await strip(repo, 'main_table', 'id,name,author.email', undefined);
    expect(result.fields).toBe('id,name');
  });

  it('strips denied relation from fields array', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeRelDenyPolicy('main_table', 'author')] },
    });
    const result = await strip(repo, 'main_table', ['id', 'name', 'author.email'], undefined);
    expect(result.fields).toEqual(['id', 'name']);
  });

  it('strips denied relation from deep', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeRelDenyPolicy('main_table', 'author')] },
    });
    const result = await strip(repo, 'main_table', ['id'], { author: {}, category: {} });
    expect(result.deep).not.toHaveProperty('author');
    expect(result.deep).toHaveProperty('category');
  });

  it('keeps allowed relations untouched', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', ['id', 'author.name'], { author: {} });
    expect(result.fields).toEqual(['id', 'author.name']);
    expect(result.deep).toHaveProperty('author');
  });

  it('recursively strips denied nested relation from deep', async () => {
    const repo = makeRepo({
      policiesMap: { 'user_table:read': [makeRelDenyPolicy('user_table', 'company')] },
    });
    const deep = { author: { fields: ['id', 'email'], deep: { company: { fields: ['id'] } } } };
    const result = await strip(repo, 'main_table', ['id', 'author'], deep);
    expect(result.deep?.author?.deep).not.toHaveProperty('company');
  });

  it('strips nested relation from deep.fields string', async () => {
    const repo = makeRepo({
      policiesMap: { 'user_table:read': [makeRelDenyPolicy('user_table', 'company')] },
    });
    const deep = { author: { fields: 'id,email,company.name', deep: { company: { fields: ['name'] } } } };
    const result = await strip(repo, 'main_table', ['id', 'author'], deep);
    expect(result.deep?.author?.fields).toBe('id,email');
    expect(result.deep?.author?.deep).not.toHaveProperty('company');
  });
});

describe('stripDeniedFields — columns', () => {
  it('strips denied column from explicit fields string', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeColDenyPolicy('main_table', 'name')] },
    });
    const result = await strip(repo, 'main_table', 'id,name', undefined);
    expect(result.fields).toBe('id');
  });

  it('strips denied column from explicit fields array', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeColDenyPolicy('main_table', 'name')] },
    });
    const result = await strip(repo, 'main_table', ['id', 'name'], undefined);
    expect(result.fields).toEqual(['id']);
  });

  it('never strips primary key column', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeColDenyPolicy('main_table', 'id')] },
    });
    const result = await strip(repo, 'main_table', ['id', 'name'], undefined);
    expect(result.fields).toEqual(['id', 'name']);
  });

  it('strips both denied column and relation in one call', async () => {
    const repo = makeRepo({
      policiesMap: {
        'main_table:read': [
          makeColDenyPolicy('main_table', 'name'),
          makeRelDenyPolicy('main_table', 'author'),
        ],
      },
    });
    const result = await strip(repo, 'main_table', ['id', 'name', 'author'], { author: {} });
    expect(result.fields).toEqual(['id']);
    expect(result.deep).not.toHaveProperty('author');
  });

  it('strips isPublished:false column without explicit deny rule', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', 'id,name,secret', undefined);
    expect(result.fields).toBe('id,name');
  });
});

describe('stripDeniedFields — wildcard resolution', () => {
  it('resolves undefined fields to explicit column list and strips denied', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeRelDenyPolicy('main_table', 'author')] },
    });
    const result = await strip(repo, 'main_table', undefined, { author: {}, category: {} });
    const fields = String(result.fields).split(',');
    expect(fields).toContain('id');
    expect(fields).toContain('name');
    expect(fields).toContain('category');
    expect(fields).not.toContain('author');
    expect(result.deep).not.toHaveProperty('author');
  });

  it('resolves empty string to explicit column list and strips denied', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', '', undefined);
    const fields = String(result.fields).split(',');
    expect(fields).toContain('id');
    expect(fields).toContain('name');
    expect(fields).not.toContain('secret');
  });

  it('resolves "*" to explicit column list and strips denied', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', '*', undefined);
    const fields = String(result.fields).split(',');
    expect(fields).toContain('id');
    expect(fields).toContain('name');
    expect(fields).not.toContain('secret');
  });

  it('resolves empty array to explicit column list', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', [], undefined);
    const fields = String(result.fields).split(',');
    expect(fields).toContain('id');
    expect(fields).toContain('name');
  });

  it('includes deep relation names in resolved wildcard fields', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', undefined, { author: {}, category: {} });
    const fields = String(result.fields).split(',');
    expect(fields).toContain('author');
    expect(fields).toContain('category');
  });
});

function makeConditionalRelAllowPolicy(tableName: string, relName: string) {
  return {
    unconditionalAllowedColumns: new Set<string>(),
    unconditionalAllowedRelations: new Set<string>(),
    unconditionalDeniedColumns: new Set<string>(),
    unconditionalDeniedRelations: new Set<string>(),
    rules: [
      {
        id: 1,
        isEnabled: true,
        action: 'read' as const,
        effect: 'allow' as const,
        tableName,
        roleId: null,
        allowedUserIds: [],
        columnName: null,
        relationPropertyName: relName,
        condition: { owner: { _eq: '@USER.id' } },
      },
    ],
  };
}

const TABLE_META_UNPUBLISHED_REL: Record<string, any> = {
  ...TABLE_META,
  main_table: {
    ...TABLE_META.main_table,
    relations: [
      ...TABLE_META.main_table.relations,
      { propertyName: 'secretRel', targetTable: 'company_table', isPublished: false },
    ],
  },
};

const TABLE_META_NESTED_CONDITIONAL: Record<string, any> = {
  ...TABLE_META,
  user_table: {
    ...TABLE_META.user_table,
    columns: [
      ...TABLE_META.user_table.columns,
      { name: 'salary', isPublished: false },
    ],
  },
};

describe('stripDeniedFields — conditional rules safety', () => {
  it('does NOT strip isPublished:false column when conditional allow rule exists', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeConditionalAllowPolicy('main_table', 'secret')] },
    });
    const result = await strip(repo, 'main_table', ['id', 'name', 'secret'], undefined);
    expect(result.fields).toEqual(['id', 'name', 'secret']);
    expect(result.needsPostSql).toBe(true);
  });

  it('strips isPublished:false column when no conditional rules exist', async () => {
    const repo = makeRepo();
    const result = await strip(repo, 'main_table', ['id', 'name', 'secret'], undefined);
    expect(result.fields).toEqual(['id', 'name']);
    expect(result.needsPostSql).toBe(false);
  });

  it('does NOT strip isPublished:false relation when conditional allow rule exists', async () => {
    const repo = makeRepo({
      tableMetaMap: TABLE_META_UNPUBLISHED_REL,
      policiesMap: { 'main_table:read': [makeConditionalRelAllowPolicy('main_table', 'secretRel')] },
    });
    const result = await strip(repo, 'main_table', ['id', 'secretRel'], { secretRel: {} });
    expect(String(result.fields)).toContain('secretRel');
    expect(result.deep).toHaveProperty('secretRel');
    expect(result.needsPostSql).toBe(true);
  });

  it('strips isPublished:false relation when no conditional rules exist', async () => {
    const repo = makeRepo({ tableMetaMap: TABLE_META_UNPUBLISHED_REL });
    const result = await strip(repo, 'main_table', ['id', 'secretRel'], { secretRel: {} });
    expect(String(result.fields)).not.toContain('secretRel');
    expect(result.deep).not.toHaveProperty('secretRel');
    expect(result.needsPostSql).toBe(false);
  });

  it('needsPostSql bubbles up from nested deep', async () => {
    const repo = makeRepo({
      tableMetaMap: TABLE_META_NESTED_CONDITIONAL,
      policiesMap: { 'user_table:read': [makeConditionalAllowPolicy('user_table', 'salary')] },
    });
    const deep = { author: { fields: ['id', 'salary'], deep: {} } };
    const result = await strip(repo, 'main_table', ['id', 'author'], deep);
    expect(result.needsPostSql).toBe(true);
  });

  it('needsPostSql is false when nested deep has no conditional rules', async () => {
    const repo = makeRepo();
    const deep = { author: { fields: ['id', 'email'], deep: {} } };
    const result = await strip(repo, 'main_table', ['id', 'author'], deep);
    expect(result.needsPostSql).toBe(false);
  });
});

describe('hasConditionalRulesForField — direct', () => {
  it('returns true when conditional rule matches field', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeConditionalAllowPolicy('main_table', 'secret')] },
    });
    const result = await (repo as any).hasConditionalRulesForField('main_table', 'read', 'column', 'secret');
    expect(result).toBe(true);
  });

  it('returns false when no conditional rules for field', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeColDenyPolicy('main_table', 'name')] },
    });
    const result = await (repo as any).hasConditionalRulesForField('main_table', 'read', 'column', 'name');
    expect(result).toBe(false);
  });

  it('returns false when no policies at all', async () => {
    const repo = makeRepo();
    const result = await (repo as any).hasConditionalRulesForField('main_table', 'read', 'column', 'name');
    expect(result).toBe(false);
  });

  it('returns true for conditional relation rule', async () => {
    const repo = makeRepo({
      policiesMap: { 'main_table:read': [makeConditionalRelAllowPolicy('main_table', 'author')] },
    });
    const result = await (repo as any).hasConditionalRulesForField('main_table', 'read', 'relation', 'author');
    expect(result).toBe(true);
  });

  it('returns false for fieldPermissionCacheService not set', async () => {
    const repo = makeRepo();
    repo.fieldPermissionCacheService = undefined;
    const result = await (repo as any).hasConditionalRulesForField('main_table', 'read', 'column', 'secret');
    expect(result).toBe(false);
  });
});

describe('DynamicRepository.find — stripDeniedFields integration', () => {
  function makeFullRepo({
    enforceFieldPermission = true,
    isRootAdmin = false,
    policiesMap = {} as Record<string, any[]>,
    queryEngineResult = { data: [], count: 0 },
  } = {}) {
    const { DynamicRepository } = require('../../src/modules/dynamic-api/repositories/dynamic.repository');
    const repo = Object.create(DynamicRepository.prototype);
    repo.enforceFieldPermission = enforceFieldPermission;
    repo.tableName = 'main_table';
    repo.context = {
      $user: { id: 1, role: { id: '2' }, isRootAdmin },
      $query: {},
    };
    repo.tableMetadata = TABLE_META['main_table'];
    repo.metadataCacheService = {
      lookupTableByName: jest.fn().mockImplementation(
        async (name: string) => TABLE_META[name] ?? null,
      ),
      getDirectMetadata: jest.fn().mockReturnValue({ tables: new Map(), tablesList: [] }),
    };
    repo.fieldPermissionCacheService = {
      getPoliciesFor: jest.fn().mockImplementation(
        async (_user: any, tableName: string, action: string) =>
          policiesMap[`${tableName}:${action}`] ?? [],
      ),
      ensureLoaded: jest.fn().mockResolvedValue(undefined),
    };
    repo.queryEngine = {
      find: jest.fn().mockResolvedValue(queryEngineResult),
    };
    repo.settingCacheService = { getMaxQueryDepth: jest.fn().mockReturnValue(5) };
    return repo;
  }

  it('passes clean fields to queryEngine when relation denied', async () => {
    const repo = makeFullRepo({
      policiesMap: { 'main_table:read': [makeRelDenyPolicy('main_table', 'author')] },
    });
    repo.context.$query.fields = ['id', 'name', 'author.email'];
    await repo.find({});
    const callArgs = repo.queryEngine.find.mock.calls[0][0];
    expect(callArgs.fields).toEqual(['id', 'name']);
  });

  it('passes clean fields to queryEngine when column denied', async () => {
    const repo = makeFullRepo({
      policiesMap: { 'main_table:read': [makeColDenyPolicy('main_table', 'name')] },
    });
    repo.context.$query.fields = ['id', 'name'];
    await repo.find({});
    const callArgs = repo.queryEngine.find.mock.calls[0][0];
    expect(callArgs.fields).toEqual(['id']);
  });

  it('resolves wildcard and strips denied when fields not specified', async () => {
    const repo = makeFullRepo();
    await repo.find({});
    const callArgs = repo.queryEngine.find.mock.calls[0][0];
    const fields = String(callArgs.fields).split(',');
    expect(fields).toContain('id');
    expect(fields).toContain('name');
    expect(fields).not.toContain('secret');
  });

  it('post-SQL runs only when conditional rules exist', async () => {
    const repo = makeFullRepo({
      policiesMap: { 'main_table:read': [makeConditionalAllowPolicy('main_table', 'secret')] },
      queryEngineResult: { data: [{ id: 1, secret: 'x' }], count: 1 },
    });
    repo.context.$query.fields = ['id', 'secret'];
    repo.metadataCacheService.getDirectMetadata = jest.fn().mockReturnValue({
      tables: new Map(Object.entries(TABLE_META)),
      tablesList: Object.values(TABLE_META),
    });
    const sanitizeSpy = jest.spyOn(
      require('../../src/shared/utils/sanitize-field-permissions.util'),
      'sanitizeFieldPermissionsResult',
    ).mockResolvedValue([{ id: 1 }]);
    await repo.find({});
    expect(sanitizeSpy).toHaveBeenCalled();
    sanitizeSpy.mockRestore();
  });

  it('post-SQL skipped when no conditional rules (all resolved pre-SQL)', async () => {
    const repo = makeFullRepo({
      queryEngineResult: { data: [{ id: 1, name: 'test' }], count: 1 },
    });
    repo.context.$query.fields = ['id', 'name'];
    const sanitizeSpy = jest.spyOn(
      require('../../src/shared/utils/sanitize-field-permissions.util'),
      'sanitizeFieldPermissionsResult',
    );
    await repo.find({});
    expect(sanitizeSpy).not.toHaveBeenCalled();
    sanitizeSpy.mockRestore();
  });
});
