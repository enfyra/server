import { ForbiddenException } from '@nestjs/common';
import { CascadeHandler } from '../../src/infrastructure/knex/utils/cascade-handler';

function makeCascadeHandler(opts: {
  getFieldPermissionContext?: () => { check: (...args: any[]) => Promise<void> } | null;
  insertWithCascade?: (...args: any[]) => Promise<any>;
} = {}) {
  const knex = {} as any;
  const metadataCache = {
    getMetadata: jest.fn().mockResolvedValue({ tables: new Map(), tablesList: [] }),
  } as any;
  const logger = { warn: jest.fn() } as any;

  return new CascadeHandler(
    knex,
    metadataCache,
    logger,
    undefined,
    undefined,
    opts.insertWithCascade,
    undefined,
    undefined,
    opts.getFieldPermissionContext,
  );
}

describe('CascadeHandler — field permission on cascade create', () => {
  it('calls field permission check before inserting child record', async () => {
    const checker = jest.fn().mockResolvedValue(undefined);
    const handler = makeCascadeHandler({
      getFieldPermissionContext: () => ({ check: checker }),
      insertWithCascade: async (_tbl, data) => ({ id: 42, ...data }),
    });

    await (handler as any).insertRecordAndGetId('child_table', { name: 'test' });

    expect(checker).toHaveBeenCalledWith('child_table', 'create', expect.objectContaining({ name: 'test' }));
  });

  it('throws when field permission check denies access', async () => {
    const checker = jest.fn().mockRejectedValue(new ForbiddenException('Field denied'));
    const handler = makeCascadeHandler({
      getFieldPermissionContext: () => ({ check: checker }),
      insertWithCascade: jest.fn(),
    });

    await expect(
      (handler as any).insertRecordAndGetId('child_table', { secret_field: 'value' }),
    ).rejects.toThrow(ForbiddenException);

    expect((handler as any).insertWithCascade).not.toHaveBeenCalled();
  });

  it('skips field permission check when context is null', async () => {
    const insertWithCascade = jest.fn().mockResolvedValue({ id: 1 });
    const handler = makeCascadeHandler({
      getFieldPermissionContext: () => null,
      insertWithCascade,
    });

    await (handler as any).insertRecordAndGetId('child_table', { name: 'ok' });

    expect(insertWithCascade).toHaveBeenCalled();
  });

  it('skips field permission check when getFieldPermissionContext is not provided', async () => {
    const insertWithCascade = jest.fn().mockResolvedValue({ id: 1 });
    const handler = makeCascadeHandler({ insertWithCascade });

    await (handler as any).insertRecordAndGetId('child_table', { name: 'ok' });

    expect(insertWithCascade).toHaveBeenCalled();
  });

  it('returns null immediately without checking when data is null', async () => {
    const checker = jest.fn();
    const handler = makeCascadeHandler({
      getFieldPermissionContext: () => ({ check: checker }),
    });

    const result = await (handler as any).insertRecordAndGetId('child_table', null);

    expect(result).toBeNull();
    expect(checker).not.toHaveBeenCalled();
  });
});

describe('DynamicRepository — wrapWithFieldPermissionCheck', () => {
  function makeRepo(opts: {
    enforceFieldPermission?: boolean;
    isRootAdmin?: boolean;
    fieldPermissionCacheService?: any;
    runWithFieldPermissionCheck?: jest.Mock;
  }) {
    const { DynamicRepository } = require('../../src/modules/dynamic-api/repositories/dynamic.repository');
    const repo = Object.create(DynamicRepository.prototype);
    repo.enforceFieldPermission = opts.enforceFieldPermission ?? false;
    repo.fieldPermissionCacheService = opts.fieldPermissionCacheService ?? null;
    repo.context = { $user: opts.isRootAdmin ? { isRootAdmin: true } : { id: 1 } };
    repo.queryBuilder = {
      runWithFieldPermissionCheck: opts.runWithFieldPermissionCheck ?? jest.fn((_, cb) => cb()),
    };
    repo.metadataCacheService = { lookupTableByName: jest.fn().mockResolvedValue(null) };
    repo.tableName = 'test_table';
    return repo;
  }

  it('calls callback directly when enforceFieldPermission is false', async () => {
    const repo = makeRepo({ enforceFieldPermission: false });
    const cb = jest.fn().mockResolvedValue('result');

    const result = await repo.wrapWithFieldPermissionCheck(cb);

    expect(result).toBe('result');
    expect(repo.queryBuilder.runWithFieldPermissionCheck).not.toHaveBeenCalled();
  });

  it('calls callback directly for root admin', async () => {
    const repo = makeRepo({
      enforceFieldPermission: true,
      isRootAdmin: true,
      fieldPermissionCacheService: {},
    });
    const cb = jest.fn().mockResolvedValue('result');

    const result = await repo.wrapWithFieldPermissionCheck(cb);

    expect(result).toBe('result');
    expect(repo.queryBuilder.runWithFieldPermissionCheck).not.toHaveBeenCalled();
  });

  it('uses runWithFieldPermissionCheck for regular user when enforcing', async () => {
    const runMock = jest.fn((_checker, cb) => cb());
    const repo = makeRepo({
      enforceFieldPermission: true,
      isRootAdmin: false,
      fieldPermissionCacheService: {},
      runWithFieldPermissionCheck: runMock,
    });
    const cb = jest.fn().mockResolvedValue('result');

    await repo.wrapWithFieldPermissionCheck(cb);

    expect(runMock).toHaveBeenCalledWith(expect.any(Function), cb);
  });

  it('cascadeFieldPermissionCheck returns early when no metadata', async () => {
    const repo = makeRepo({
      enforceFieldPermission: true,
      fieldPermissionCacheService: { getPoliciesFor: jest.fn().mockResolvedValue([]) },
    });
    repo.metadataCacheService.lookupTableByName = jest.fn().mockResolvedValue(null);

    await expect(
      repo.cascadeFieldPermissionCheck('some_table', 'create', { field: 'value' }),
    ).resolves.toBeUndefined();
  });

  it('cascadeFieldPermissionCheck throws when field is denied', async () => {
    const repo = makeRepo({
      enforceFieldPermission: true,
      fieldPermissionCacheService: {},
    });
    repo.metadataCacheService.lookupTableByName = jest.fn().mockResolvedValue({
      columns: [{ name: 'secret', isPublished: true }],
      relations: [],
    });

    const { decideFieldPermission } = require('../../src/shared/utils/field-permission.util');
    jest.spyOn(require('../../src/shared/utils/field-permission.util'), 'decideFieldPermission')
      .mockResolvedValue({ allowed: false, reason: 'Denied' });

    await expect(
      repo.cascadeFieldPermissionCheck('child_table', 'create', { secret: 'value' }),
    ).rejects.toThrow(ForbiddenException);

    jest.restoreAllMocks();
  });
});
