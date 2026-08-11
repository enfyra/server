import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { FileAssetAccessService } from '../../src/modules/file-management/services/file-asset-access.service';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

vi.mock('../../src/shared/utils/load-user-with-role.util', () => ({
  loadCachedUserWithRoles: vi.fn(
    async (_qb: any, userId: string) => ({
      id: userId,
      roles: [{ id: 'role-1', name: 'member' }],
    }),
  ),
}));

vi.mock('../../src/modules/file-management/utils/file-validation.helper', () => ({
  FileValidationHelper: {
    checkFilePermissions: vi.fn(async () => {}),
  },
}));

function makeFile(overrides: Record<string, any> = {}) {
  return {
    id: 'file-1',
    filename: 'test.txt',
    mimetype: 'text/plain',
    type: 'document',
    location: '/uploads/test.txt',
    filesize: 100,
    isPublic: true,
    storageConfig: null,
    ...overrides,
  };
}

function makePermission(overrides: Record<string, any> = {}) {
  return {
    id: 'perm-1',
    isEnabled: true,
    file: { id: 'file-1' },
    role: { id: 'role-1', name: 'member' },
    allowedUsers: [],
    ...overrides,
  };
}

function makeQueryBuilder(state: { files: any[]; permissions: any[] }) {
  return {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn(async (args: any) => {
      if (args.table === 'enfyra_file') {
        const id = args.filter?.id?._eq ?? args.filter?._id?._eq;
        return { data: state.files.filter((f) => String(f.id ?? f._id) === String(id)) };
      }
      if (args.table === 'enfyra_file_permission') {
        const ids = args.filter?.id?._in ?? args.filter?._id?._in;
        if (ids) {
          const set = new Set(ids.map(String));
          return {
            data: state.permissions.filter((p) => set.has(String(p.id))),
          };
        }
        const fileId = args.filter?._and?.find((e: any) => e.file)?.file
          ?.id?._eq;
        return {
          data: state.permissions.filter((p) => {
            const permFileId = p.file?.id ?? p.file?._id ?? p.file;
            return p.isEnabled !== false && String(permFileId) === String(fileId);
          }),
        };
      }
      if (args.table === 'enfyra_role') {
        return { data: [] };
      }
      return { data: [] };
    }),
    findOne: vi.fn(async () => null),
  } as any;
}

function makeReq(overrides: Record<string, any> = {}): any {
  return { user: null, headers: {}, query: {}, ...overrides };
}

describe('FileAssetAccessService', () => {
  it('returns cloned file records so cache mutations do not leak', async () => {
    const file = makeFile();
    const qb = makeQueryBuilder({ files: [file], permissions: [] });
    const service = new FileAssetAccessService({ queryBuilderService: qb });

    const first = await service.resolveAuthorizedFile(makeReq(), 'file-1');
    expect(first).not.toBeNull();
    first!.mutated = true;

    const second = await service.resolveAuthorizedFile(makeReq(), 'file-1');
    expect((second as any)?.mutated).toBeUndefined();
    expect(qb.find).toHaveBeenCalledTimes(1);
  });

  it('invalidates only the affected file on partial enfyra_file event', async () => {
    const file1 = makeFile({ id: 'file-1' });
    const file2 = makeFile({ id: 'file-2' });
    const qb = makeQueryBuilder({ files: [file1, file2], permissions: [] });
    const emitter = new EventEmitter2();
    const service = new FileAssetAccessService({
      queryBuilderService: qb,
      eventEmitter: emitter,
    });

    await service.resolveAuthorizedFile(makeReq(), 'file-1');
    await service.resolveAuthorizedFile(makeReq(), 'file-2');
    expect(qb.find).toHaveBeenCalledTimes(2);

    await emitter.emitAsync(CACHE_EVENTS.INVALIDATE, {
      table: 'enfyra_file',
      scope: 'partial',
      ids: ['file-1'],
    });

    await service.resolveAuthorizedFile(makeReq(), 'file-1');
    expect(qb.find).toHaveBeenCalledTimes(3);

    await service.resolveAuthorizedFile(makeReq(), 'file-2');
    expect(qb.find).toHaveBeenCalledTimes(3);
  });

  it('invalidates only affected file permission caches on partial permission event', async () => {
    const file1 = makeFile({ id: 'file-1', isPublic: false });
    const file2 = makeFile({ id: 'file-2', isPublic: false });
    const perm1 = makePermission({ id: 'perm-1', file: { id: 'file-1' } });
    const perm2 = makePermission({ id: 'perm-2', file: { id: 'file-2' } });
    const qb = makeQueryBuilder({
      files: [file1, file2],
      permissions: [perm1, perm2],
    });
    const emitter = new EventEmitter2();
    const service = new FileAssetAccessService({
      queryBuilderService: qb,
      eventEmitter: emitter,
    });

    const req1 = makeReq({
      user: { id: 'u1', roles: [{ id: 'role-1' }] },
    });
    const req2 = makeReq({
      user: { id: 'u1', roles: [{ id: 'role-1' }] },
    });

    await service.resolveAuthorizedFile(req1, 'file-1');
    await service.resolveAuthorizedFile(req2, 'file-2');
    const callsBefore = qb.find.mock.calls.length;

    emitter.emit(CACHE_EVENTS.INVALIDATE, {
      table: 'enfyra_file_permission',
      scope: 'partial',
      ids: ['perm-1'],
    });

    await service.resolveAuthorizedFile(req1, 'file-1');
    const callsAfterFile1 = qb.find.mock.calls.length;
    expect(callsAfterFile1).toBeGreaterThan(callsBefore);

    await service.resolveAuthorizedFile(req2, 'file-2');
    const callsAfterFile2 = qb.find.mock.calls.length;
    expect(callsAfterFile2).toBe(callsAfterFile1);
  });

  it('skips permission lookup for public files', async () => {
    const file = makeFile({ isPublic: true });
    const qb = makeQueryBuilder({ files: [file], permissions: [] });
    const service = new FileAssetAccessService({ queryBuilderService: qb });

    const result = await service.resolveAuthorizedFile(makeReq(), 'file-1');
    expect(result).not.toBeNull();
    const permissionCalls = qb.find.mock.calls.filter(
      ([args]: any[]) => args.table === 'enfyra_file_permission',
    );
    expect(permissionCalls).toHaveLength(0);
  });

  it('uses the active backend primary key for permission relation projections', async () => {
    const file = makeFile({ _id: 'file-1', id: undefined, isPublic: false });
    const qb = makeQueryBuilder({ files: [file], permissions: [] });
    qb.getPkField.mockReturnValue('_id');
    const service = new FileAssetAccessService({ queryBuilderService: qb });

    await service.resolveAuthorizedFile(
      makeReq({ user: { _id: 'user-1', roles: [{ _id: 'role-1' }] } }),
      'file-1',
    );

    const permissionQuery = qb.find.mock.calls
      .map(([args]: any[]) => args)
      .find((args: any) => args.table === 'enfyra_file_permission');
    expect(permissionQuery.fields).toEqual([
      '_id',
      'isEnabled',
      'file._id',
      'role._id',
      'role.name',
      'allowedUsers._id',
      'allowedUsers.email',
    ]);
  });

  it('uses the active backend primary key to resolve partial permission invalidation', async () => {
    const file = makeFile({ _id: 'file-1', id: undefined, isPublic: false });
    const permission = makePermission({ _id: 'perm-1', id: undefined });
    const qb = makeQueryBuilder({ files: [file], permissions: [permission] });
    qb.getPkField.mockReturnValue('_id');
    const emitter = new EventEmitter2();
    const service = new FileAssetAccessService({ queryBuilderService: qb, eventEmitter: emitter });

    await service.resolveAuthorizedFile(
      makeReq({ user: { _id: 'user-1', roles: [{ _id: 'role-1' }] } }),
      'file-1',
    );
    await emitter.emitAsync(CACHE_EVENTS.INVALIDATE, {
      table: 'enfyra_file_permission',
      scope: 'partial',
      ids: ['perm-1'],
    });

    const invalidationQuery = qb.find.mock.calls
      .map(([args]: any[]) => args)
      .find((args: any) => args.filter?._id?._in?.includes('perm-1'));
    expect(invalidationQuery.fields).toEqual(['_id', 'file._id']);
  });

  it('skips permission hydration for root admin on private files', async () => {
    const file = makeFile({ isPublic: false });
    const qb = makeQueryBuilder({ files: [file], permissions: [] });
    const service = new FileAssetAccessService({ queryBuilderService: qb });

    const req = makeReq({
      user: { id: 'admin-1', isRootAdmin: true },
    });
    const result = await service.resolveAuthorizedFile(req, 'file-1');
    expect(result).not.toBeNull();
    const permissionCalls = qb.find.mock.calls.filter(
      ([args]: any[]) => args.table === 'enfyra_file_permission',
    );
    expect(permissionCalls).toHaveLength(0);
  });

  it('hydrates user roles for private file when user lacks role data', async () => {
    const { loadCachedUserWithRoles } = await import(
      '../../src/shared/utils/load-user-with-role.util'
    );
    const file = makeFile({ isPublic: false });
    const perm = makePermission({ file: { id: 'file-1' } });
    const qb = makeQueryBuilder({ files: [file], permissions: [perm] });
    const service = new FileAssetAccessService({ queryBuilderService: qb });

    const req = makeReq({ user: { id: 'u1' } });
    await service.resolveAuthorizedFile(req, 'file-1');
    expect(loadCachedUserWithRoles).toHaveBeenCalled();
  });
});
