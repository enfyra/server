import { describe, expect, it } from 'vitest';
import { FileValidationHelper } from '../../src/modules/file-management/utils/file-validation.helper';

function request(user: Record<string, unknown>) {
  return { user } as any;
}

function privateFile(permission: Record<string, unknown>) {
  return {
    isPublic: false,
    permissions: [{ isEnabled: true, allowedUsers: [{ id: 7 }], ...permission }],
  };
}

describe('FileValidationHelper permission actions', () => {
  it('denies bytes when the granted actions exclude read access', async () => {
    for (const actions of [['delete'], ['share'], ['edit']]) {
      await expect(
        FileValidationHelper.checkFilePermissions(
          privateFile({ actions }),
          request({ id: 7 }),
        ),
      ).rejects.toThrow('Access denied');
    }
  });

  it('allows bytes for view and download actions', async () => {
    for (const actions of [['view'], ['download'], ['view', 'edit']]) {
      await expect(
        FileValidationHelper.checkFilePermissions(
          privateFile({ actions }),
          request({ id: 7 }),
        ),
      ).resolves.toBeUndefined();
    }
  });

  it('keeps legacy permissions without an actions list readable', async () => {
    await expect(
      FileValidationHelper.checkFilePermissions(
        privateFile({}),
        request({ id: 7 }),
      ),
    ).resolves.toBeUndefined();
    await expect(
      FileValidationHelper.checkFilePermissions(
        privateFile({ actions: [] }),
        request({ id: 7 }),
      ),
    ).resolves.toBeUndefined();
  });

  it('still denies an unrelated user and honours root bypass', async () => {
    await expect(
      FileValidationHelper.checkFilePermissions(
        privateFile({ actions: ['view'] }),
        request({ id: 8 }),
      ),
    ).rejects.toThrow('Access denied');
    await expect(
      FileValidationHelper.checkFilePermissions(
        privateFile({ actions: ['delete'] }),
        request({ id: 9, isRootAdmin: true }),
      ),
    ).resolves.toBeUndefined();
  });

  it('leaves public files open', async () => {
    await expect(
      FileValidationHelper.checkFilePermissions(
        { isPublic: true, permissions: [] },
        request({ id: 7 }),
      ),
    ).resolves.toBeUndefined();
  });
});
