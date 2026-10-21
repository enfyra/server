import { describe, expect, it, vi } from 'vitest';
import { revertCodeAudit } from '../../src/modules/dynamic-api/services/code-audit-revert.service';

function fixture() {
  const entry = {
    tableName: 'enfyra_helper',
    recordId: '7',
    action: 'update',
    fromVersion: '1.0.0',
    previous: { sourceCode: 'return 1;', version: '1.0.0', id: 99 },
  };
  const update = vi
    .fn()
    .mockResolvedValue({ data: [{ id: 7, version: '1.0.3' }] });
  const context: any = {
    $user: { id: 1 },
    $repos: {
      secure: {
        enfyra_audit: {
          find: vi.fn().mockResolvedValue({ data: [entry] }),
        },
        enfyra_helper: { update },
      },
      enfyra_helper: { update: vi.fn() },
    },
  };
  const deps: any = {
    context,
    auditId: 2,
    version: '1.0.3',
    pkField: 'id',
    routes: [
      {
        mainTable: { name: 'enfyra_helper' },
        methodConfigs: [
          {
            available: true,
            method: { name: 'PATCH' },
            routePermissions: [{ id: 1 }],
          },
        ],
      },
    ],
    policyService: {
      checkRequestAccess: vi.fn().mockReturnValue({ allow: true }),
    },
  };
  return { entry, update, context, deps };
}

describe('code history revert boundary', () => {
  it('checks target PATCH access and uses only secure repositories', async () => {
    const { deps, update, context } = fixture();
    await revertCodeAudit(deps);
    expect(deps.policyService.checkRequestAccess).toHaveBeenCalledWith(
      expect.objectContaining({ method: 'PATCH', user: context.$user }),
    );
    expect(update).toHaveBeenCalledWith({
      id: '7',
      data: { sourceCode: 'return 1;', version: '1.0.3' },
    });
    expect(context.$repos.enfyra_helper.update).not.toHaveBeenCalled();
    expect(context.$codeAudit).toBeUndefined();
  });
  it('denies callers with revert access but no target mutation permission', async () => {
    const { deps, update } = fixture();
    deps.policyService.checkRequestAccess.mockReturnValue({ allow: false });
    await expect(revertCodeAudit(deps)).rejects.toThrow('Forbidden');
    expect(update).not.toHaveBeenCalled();
  });
  it('rejects delete history, missing targets, and malformed pre-images', async () => {
    for (const change of [
      { action: 'delete' },
      { previous: null },
      { previous: [] },
      { tableName: 'tasks' },
    ]) {
      const { deps, entry, update } = fixture();
      Object.assign(entry, change);
      await expect(revertCodeAudit(deps)).rejects.toThrow();
      expect(update).not.toHaveBeenCalled();
    }
    const { deps, update } = fixture();
    deps.routes = [];
    await expect(revertCodeAudit(deps)).rejects.toThrow('Forbidden');
    expect(update).not.toHaveBeenCalled();
  });
  it('clears the marker after a failed normal update', async () => {
    const { deps, update, context } = fixture();
    update.mockRejectedValue(new Error('version rejected'));
    await expect(revertCodeAudit(deps)).rejects.toThrow('version rejected');
    expect(context.$codeAudit).toBeUndefined();
  });
});
