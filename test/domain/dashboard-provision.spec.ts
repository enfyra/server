import { describe, expect, it, vi } from 'vitest';
import { DashboardProvisionService } from '../../src/engines/bootstrap/services/dashboard-provision.service';
import { dashboardExtension } from '../../src/data/dashboard-extension';
import { processExtensionDefinition } from '../../src/modules/extension-definition/utils/processor.util';

vi.mock('../../src/modules/extension-definition/utils/processor.util', () => ({
  processExtensionDefinition: vi.fn(async (body) => ({
    processedBody: { ...body, compiledCode: 'compiled-vue' },
  })),
}));

describe('DashboardProvisionService', () => {
  it.each(['id', '_id'])(
    'compiles source and binds the default page using %s',
    async (pk) => {
      const query = {
        getPkField: () => pk,
        find: vi
          .fn()
          .mockResolvedValueOnce({ data: [{ [pk]: 7 }] })
          .mockResolvedValueOnce({ data: [{ [pk]: 1 }] })
          .mockResolvedValueOnce({ data: [] }),
        insert: vi.fn(),
        update: vi.fn(),
      };
      await new DashboardProvisionService(query as never).provision();
      expect(dashboardExtension).not.toHaveProperty('compiledCode');
      expect(processExtensionDefinition).toHaveBeenCalledWith(
        expect.objectContaining({
          code: dashboardExtension.code,
          menu: { [pk]: 7 },
        }),
        'POST',
      );
      expect(query.insert).toHaveBeenCalledWith(
        'enfyra_extension',
        expect.objectContaining({
          compiledCode: 'compiled-vue',
          isSystem: false,
        }),
      );
      expect(query.update).toHaveBeenCalledWith('enfyra_setting', 1, {
        defaultPage: { [pk]: 7 },
      });
    },
  );

  it('does not replace an assigned extension', async () => {
    const query = {
      getPkField: () => 'id',
      find: vi
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 7 }] })
        .mockResolvedValueOnce({ data: [{ id: 1 }] })
        .mockResolvedValueOnce({ data: [{ id: 9 }] }),
      insert: vi.fn(),
      update: vi.fn(),
    };
    await expect(
      new DashboardProvisionService(query as never).provision(),
    ).rejects.toThrow(/already has an extension/);
    expect(query.insert).not.toHaveBeenCalled();
    expect(query.update).not.toHaveBeenCalled();
  });

  it('fails without publishing the setting when compilation fails', async () => {
    vi.mocked(processExtensionDefinition).mockRejectedValueOnce(
      new Error('compile failed'),
    );
    const query = {
      getPkField: () => 'id',
      find: vi
        .fn()
        .mockResolvedValueOnce({ data: [{ id: 7 }] })
        .mockResolvedValueOnce({ data: [{ id: 1 }] })
        .mockResolvedValueOnce({ data: [] }),
      insert: vi.fn(),
      update: vi.fn(),
    };
    await expect(
      new DashboardProvisionService(query as never).provision(),
    ).rejects.toThrow('compile failed');
    expect(query.update).not.toHaveBeenCalled();
  });
});
