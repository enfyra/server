import { describe, expect, it, vi } from 'vitest';
import { ObjectId } from 'mongodb';
import { DashboardProvisionService } from '../../src/engines/bootstrap/services/dashboard-provision.service';
import { dashboardExtension } from '../../src/data/dashboard-extension';
import { processExtensionDefinition } from '../../src/modules/extension-definition/utils/processor.util';

vi.mock('../../src/modules/extension-definition/utils/processor.util', () => ({
  processExtensionDefinition: vi.fn(async (body) => ({
    processedBody: { ...body, compiledCode: 'compiled-vue' },
  })),
}));

function setup(pk = 'id', customMapping = false) {
  const mongo = pk === '_id';
  const menuId = mongo ? new ObjectId() : 7;
  const settingId = mongo ? new ObjectId() : 1;
  const menuField = customMapping ? 'pageMenuRef' : mongo ? 'menu' : 'menuId';
  const defaultPageField = customMapping ? 'homeMenuRef' : mongo ? 'defaultPage' : 'defaultPageId';
  const tables = new Map([
    ['enfyra_extension', { relations: [{ propertyName: 'menu', type: 'one-to-one', foreignKeyColumn: menuField }] }],
    ['enfyra_setting', { relations: [{ propertyName: 'defaultPage', type: 'many-to-one', foreignKeyColumn: defaultPageField }] }],
  ]);
  const metadataCache = { getDirectMetadata: () => ({ tables }) };
  const query = {
    getPkField: () => pk,
    find: vi.fn()
      .mockResolvedValueOnce({ data: [{ [pk]: mongo ? String(menuId) : menuId }] })
      .mockResolvedValueOnce({ data: [{ [pk]: settingId }] })
      .mockResolvedValueOnce({ data: [] }),
    insert: vi.fn(),
    update: vi.fn(),
  };
  const service = new DashboardProvisionService(query as never, metadataCache as never);
  return { service, query, tables, menuId, settingId, menuField, defaultPageField };
}

describe('DashboardProvisionService', () => {
  it.each(['id', '_id'])('persists staged relation mappings before runtime activation using %s', async (pk) => {
    const { service, query, menuId, settingId, menuField, defaultPageField } = setup(pk);
    await service.provision();
    expect(dashboardExtension).not.toHaveProperty('compiledCode');
    expect(processExtensionDefinition).toHaveBeenCalledWith(expect.objectContaining({ code: dashboardExtension.code }), 'POST');
    const body = query.insert.mock.calls[0][1];
    expect(body).toMatchObject({ compiledCode: 'compiled-vue', isSystem: false, [menuField]: menuId });
    if (pk === 'id') expect(body).not.toHaveProperty('menu');
    expect(query.update).toHaveBeenCalledWith('enfyra_setting', settingId, { [defaultPageField]: menuId });
  });

  it.each(['id', '_id'])('uses persisted non-default storage mappings using %s', async (pk) => {
    const { service, query, menuId, settingId, menuField, defaultPageField } = setup(pk, true);
    await service.provision();
    expect(query.insert.mock.calls[0][1][menuField]).toEqual(menuId);
    expect(query.update).toHaveBeenCalledWith('enfyra_setting', settingId, { [defaultPageField]: menuId });
  });

  it('fails before writes when staged relation metadata is missing', async () => {
    const { service, query, tables } = setup();
    tables.delete('enfyra_extension');
    await expect(service.provision()).rejects.toThrow(/requires enfyra_extension.menu metadata/);
    expect(query.insert).not.toHaveBeenCalled();
    expect(query.update).not.toHaveBeenCalled();
  });

  it('does not replace an assigned extension', async () => {
    const { service, query } = setup();
    query.find.mockReset().mockResolvedValueOnce({ data: [{ id: 7 }] }).mockResolvedValueOnce({ data: [{ id: 1 }] }).mockResolvedValueOnce({ data: [{ id: 9 }] });
    await expect(service.provision()).rejects.toThrow(/already has an extension/);
    expect(query.insert).not.toHaveBeenCalled();
    expect(query.update).not.toHaveBeenCalled();
  });

  it('does not publish settings when compilation fails', async () => {
    vi.mocked(processExtensionDefinition).mockRejectedValueOnce(new Error('compile failed'));
    const { service, query } = setup();
    await expect(service.provision()).rejects.toThrow('compile failed');
    expect(query.insert).not.toHaveBeenCalled();
    expect(query.update).not.toHaveBeenCalled();
  });
});
