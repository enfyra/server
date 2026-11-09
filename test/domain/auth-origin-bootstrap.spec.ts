import { describe, expect, it } from 'vitest';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap/services/bootstrap-definition.service';

describe('Auth Origins bootstrap contract', () => {
  it.each(['postgres', 'mysql', 'mongodb'])('preserves independent auth metadata alongside custom-header cleanup on %s', backend => {
    const definition = new BootstrapDefinitionService({ databaseConfigService: { getDbType: () => backend } } as never).resolveForVersion('2.4.0');
    const table = definition.snapshot.enfyra_auth_origin;
    expect(table).toBeDefined();
    expect(table.uniques).toEqual([['value']]);
    expect(table.columns).toEqual(expect.arrayContaining([
      expect.objectContaining({ name: 'value', isNullable: false }),
      expect.objectContaining({ name: 'isEnabled', defaultValue: true }),
    ]));
    const route = definition.defaultData.enfyra_route.find((row: { path?: string }) => row.path === '/enfyra_auth_origin');
    expect(route).toMatchObject({ mainTable: 'enfyra_auth_origin', availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'], publicMethods: ['GET'] });
    const menu = definition.defaultData.enfyra_menu.find((row: { path?: string }) => row.path === '/settings/oauth/origins');
    expect(menu).toMatchObject({ label: 'Auth Origins', parent: 'OAuth', order: 3, isPublic: false });
    expect(definition.dataMigration.enfyra_route).toContainEqual(expect.objectContaining({ _unique: { path: { _eq: '/enfyra_auth_origin' } } }));
    expect(definition.snapshot.enfyra_auth_header).toBeUndefined();
    expect(definition.defaultData.enfyra_auth_header).toBeUndefined();
    expect(definition.migration?.tablesToDrop ?? []).toContain('enfyra_auth_header');
    expect(definition.migration?.tablesToDrop ?? []).not.toContain('enfyra_auth_origin');
  });
});
