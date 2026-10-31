import { readFileSync } from 'node:fs';
import { describe, expect, it, vi } from 'vitest';
import { AuthenticationService } from '../../src/domain/auth/services/authentication.service';
import { bootstrapSourceArtifacts } from '../../src/data';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap';
import { compileMetadataMigrationExecutionPlan } from '../../src/engines/bootstrap/utils/metadata-migration-plan.util';
import { deleteMongoMetadataRecords } from '../../src/engines/bootstrap/utils/delete-mongo-metadata-records.util';

const removedPaths = ['/enfyra_auth_header', '/admin/auth-header/reorder', '/settings/auth-headers'];

describe('fixed native authentication transport', () => {
  it('ignores persisted custom mappings and Bearer PATs before credential verification', async () => {
    const verifyPat = vi.fn();
    const verifyJwt = vi.fn();
    const getAuthHeaderConfigs = vi.fn(() => [{ headerKey: 'x-api-key', scheme: 'raw', credentialType: 'pat', isEnabled: true }]);
    const deps = {
      queryBuilderService: {} as never,
      patVerifierService: { verify: verifyPat } as never,
      jwtVerifierService: { verify: verifyJwt } as never,
      runtimeRegistryService: { getAuthHeaderConfigs },
    };
    const service = new AuthenticationService(deps);
    expect(service.hasCredentials({ 'x-api-key': 'efy_pat_fixture' })).toBe(false);
    expect(await service.authenticate({ headers: { 'x-api-key': 'efy_pat_fixture' } })).toBeNull();
    expect(await service.authenticate({ headers: { authorization: 'Bearer efy_pat_fixture' }, allowAnonymous: true })).toBeNull();
    expect(getAuthHeaderConfigs).not.toHaveBeenCalled();
    expect(verifyPat).not.toHaveBeenCalled();
    expect(verifyJwt).not.toHaveBeenCalled();
  });

  it('does not let another valid transport override duplicate native headers', async () => {
    const service = new AuthenticationService({ queryBuilderService: {} as never, patVerifierService: {} as never, jwtVerifierService: {} as never });
    for (const headers of [
      { 'x-enfyra-pat': ['efy_pat_one', 'efy_pat_two'], authorization: 'Bearer aaa.bbb.ccc' },
      { 'x-enfyra-pat': 'efy_pat_fixture', authorization: ['Bearer aaa.bbb.ccc'] },
    ]) {
      await expect(service.authenticate({ headers })).rejects.toThrow();
    }
  });

  it('retains direct PAT credentials for native tooling and Bearer JWT session detection', () => {
    const service = new AuthenticationService({ queryBuilderService: {} as never, patVerifierService: {} as never, jwtVerifierService: {} as never });
    expect(service.hasCredentials({ 'x-enfyra-pat': 'efy_pat_fixture' })).toBe(true);
    expect(service.hasCredentials({ authorization: 'Bearer aaa.bbb.ccc' })).toBe(true);
  });
});

describe('custom authentication-header upgrade cleanup', () => {
  it('deletes exact Mongo route/menu dependencies and preserves unrelated records on retry', async () => {
    const rows: Record<string, Record<string, string>[]> = {
      enfyra_route: [{ _id: 'headers', path: '/enfyra_auth_header' }, { _id: 'reorder', path: '/admin/auth-header/reorder' }, { _id: 'keep', path: '/orders' }],
      enfyra_route_handler: [{ _id: 'handler', route: 'headers' }],
      enfyra_route_method_config: [{ _id: 'config', route: 'headers' }],
      enfyra_pre_hook: [{ _id: 'hook', route: 'headers' }],
      enfyra_route_permission: [{ _id: 'permission', route: 'headers' }],
      enfyra_menu: [{ _id: 'menu', path: '/settings/auth-headers' }, { _id: 'keep-menu', path: '/settings' }],
      enfyra_menu_permission: [{ _id: 'menu-permission', menu: 'menu' }],
    };
    const matches = (row: Record<string, string>, filter: Record<string, unknown>) => Object.entries(filter).every(([key, value]) =>
      value && typeof value === 'object' && '$in' in value
        ? (value.$in as string[]).includes(row[key]!)
        : row[key] === value,
    );
    const db = {
      listCollections: ({ name }: { name: string }) => ({ toArray: async () => name in rows ? [{ name }] : [] }),
      collection: (name: string) => ({
        find: (filter: Record<string, unknown>) => ({ toArray: async () => (rows[name] ?? []).filter(row => matches(row, filter)) }),
        findOne: async (filter: Record<string, unknown>) => (rows[name] ?? []).find(row => matches(row, filter)) ?? null,
        deleteMany: async (filter: Record<string, unknown>) => { rows[name] = (rows[name] ?? []).filter(row => !matches(row, filter)); return { deletedCount: 1 }; },
        updateMany: async () => ({ modifiedCount: 0 }),
        countDocuments: async (filter: Record<string, unknown>) => (rows[name] ?? []).filter(row => matches(row, filter)).length,
      }),
    };
    for (let attempt = 0; attempt < 2; attempt++) {
      for (const path of removedPaths) await deleteMongoMetadataRecords(db as never, path.startsWith('/settings/') ? 'enfyra_menu' : 'enfyra_route', { path });
    }
    expect(rows.enfyra_route).toEqual([{ _id: 'keep', path: '/orders' }]);
    expect(rows.enfyra_menu).toEqual([{ _id: 'keep-menu', path: '/settings' }]);
    for (const table of ['enfyra_route_handler', 'enfyra_route_method_config', 'enfyra_pre_hook', 'enfyra_route_permission', 'enfyra_menu_permission']) expect(rows[table]).toEqual([]);
  });

  it('does not provision the table, routes, menu or metadata cache on fresh installs', () => {
    expect(bootstrapSourceArtifacts.snapshot).not.toHaveProperty('enfyra_auth_header');
    expect(JSON.stringify(bootstrapSourceArtifacts.defaultData)).not.toContain('enfyra_auth_header');
    expect(JSON.stringify(bootstrapSourceArtifacts.dataCorrections)).not.toContain('enfyra_auth_header');
    for (const file of ['src/wiring/cradle.ts', 'src/wiring/registers/cache.ts', 'src/wiring/init/phases.ts', 'src/engines/cache/services/cache-orchestrator.service.ts', 'src/engines/cache/services/runtime-registry.service.ts'])
      expect(readFileSync(file, 'utf8')).not.toMatch(/authHeader|AUTH_HEADER|AuthHeaderCache/);
  });

  it.each(['postgresql', 'mysql', 'mongodb'] as const)('declares table and exact dependent-record removal for %s upgrades', (database) => {
    const service = new BootstrapDefinitionService({ databaseConfigService: { getDbType: () => database } as never });
    const definition = service.resolveForVersion('2.4.0');
    expect(service.getApplicableSchemaSteps().map(step => step.toVersion)).toEqual(['2.4.1']);
    expect(definition.migration?.tablesToDrop).toContain('enfyra_auth_header');
    const plan = compileMetadataMigrationExecutionPlan(definition.migration, {
      mode: 'upgrade', database, targetTableCount: Object.keys(definition.snapshot).length,
      observedMetadata: { tables: 1, columns: 1, relations: 0 }, steps: [...service.getApplicableSchemaSteps()],
    });
    expect(plan.operations).toContainEqual(expect.objectContaining({ kind: 'drop-table', tableName: 'enfyra_auth_header' }));
    for (const path of removedPaths) {
      expect(definition.dataMigration._deletedRecords).toContainEqual({ table: path.startsWith('/settings/') ? 'enfyra_menu' : 'enfyra_route', filter: { path: { _eq: path } } });
    }
  });
});
