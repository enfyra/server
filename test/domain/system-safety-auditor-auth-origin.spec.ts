import { describe, expect, it, vi } from 'vitest';
import { SystemSafetyAuditorService } from '../../src/domain/policy/services/system-safety-auditor.service';

function assertOrigin(operation: string, data: unknown, existing: unknown = null) {
  const service = new SystemSafetyAuditorService({
    commonService: { assertNoSystemFlagDeep: vi.fn() },
    runtimeRegistryService: { requireMetadata: () => ({ tables: new Map([['enfyra_auth_origin', { columns: [] }]]) }) },
    schemaMigrationValidatorService: { getChangedFields: () => [], getAllowedFields: () => [], getAllRelationFieldsWithInverse: async () => [] },
    queryBuilderService: {},
  } as never);
  return service.assertSystemSafe({ operation, tableName: 'enfyra_auth_origin', data, existing, currentUser: null });
}

describe('Auth origin metadata mutation policy', () => {
  it.each(['https://console.example.test', 'http://localhost:3000', 'http://127.0.0.1:3000'])('allows a normalized explicit origin %s', async value => {
    await expect(assertOrigin('create', { value })).resolves.toBeUndefined();
  });
  it.each(['*', 'null', 'javascript:alert(1)', 'https://console.example.test/path', 'https://u:p@console.example.test', 'https://console.example.test?x=1', 'https://console.example.test#fragment', ' https://console.example.test ', 'https://console.example.test/'])('rejects unsafe or noncanonical origin %s', async value => {
    await expect(assertOrigin('create', { value })).rejects.toThrow('Auth origin must be');
  });
  it('permits disabling and deleting an origin without a value in the mutation body', async () => {
    await expect(assertOrigin('update', { isEnabled: false }, { value: 'https://console.example.test' })).resolves.toBeUndefined();
    await expect(assertOrigin('delete', {}, { value: 'https://console.example.test' })).resolves.toBeUndefined();
  });
});
