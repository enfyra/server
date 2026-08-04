import { describe, expect, it, vi } from 'vitest';
import {
  SqlTableCreateService,
  SqlTableUpdateService,
} from '../../src/modules/table-management';
import { TableManagementValidationService } from '../../src/modules/table-management/services/table-validation.service';

function invalidBody() {
  return {
    name: 'gateway_models',
    columns: [{ name: 'as', type: 'varchar' }],
  } as any;
}

describe('column identifier validation boundaries', () => {
  it.each([
    ['SQL table create', SqlTableCreateService, 'createTable', 'postgres'],
    ['SQL table update', SqlTableUpdateService, 'updateTable', 'postgres'],
  ] as const)(
    '%s validates before policy, schema locks, or mutation work',
    async (_name, Service, method, database) => {
      const service: any = Object.create(Service.prototype);
      const policyService = { checkSchemaMigration: vi.fn() };
      service.tableValidationService = new TableManagementValidationService();
      service.queryBuilderService = {
        getDatabaseType: () => database,
      };
      service.policyService = policyService;

      const args =
        method === 'createTable' ? [invalidBody()] : [1, invalidBody()];
      await expect(service[method](...args)).rejects.toThrow(
        /reserved keyword/i,
      );

      expect(policyService.checkSchemaMigration).not.toHaveBeenCalled();
    },
  );
});
