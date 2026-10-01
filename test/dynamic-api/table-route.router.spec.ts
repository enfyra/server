import { describe, expect, it, vi } from 'vitest';
import { TableRouteRouter } from '../../src/modules/dynamic-api/repositories/table-route.router';

function createHandlers(overrides: Record<string, unknown> = {}): any {
  return {
    isSchemaRoutedTable: vi.fn(() => false),
    isTableDefinition: vi.fn(() => false),
    normalizeRouteMethods: vi.fn(),
    normalizeExtension: vi.fn(),
    assertColumnRuleUnique: vi.fn(),
    assertGuardCreate: vi.fn(),
    assertGuardUpdate: vi.fn(),
    assertGuardRuleCreate: vi.fn(),
    assertGuardRuleUpdate: vi.fn(),
    assertFlowTriggerBody: vi.fn(),
    normalizeUserPassword: vi.fn(),
    normalizeFolderSlug: vi.fn(),
    postStorageDefault: vi.fn(),
    postFlowJobs: vi.fn(),
    postUserRevocation: vi.fn(),
    createRouteMethodConfigsForRoute: vi.fn(),
    createRouteMethodConfigsForMethod: vi.fn(),
    removeIncompleteRouteMethodMatrix: vi.fn(),
    ...overrides,
  };
}

describe('TableRouteRouter route method config lifecycle', () => {
  it.each([
    ['enfyra_route', 'createRouteMethodConfigsForRoute'],
    ['enfyra_method', 'createRouteMethodConfigsForMethod'],
  ] as const)(
    'marks %s creation as critical and removes incomplete matrices',
    async (tableName, createMethod) => {
      const failure = new Error('matrix failed');
      const removeIncompleteRouteMethodMatrix = vi.fn(async () => undefined);
      const handlers = createHandlers({
        [createMethod]: vi.fn(async () => {
          throw failure;
        }),
        removeIncompleteRouteMethodMatrix,
      });
      const strategy = new TableRouteRouter(handlers).getStrategy(tableName);

      expect(strategy.requiresCriticalReload).toBe(true);
      await expect(
        strategy.afterCreateWrite?.({
          tableName,
          id: 42,
          body: { isSystem: true },
          existing: null,
        }),
      ).rejects.toBe(failure);
      expect(removeIncompleteRouteMethodMatrix).toHaveBeenCalledWith(
        tableName,
        42,
      );
    },
  );
});
