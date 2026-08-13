import { describe, expect, it, vi } from 'vitest';
import { DynamicTableRouteHandlerService } from '../../src/modules/dynamic-api/services/dynamic-table-route-handler.service';

function createService() {
  return new DynamicTableRouteHandlerService({
    bcryptService: { hash: vi.fn() } as any,
    flowQueueMaintenanceService: { removeFlowJobs: vi.fn() } as any,
    guardValidationService: {
      assertGuardCreate: vi.fn(),
      assertGuardUpdate: vi.fn(),
      assertGuardRuleBody: vi.fn(),
      assertGuardRuleUpdate: vi.fn(),
    } as any,
    queryBuilderService: {
      getPkField: vi.fn(() => 'id'),
      find: vi.fn(),
      update: vi.fn(),
    } as any,
    runtimeMetadataSchemaRouterService: { handles: vi.fn(() => false) } as any,
    userRevocationService: { publish: vi.fn() } as any,
  });
}

describe('DynamicTableRouteHandlerService', () => {
  it('keeps event and webhook trigger validation', () => {
    const service = createService();

    expect(() => service.assertFlowTriggerBody({ type: 'event' })).toThrow(
      'Event trigger requires table reference',
    );
    expect(() =>
      service.assertFlowTriggerBody({ type: 'event', table: 'articles' }),
    ).toThrow('Event trigger requires tableEvent (create|update|delete)');
    expect(() => service.assertFlowTriggerBody({ type: 'webhook' })).toThrow(
      'Webhook trigger requires route reference',
    );
  });

  it('keeps only available route methods across Mongo-like and patch inputs', () => {
    const service = createService();
    const getId = '507f1f77bcf86cd799439011';
    const postId = '507f1f77bcf86cd799439012';
    const deleteId = '507f1f77bcf86cd799439013';
    const body = {
      availableMethods: [{ _id: getId }, { _id: postId }],
      publicMethods: [{ _id: getId }, postId, deleteId],
    };

    service.normalizeRouteMethods(body, null, 'publicMethods');

    expect(body.publicMethods).toEqual([{ _id: getId }, postId]);

    const patchId = '507f1f77bcf86cd799439014';
    const patch = { publicMethods: [{ id: patchId }, { id: deleteId }] };
    service.normalizeRouteMethods(
      patch,
      { availableMethods: [{ id: patchId }] },
      'publicMethods',
    );

    expect(patch.publicMethods).toEqual([{ id: patchId }]);
  });
});
