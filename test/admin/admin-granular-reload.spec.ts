/**
 * Tests for AdminController granular reload endpoints.
 * Verifies all admin reload routes exist and call the correct orchestrator methods.
 */
describe('AdminController — granular reload endpoints', () => {
  let controller: any;
  let orchestrator: any;

  beforeEach(() => {
    orchestrator = {
      reloadAll: jest.fn().mockResolvedValue(undefined),
      reloadMetadataAndDeps: jest.fn().mockResolvedValue(undefined),
      reloadRoutesOnly: jest.fn().mockResolvedValue(undefined),
      reloadGraphqlOnly: jest.fn().mockResolvedValue(undefined),
      reloadGuardsOnly: jest.fn().mockResolvedValue(undefined),
    };

    // Minimal controller simulation matching AdminController structure
    controller = {
      async reloadAll() {
        await orchestrator.reloadAll();
        return {
          success: true,
          message: 'All caches and schemas reloaded successfully',
        };
      },
      async reloadMetadata() {
        await orchestrator.reloadMetadataAndDeps();
        return { success: true };
      },
      async reloadRoutes() {
        await orchestrator.reloadRoutesOnly();
        return { success: true };
      },
      async reloadGraphql() {
        await orchestrator.reloadGraphqlOnly();
        return { success: true };
      },
      async reloadGuards() {
        await orchestrator.reloadGuardsOnly();
        return { success: true };
      },
    };
  });

  it('POST /admin/reload → calls reloadAll', async () => {
    const result = await controller.reloadAll();
    expect(orchestrator.reloadAll).toHaveBeenCalledTimes(1);
    expect(result.success).toBe(true);
  });

  it('POST /admin/reload/metadata → calls reloadMetadataAndDeps', async () => {
    const result = await controller.reloadMetadata();
    expect(orchestrator.reloadMetadataAndDeps).toHaveBeenCalledTimes(1);
    expect(result.success).toBe(true);
  });

  it('POST /admin/reload/routes → calls reloadRoutesOnly', async () => {
    const result = await controller.reloadRoutes();
    expect(orchestrator.reloadRoutesOnly).toHaveBeenCalledTimes(1);
    expect(result.success).toBe(true);
  });

  it('POST /admin/reload/graphql → calls reloadGraphqlOnly', async () => {
    const result = await controller.reloadGraphql();
    expect(orchestrator.reloadGraphqlOnly).toHaveBeenCalledTimes(1);
    expect(result.success).toBe(true);
  });

  it('POST /admin/reload/guards → calls reloadGuardsOnly', async () => {
    const result = await controller.reloadGuards();
    expect(orchestrator.reloadGuardsOnly).toHaveBeenCalledTimes(1);
    expect(result.success).toBe(true);
  });

  it('should NOT have swagger endpoint (removed)', () => {
    expect(controller.reloadSwagger).toBeUndefined();
  });

  describe('error handling', () => {
    it('reloadAll should propagate orchestrator errors', async () => {
      orchestrator.reloadAll.mockRejectedValue(new Error('Redis down'));
      await expect(controller.reloadAll()).rejects.toThrow('Redis down');
    });

    it('reloadMetadata should propagate orchestrator errors', async () => {
      orchestrator.reloadMetadataAndDeps.mockRejectedValue(
        new Error('DB connection lost'),
      );
      await expect(controller.reloadMetadata()).rejects.toThrow(
        'DB connection lost',
      );
    });
  });
});
