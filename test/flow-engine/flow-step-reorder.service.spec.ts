import { describe, expect, it, vi } from "vitest";

import { FlowService } from "../../src/modules/flow/services/flow.service";

function createService(records: any[]) {
  const repo = {
    find: vi.fn(async () => ({ data: records })),
    update: vi.fn(async ({ id, data }) => ({ data: [{ id, ...data }] })),
  };
  const transaction = {
    run: vi.fn(async (callback: () => Promise<unknown>) => callback()),
  };
  const context = { $transaction: transaction, $repos: {} as any };
  const service = new FlowService({
    flowQueue: {} as any,
    runtimeRegistryService: {} as any,
    executorEngineService: {} as any,
    repoRegistryService: {
      createReposProxy: vi.fn(() => ({ main: repo })),
    } as any,
    dynamicContextFactory: {
      createBase: vi.fn(() => context),
    } as any,
    queryBuilderService: {
      getPkField: vi.fn(() => "id"),
    } as any,
  });
  return { service, repo, transaction };
}

describe("FlowService.swapStepOrder", () => {
  it("swaps sibling orders inside one transaction", async () => {
    const { service, repo, transaction } = createService([
      { id: 1, flow: { id: 10 }, parent: null, branch: null, stepOrder: 2 },
      { id: 2, flow: { id: 10 }, parent: null, branch: null, stepOrder: 3 },
    ]);

    await service.swapStepOrder(
      {
        flowId: 10,
        currentId: 1,
        swapWithId: 2,
        expectedCurrentOrder: 2,
        expectedSwapOrder: 3,
      },
      { id: "user-1" },
    );

    expect(transaction.run).toHaveBeenCalledOnce();
    expect(repo.update).toHaveBeenNthCalledWith(1, {
      id: 1,
      data: { stepOrder: 3 },
    });
    expect(repo.update).toHaveBeenNthCalledWith(2, {
      id: 2,
      data: { stepOrder: 2 },
    });
  });

  it("rejects a stale order without writing", async () => {
    const { service, repo } = createService([
      { id: 1, flow: { id: 10 }, parent: null, branch: null, stepOrder: 4 },
      { id: 2, flow: { id: 10 }, parent: null, branch: null, stepOrder: 3 },
    ]);

    await expect(
      service.swapStepOrder(
        {
          flowId: 10,
          currentId: 1,
          swapWithId: 2,
          expectedCurrentOrder: 2,
          expectedSwapOrder: 3,
        },
        { id: "user-1" },
      ),
    ).rejects.toThrow("Flow step order changed");
    expect(repo.update).not.toHaveBeenCalled();
  });
});
