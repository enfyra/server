import { describe, expect, it, vi } from 'vitest';
import { FlowExecutionHistoryService } from '../../src/modules/flow/services/flow-execution-history.service';

function makeFlow() {
  return {
    id: 13,
    name: 'test-flow',
    steps: [],
  } as any;
}

function makeQueryBuilder() {
  return {
    insert: vi.fn(async (_table: string, data: any) => ({
      data: [{ id: 101, ...data }],
    })),
    update: vi.fn(async (_table: string, id: any, data: any) => ({
      data: [{ id, ...data }],
    })),
  } as any;
}

describe('FlowExecutionHistoryService', () => {
  it('start extracts the inserted record id', async () => {
    const qb = makeQueryBuilder();
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });
    const flow = makeFlow();

    const id = await service.start(flow, { key: 'val' }, { id: 'user-1' }, Date.now());
    expect(id).toBe(101);
    expect(qb.insert).toHaveBeenCalledWith('enfyra_flow_execution', {
      flow: flow.id,
      status: 'running',
      triggeredBy: 'user-1',
      payload: { key: 'val' },
      completedSteps: [],
      currentStep: null,
      error: null,
      startedAt: expect.any(Date),
      completedAt: null,
      duration: null,
    });
  });

  it('start returns null and logs on insert failure', async () => {
    const qb = makeQueryBuilder();
    qb.insert.mockRejectedValueOnce(new Error('db down'));
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });

    const id = await service.start(makeFlow(), {}, null, Date.now());
    expect(id).toBeNull();
  });

  it('finalize updates when executionHistoryId is present', async () => {
    const qb = makeQueryBuilder();
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });
    const flow = makeFlow();

    await service.finalize(flow, {}, null, 101, {
      status: 'completed',
      duration: 500,
    });

    expect(qb.update).toHaveBeenCalledWith('enfyra_flow_execution', 101, {
      status: 'completed',
      duration: 500,
    });
    expect(qb.insert).not.toHaveBeenCalled();
  });

  it('finalize falls back to insert when executionHistoryId is null', async () => {
    const qb = makeQueryBuilder();
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });
    const flow = makeFlow();

    await service.finalize(
      flow,
      { key: 'val' },
      { id: 'user-1' },
      null,
      { status: 'failed', error: { message: 'boom' } },
    );

    expect(qb.update).not.toHaveBeenCalled();
    expect(qb.insert).toHaveBeenCalledWith('enfyra_flow_execution', expect.objectContaining({
      flow: flow.id,
      status: 'failed',
      triggeredBy: 'user-1',
      payload: { key: 'val' },
      error: { message: 'boom' },
    }));
  });

  it('updateProgress updates the history row', async () => {
    const qb = makeQueryBuilder();
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });
    const flow = makeFlow();

    await service.updateProgress(flow, 101, {
      completedSteps: [{ key: 'step-1' }],
      currentStep: 'step-2',
      totalSteps: 3,
    });

    expect(qb.update).toHaveBeenCalledWith('enfyra_flow_execution', 101, {
      status: 'running',
      completedSteps: [{ key: 'step-1' }],
      currentStep: 'step-2',
    });
  });

  it('updateProgress is a no-op when executionHistoryId is null', async () => {
    const qb = makeQueryBuilder();
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });

    await service.updateProgress(makeFlow(), null, {
      completedSteps: [],
      currentStep: 'step-1',
    });

    expect(qb.update).not.toHaveBeenCalled();
    expect(qb.insert).not.toHaveBeenCalled();
  });

  it('updateProgress logs warning but does not throw on failure', async () => {
    const qb = makeQueryBuilder();
    qb.update.mockRejectedValueOnce(new Error('db timeout'));
    const service = new FlowExecutionHistoryService({ queryBuilderService: qb });

    await expect(
      service.updateProgress(makeFlow(), 101, {
        completedSteps: [],
        currentStep: 'step-1',
      }),
    ).resolves.toBeUndefined();
  });
});
