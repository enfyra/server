import { expect, it, vi } from 'vitest';
import { FlowExecutionQueueService } from '../../src/modules/flow/queues/flow-execution-queue.service';
import { FlowService } from '../../src/modules/flow/services/flow.service';

function fixture(flows: any[]) {
  const executed: string[] = [];
  const service = new FlowExecutionQueueService({ runtimeRegistryService: { requireActiveData: () => flows }, queryBuilderService: { find: async () => ({ data: [] }), insert: async () => ({ id: 1 }), update: async () => {} }, dynamicContextFactory: { createFlow: () => ({}) }, repoRegistryService: { createReposProxy: () => ({}) }, envService: { get: () => undefined }, websocketEmitService: {}, flowQueue: {} } as any);
  Object.assign(service, { executeStep: async (step: any) => { executed.push(step.key); return true; }, flowExecutionHistoryService: { start: async () => 1, finalize: vi.fn(async () => {}), updateProgress: async () => {} }, emitFlowEvent: () => {}, cleanupOldExecutions: async () => {} });
  const job: any = { id: 'job', data: { flowId: 1, flowName: 'old-name' }, updateProgress: async () => {} };
  return { service, job, executed };
}

it('resolves a queued flow by immutable id before its reusable name', async () => {
  const { service, job, executed } = fixture([{ id: 1, name: 'new-name', steps: [{ id: 1, key: 'correct', isEnabled: true }] }, { id: 2, name: 'old-name', steps: [{ id: 2, key: 'wrong', isEnabled: true }] }]);
  await service.process(job);
  expect(executed).toEqual(['correct']);
});

it('does not fall back to a reused name when a queued id no longer exists', async () => {
  const { service, job, executed } = fixture([{ id: 2, name: 'old-name', steps: [{ id: 2, key: 'wrong', isEnabled: true }] }]);
  await expect(service.process(job)).rejects.toThrow('not found');
  expect(executed).toEqual([]);
});

it('traverses condition children after a successful retry', async () => {
  const flow = { id: 1, name: 'old-name', steps: [{ id: 1, key: 'condition', type: 'condition', onError: 'retry', retryAttempts: 1, isEnabled: true, stepOrder: 1 }, { id: 2, parentId: 1, branch: 'true', key: 'child', type: 'script', isEnabled: true, stepOrder: 2 }] };
  const { service, job, executed } = fixture([flow]);
  let attempts = 0;
  (service as any).executeStep = async (step: any) => { executed.push(step.key); if (step.key === 'condition' && attempts++ === 0) throw new Error('retry'); return true; };
  await service.process(job);
  expect(executed).toEqual(['condition', 'condition', 'child']);
});

it('does not retry a successful condition when its child fails', async () => {
  const flow = { id: 1, name: 'old-name', steps: [{ id: 1, key: 'condition', type: 'condition', onError: 'retry', retryAttempts: 1, isEnabled: true, stepOrder: 1 }, { id: 2, parentId: 1, branch: 'true', key: 'child', type: 'script', onError: 'stop', isEnabled: true, stepOrder: 2 }] };
  const { service, job, executed } = fixture([flow]);
  (service as any).executeStep = async (step: any) => { executed.push(step.key); if (step.key === 'child') throw new Error('child failed'); return true; };
  await expect(service.process(job)).resolves.toMatchObject({ success: false, error: 'child failed' });
  expect(executed).toEqual(['condition', 'child']);
  expect((service as any).flowExecutionHistoryService.finalize).toHaveBeenCalledWith(
    flow, undefined, undefined, 1,
    expect.objectContaining({ error: expect.objectContaining({ failedStep: 'child' }) }),
  );
});

it('enforces one total flow budget across steps', async () => {
  const { service, job, executed } = fixture([{ id: 1, name: 'old-name', timeout: 10, steps: [{ id: 1, key: 'slow', isEnabled: true }, { id: 2, key: 'late', isEnabled: true }] }]);
  (service as any).executeStep = async (step: any) => { executed.push(step.key); await new Promise(resolve => setTimeout(resolve, 20)); return true; };
  await expect(service.process(job)).resolves.toMatchObject({ success: false, error: expect.stringMatching(/timed out/) });
  expect(executed).toEqual(['slow']);
});

it('recompiles changed draft source rather than inheriting saved compiled code', async () => {
  const flow = { id: 1, name: 'draft', steps: [{ id: 7, key: 'step', type: 'script', sourceCode: 'return 1;', compiledCode: 'return 1;', scriptLanguage: 'javascript' }] };
  const service = new FlowService({ runtimeRegistryService: { requireActiveData: () => [flow] } } as any);
  const selected = await (service as any).resolveTestFlowStep({ flowId: 1, stepId: 7, sourceCode: 'return 2;', scriptLanguage: 'javascript' });
  expect(selected.step.compiledCode).not.toBe('return 1;');
});
