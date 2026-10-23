import { expect, it, vi } from 'vitest';
import { FileCleanupService } from '../../src/modules/file-management/services/file-cleanup.service';
import { Worker } from 'bullmq';

vi.mock('bullmq', async importOriginal => {
  const actual = await importOriginal<typeof import('bullmq')>();
  return { ...actual, Worker: vi.fn(class {
    waitUntilReady = async () => {};
    close = async () => {};
  }) };
});

it('uses the registered queue Redis connection for the cleanup worker', async () => {
  const connection = { host: 'synthetic.invalid', port: 16379, db: 4, tls: {} };
  const service = new FileCleanupService({
    fileCleanupQueue: { opts: { connection } },
    envService: { get: (key: string) => key === 'NODE_NAME' ? 'synthetic' : 'rediss://synthetic.invalid:16379/4' },
  } as any);
  try {
    await service.init();
    expect(Worker).toHaveBeenCalledWith('sys_file-cleanup', expect.any(Function), expect.objectContaining({ connection }));
  } finally { await service.onDestroy(); }
});

it('queues cleanup with bounded retries and never deletes a referenced object', async () => {
  const add = vi.fn(async () => ({}));
  const remove = vi.fn(async () => {});
  const find = vi.fn(async () => ({ data: [{ id: 1 }] }));
  const transaction = vi.fn(async (work: () => Promise<unknown>) => work());
  const service = new FileCleanupService({ fileCleanupQueue: { add }, queryBuilderService: { find, transaction, getPkField: () => 'id' }, runtimeRegistryService: { getStorageConfigById: () => ({ id: 's3', isEnabled: true }) }, storageFactoryService: { getStorageServiceByConfig: () => ({ delete: remove }) } } as any);
  await service.enqueue('uploads/old', 's3');
  expect(add).toHaveBeenCalledWith('delete-unreferenced-object', { location: 'uploads/old', storageConfigId: 's3' }, expect.objectContaining({ attempts: 8, backoff: { type: 'exponential', delay: 1000 }, removeOnFail: false }));
  await service.process({ location: 'uploads/old', storageConfigId: 's3' });
  expect(remove).not.toHaveBeenCalled();
  expect(transaction).toHaveBeenCalledOnce();
  find.mockResolvedValue({ data: [] });
  await service.process({ location: 'uploads/old', storageConfigId: 's3' });
  expect(remove).toHaveBeenCalledWith('uploads/old', expect.objectContaining({ id: 's3' }));
  find.mockRejectedValue(new Error('DB unavailable'));
  await expect(service.process({ location: 'uploads/old', storageConfigId: 's3' })).rejects.toThrow('DB unavailable');
  expect(remove).toHaveBeenCalledTimes(1);
});
