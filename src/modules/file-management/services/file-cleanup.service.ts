import { Worker, type Queue } from 'bullmq';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { EnvService } from '../../../shared/services';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import type { StorageFactoryService } from '../storage/storage-factory.service';
import type { FileCleanupJob } from '../types/file-cleanup.types';
import { SYSTEM_QUEUES } from '../../../shared/utils/constant';

export class FileCleanupService {
  private worker: Worker<FileCleanupJob> | undefined;

  constructor(private readonly deps: {
    fileCleanupQueue: Queue<FileCleanupJob>;
    queryBuilderService: QueryBuilderService;
    runtimeRegistryService: RuntimeRegistryService;
    storageFactoryService: StorageFactoryService;
    envService: EnvService;
  }) {}

  async init(): Promise<void> {
    this.worker = new Worker<FileCleanupJob>(SYSTEM_QUEUES.FILE_CLEANUP, job => this.process(job.data), {
      prefix: this.deps.envService.get('NODE_NAME'),
      connection: this.deps.fileCleanupQueue.opts.connection,
      concurrency: 1,
    });
    await this.worker.waitUntilReady();
  }

  async enqueue(location: string, storageConfigId?: number | string): Promise<void> {
    await this.deps.fileCleanupQueue.add('delete-unreferenced-object', {
      location,
      ...(storageConfigId !== undefined ? { storageConfigId } : {}),
    }, {
      attempts: 8,
      backoff: { type: 'exponential', delay: 1000 },
      removeOnComplete: { count: 100, age: 86400 },
      removeOnFail: false,
    });
  }

  async process(job: FileCleanupJob): Promise<void> {
    const config = job.storageConfigId != null
      ? this.deps.runtimeRegistryService.getStorageConfigById(job.storageConfigId)
      : this.deps.runtimeRegistryService.getStorageConfigByType('Local Storage');
    if (!config || config.isEnabled === false) throw new Error('Cleanup storage configuration is unavailable');
    const references = await this.deps.queryBuilderService.transaction(() =>
      this.deps.queryBuilderService.find({
        table: 'enfyra_file',
        filter: { location: { _eq: job.location } },
        fields: [this.deps.queryBuilderService.getPkField()],
        limit: 1,
        meta: [],
      }),
    );
    if (references.data?.length) return;
    const storage = this.deps.storageFactoryService.getStorageServiceByConfig(config);
    await storage.delete(job.location, config);
  }

  async onDestroy(): Promise<void> {
    await this.worker?.close();
    this.worker = undefined;
  }
}
