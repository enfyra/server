import { describe, expect, it, vi } from 'vitest';
import { Readable } from 'stream';
import { FileManagementService } from '../../src/modules/file-management/services/file-management.service';
import { DynamicMutationLifecycleService } from '../../src/modules/dynamic-api/services/dynamic-mutation-lifecycle.service';
import { markMutationPersistence } from '../../src/shared/utils/mutation-outcome.util';
import { runWithDeferredDynamicTransactionEffects } from '../../src/shared/utils/dynamic-transaction-effects.util';
import type { TDynamicContext } from '../../src/shared/types';

function makeMp4Buffer() {
  return Buffer.concat([
    Buffer.from([0x00, 0x00, 0x00, 0x18]),
    Buffer.from('ftypmp42', 'ascii'),
    Buffer.alloc(16),
  ]);
}

function makeService(overrides: Record<string, any> = {}) {
  const runtimeRegistryService = {
    getDefaultStorageConfig: vi.fn(() => null),
    getStorageConfigByType: vi.fn(() => ({
      id: 'local',
      type: 'Local Storage',
      isEnabled: true,
    })),
    getStorageConfigById: vi.fn((id: string) => ({
      id,
      type: id === 'local' ? 'Local Storage' : 'Amazon S3',
      isEnabled: true,
      bucket: 'bucket',
      region: 'region',
      accessKeyId: 'key',
      secretAccessKey: 'secret',
    })),
  };
  const storageService = {
    upload: vi.fn(async (stream: Readable, relativePath: string) => {
      for await (const _ of stream) {
      }
      return { location: relativePath };
    }),
    delete: vi.fn(async () => undefined),
  };
  const service = new FileManagementService({
    queryBuilderService: {
      getPkField: vi.fn(() => 'id'),
      isMongoDb: vi.fn(() => false),
    } as any,
    runtimeRegistryService:
      overrides.runtimeRegistryService ?? runtimeRegistryService,
    storageFactoryService: {
      getStorageServiceByConfig: vi.fn(() => storageService),
    } as any,
    fileCleanupService: { enqueue: async (location: string, config: string) => storageService.delete(location, runtimeRegistryService.getStorageConfigById(config)) } as any,
  });

  return { service, storageService, runtimeRegistryService };
}

describe('FileManagementService file replacement', () => {
  it('passes uploaded files through the storage adapter as a stream', async () => {
    const { service, storageService } = makeService();
    const fileRepo = {
      create: vi.fn(async ({ data }) => ({ data: [data] })),
    };

    await service.uploadFileAndCreateRecord(
      {
        filename: 'backup.sql.gz',
        mimetype: 'application/gzip',
        stream: Readable.from('backup'),
        size: 1024,
      },
      {
        storageConfig: 'local',
      },
      fileRepo,
    );

    expect(storageService.upload).toHaveBeenCalledWith(
      expect.any(Readable),
      expect.stringMatching(/^uploads\/backupsql_/),
      'application/gzip',
      expect.objectContaining({ id: 'local' }),
    );
  });

  it('uses the default storage config when upload omits storageConfig', async () => {
    const runtimeRegistryService = {
      getDefaultStorageConfig: vi.fn(() => ({
        id: 'r2-default',
        type: 'Cloudflare R2',
        isEnabled: true,
        isDefault: true,
        bucket: 'default-bucket',
      })),
      getStorageConfigByType: vi.fn(),
      getStorageConfigById: vi.fn(),
    };
    const { service, storageService } = makeService({ runtimeRegistryService });
    const fileRepo = {
      create: vi.fn(async ({ data }) => ({ data: [data] })),
    };

    await service.uploadFileAndCreateRecord(
      {
        filename: 'report.pdf',
        mimetype: 'application/pdf',
        stream: Readable.from('pdf'),
        size: 3,
      },
      {},
      fileRepo,
    );

    expect(runtimeRegistryService.getDefaultStorageConfig).toHaveBeenCalled();
    expect(runtimeRegistryService.getStorageConfigByType).not.toHaveBeenCalled();
    expect(storageService.upload).toHaveBeenCalledWith(
      expect.any(Readable),
      expect.stringMatching(/^uploads\/report_/),
      'application/pdf',
      expect.objectContaining({ id: 'r2-default' }),
    );
    expect(fileRepo.create).toHaveBeenCalledWith({
      data: expect.objectContaining({
        storageConfig: { id: 'r2-default' },
      }),
    });
  });

  it('normalizes replacement MIME metadata and updates the record to the new blob location', async () => {
    const { service, storageService } = makeService();
    const fileRepo = {
      update: vi.fn(async ({ data }) => ({ data: [data] })),
    };
    const buffer = makeMp4Buffer();

    await service.replaceFileAndUpdateRecord(
      fileRepo,
      'file-1',
      {
        id: 'file-1',
        filename: 'old.txt',
        mimetype: 'text/plain',
        type: 'document',
        filesize: 3,
        location: 'uploads/old.txt',
        storageConfig: { id: 's3' },
        status: 'active',
        isPublished: true,
      },
      {
        filename: 'clip.txt',
        mimetype: 'text/plain',
        stream: Readable.from(buffer),
        signatureBuffer: buffer,
        size: buffer.length,
      },
    );

    expect(storageService.upload).toHaveBeenCalledWith(
      expect.any(Readable),
      expect.stringMatching(/^uploads\/clip_/),
      'video/mp4',
      expect.objectContaining({ id: 's3' }),
    );
    expect(fileRepo.update).toHaveBeenCalledWith({
      id: 'file-1',
      data: expect.objectContaining({
        filename: 'clip.mp4',
        mimetype: 'video/mp4',
        type: 'video',
        location: expect.stringMatching(/^uploads\/clip_/),
        storageConfig: { id: 's3' },
      }),
    });
    expect(storageService.delete).toHaveBeenCalledWith(
      'uploads/old.txt',
      expect.objectContaining({ id: 's3' }),
    );
  });

  it('rolls back the newly uploaded blob when the metadata update fails', async () => {
    const { service, storageService } = makeService();
    const fileRepo = {
      update: vi.fn(async () => {
        markMutationPersistence('not_persisted');
        throw new Error('db failed');
      }),
    };
    const buffer = makeMp4Buffer();

    await expect(
      service.replaceFileAndUpdateRecord(
        fileRepo,
        'file-1',
        {
          id: 'file-1',
          location: 'uploads/old.txt',
          storageConfig: { id: 's3' },
          status: 'active',
          isPublished: true,
        },
        {
          filename: 'clip.mp4',
          mimetype: 'video/mp4',
          stream: Readable.from(buffer),
          signatureBuffer: buffer,
          size: buffer.length,
        },
      ),
    ).rejects.toThrow('db failed');

    expect(storageService.delete).toHaveBeenCalledWith(
      expect.stringMatching(/^uploads\/clip_/),
      expect.objectContaining({ id: 's3' }),
    );
    expect(storageService.delete).not.toHaveBeenCalledWith(
      'uploads/old.txt',
      expect.anything(),
    );
  });

  it.each(['upload', 'replace'])('keeps the committed blob after a %s post-write result failure', async operation => {
    const { service, storageService } = makeService();
    const lifecycle = new DynamicMutationLifecycleService();
    const mutate = async () => lifecycle.run({ context: { tableName: 'enfyra_file', id: 1, body: {}, existing: null }, persist: async () => ({}), buildResult: () => { throw new Error('result read failed'); }, reload: async () => {} });
    const repository = { create: mutate, update: mutate };
    const file = { filename: 'new.txt', mimetype: 'text/plain', stream: Readable.from('new'), size: 3 };
    const action = operation === 'upload' ? service.uploadFileAndCreateRecord(file, { storageConfig: 's3' }, repository) : service.replaceFileAndUpdateRecord(repository, '1', { location: 'uploads/old.txt', storageConfig: 's3' }, file);
    await expect(action).rejects.toThrow('result read failed');
    expect(storageService.delete).not.toHaveBeenCalled();
  });

  it('retains a newly uploaded blob when persistence outcome is unknown', async () => {
    const { service, storageService } = makeService();
    const lifecycle = new DynamicMutationLifecycleService();
    await expect(service.uploadFileAndCreateRecord({ filename: 'new.txt', mimetype: 'text/plain', stream: Readable.from('new'), size: 3 }, { storageConfig: 's3' }, { create: () => lifecycle.run({ context: { tableName: 'enfyra_file', id: 1, body: {}, existing: null }, persist: async () => { throw new Error('write acknowledgement lost'); }, buildResult: () => ({}), reload: async () => {} }) })).rejects.toThrow('write acknowledgement lost');
    expect(storageService.delete).not.toHaveBeenCalled();
  });

  it('does not delete bytes when repository deletion is denied', async () => {
    const { service, storageService } = makeService();
    await expect(service.deleteFileAndRecord({ delete: async () => { throw new Error('denied'); } }, 1, { location: 'uploads/old.txt', storageConfig: 's3' })).rejects.toThrow('denied');
    expect(storageService.delete).not.toHaveBeenCalled();
  });

  it('keeps the old blob when an outer metadata transaction rolls back after replacement', async () => {
    const { service, storageService } = makeService();
    const ctx = {} as TDynamicContext;
    await expect(runWithDeferredDynamicTransactionEffects(ctx, async () => {
      await service.replaceFileAndUpdateRecord({ update: async () => ({}) }, 1, { location: 'uploads/old.txt', storageConfig: 's3' }, { filename: 'new.txt', mimetype: 'text/plain', stream: Readable.from('new'), size: 3 });
      throw new Error('rollback');
    })).rejects.toThrow('rollback');
    expect(storageService.delete).not.toHaveBeenCalledWith('uploads/old.txt', expect.anything());
  });

  it('rejects storageConfig changes when no replacement blob is provided', async () => {
    const { service } = makeService();

    await expect(
      service.updateFileMetadataRecord(
        { update: vi.fn() },
        'file-1',
        { storageConfig: { id: 'local' } },
        { storageConfig: { id: 's3' } },
      ),
    ).rejects.toThrow(
      'Changing storageConfig requires replacing the file blob in the same request',
    );
  });
});
