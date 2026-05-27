import { describe, expect, it, vi } from 'vitest';
import { FileManagementService } from '../../src/modules/file-management/services/file-management.service';

function makeMp4Buffer() {
  return Buffer.concat([
    Buffer.from([0x00, 0x00, 0x00, 0x18]),
    Buffer.from('ftypmp42', 'ascii'),
    Buffer.alloc(16),
  ]);
}

function makeService(overrides: Record<string, any> = {}) {
  const storageConfigCacheService = {
    getStorageConfigByType: vi.fn(async () => ({
      id: 'local',
      type: 'Local Storage',
      isEnabled: true,
    })),
    getStorageConfigById: vi.fn(async (id: string) => ({
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
    upload: vi.fn(async (_buffer: Buffer, relativePath: string) => ({
      location: relativePath,
    })),
    delete: vi.fn(async () => undefined),
  };
  const service = new FileManagementService({
    queryBuilderService: {
      getPkField: vi.fn(() => 'id'),
      isMongoDb: vi.fn(() => false),
    } as any,
    storageConfigCacheService:
      overrides.storageConfigCacheService ?? storageConfigCacheService,
    storageFactoryService: {
      getStorageServiceByConfig: vi.fn(() => storageService),
    } as any,
  });

  return { service, storageService, storageConfigCacheService };
}

describe('FileManagementService file replacement', () => {
  it('normalizes replacement MIME metadata and updates the record to the new blob location', async () => {
    const { service, storageService } = makeService();
    const fileRepo = {
      update: vi.fn(async ({ data }) => ({ data: [data] })),
    };

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
        buffer: makeMp4Buffer(),
        size: makeMp4Buffer().length,
      },
    );

    expect(storageService.upload).toHaveBeenCalledWith(
      expect.any(Buffer),
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
        throw new Error('db failed');
      }),
    };

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
          buffer: makeMp4Buffer(),
          size: makeMp4Buffer().length,
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
