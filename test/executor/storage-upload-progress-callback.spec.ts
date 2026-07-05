import {
  mkdtempSync,
  rmSync,
  writeFileSync,
} from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { describe, expect, it, vi } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { UploadFileHelper } from '../../src/shared/helpers/upload-file.helper';
import type { TDynamicContext } from '../../src/shared/types';

function createService() {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => [],
    } as any,
    packageCdnLoaderService: {
      getPackageSources: () => [],
    } as any,
  });
}

describe('isolated executor storage upload progress callbacks', () => {
  it('lets dynamic scripts emit custom socket progress from $storage.$upload onProgress', async () => {
    const tempDir = mkdtempSync(join(tmpdir(), 'enfyra-storage-progress-'));
    const filePath = join(tempDir, 'avatar.png');
    writeFileSync(filePath, 'avatar');
    const emitted: any[] = [];
    const fileManagementService = {
      uploadFileAndCreateRecord: vi.fn(async (fileData) => {
        await fileData.onProgress?.({
          phase: 'storing',
          loaded: 3,
          total: 6,
          percent: 50,
          fileName: fileData.filename,
        });
        await fileData.onProgress?.({
          phase: 'storing',
          loaded: 6,
          total: 6,
          percent: 100,
          fileName: fileData.filename,
        });
        return { id: 'file-1', filename: fileData.filename };
      }),
    };
    const uploadFileHelper = new UploadFileHelper({
      fileManagementService: fileManagementService as any,
    });
    const ctx: TDynamicContext = {
      $helpers: {},
      $cache: {},
      $repos: {
        enfyra_file: {
          create: vi.fn(),
        },
      },
      $share: { $logs: [] },
      $user: { id: 'user-1' },
      $socket: {
        emitToUser: (userId: any, event: string, data: any) => {
          emitted.push({ userId, event, data });
        },
      },
      $uploadedFile: {
        originalname: 'avatar.png',
        mimetype: 'image/png',
        encoding: '7bit',
        path: filePath,
        size: 6,
        fieldname: 'file',
      },
    };
    ctx.$storage = uploadFileHelper.createStorageHelper(ctx);

    const service = createService();
    try {
      const result = await service.run(
        `return await $ctx.$storage.$upload({
          file: $ctx.$uploadedFile,
          onProgress: async (progress) => {
            await $ctx.$socket.emitToUser($ctx.$user.id, 'upload:progress', {
              phase: progress.phase,
              percent: progress.percent,
              loaded: progress.loaded,
              total: progress.total,
              fileName: progress.fileName,
            });
          },
        });`,
        ctx,
        5000,
      );

      expect(result).toEqual({ id: 'file-1', filename: 'avatar.png' });
      expect(emitted).toEqual([
        {
          userId: 'user-1',
          event: 'upload:progress',
          data: {
            phase: 'storing',
            percent: 50,
            loaded: 3,
            total: 6,
            fileName: 'avatar.png',
          },
        },
        {
          userId: 'user-1',
          event: 'upload:progress',
          data: {
            phase: 'storing',
            percent: 100,
            loaded: 6,
            total: 6,
            fileName: 'avatar.png',
          },
        },
      ]);
    } finally {
      service.onDestroy();
      rmSync(tempDir, { recursive: true, force: true });
    }
  });
});
