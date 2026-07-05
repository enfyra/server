import {
  closeSync,
  mkdtempSync,
  openSync,
  rmSync,
  truncateSync,
  writeFileSync,
} from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { Readable } from 'stream';
import { describe, expect, it, vi } from 'vitest';
import { UploadFileHelper } from '../../src/shared/helpers/upload-file.helper';
import type { TDynamicContext } from '../../src/shared/types';

function makeContext(): TDynamicContext {
  return {
    $helpers: {},
    $cache: {},
    $repos: {
      enfyra_file: {
        create: vi.fn(),
      },
    },
    $share: { $logs: [] },
  };
}

describe('UploadFileHelper', () => {
  it('uploads an existing request file as a stream without requiring a buffer', async () => {
    const tempDir = mkdtempSync(join(tmpdir(), 'enfyra-upload-helper-'));
    const filePath = join(tempDir, 'request-upload.txt');
    writeFileSync(filePath, 'request body');
    const fileManagementService = {
      uploadFileAndCreateRecord: vi.fn(async (fileData) => {
        expect(fileData.stream).toBeInstanceOf(Readable);
        for await (const _ of fileData.stream) {
        }
        return { data: [{ id: 1 }] };
      }),
    };
    const helper = new UploadFileHelper({
      fileManagementService: fileManagementService as any,
    });

    try {
      const uploadFile = helper.createStorageHelper(makeContext()).$upload;

      await uploadFile({
        file: {
          originalname: 'request-upload.txt',
          mimetype: 'text/plain',
          encoding: '7bit',
          path: filePath,
          size: 12,
          fieldname: 'file',
        },
        storageConfig: 1,
      });

      const fileData =
        fileManagementService.uploadFileAndCreateRecord.mock.calls[0][0];
      expect(fileData).not.toHaveProperty('signatureBuffer');
      expect(
        fileManagementService.uploadFileAndCreateRecord,
      ).toHaveBeenCalledWith(
        expect.objectContaining({
          filename: 'request-upload.txt',
          mimetype: 'text/plain',
          size: 12,
        }),
        expect.objectContaining({ storageConfig: 1 }),
        expect.anything(),
      );
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('streams large request files in chunks instead of buffering the whole file', async () => {
    const tempDir = mkdtempSync(join(tmpdir(), 'enfyra-large-upload-helper-'));
    const filePath = join(tempDir, 'large-request-upload.bin');
    const fileSize = 128 * 1024 * 1024;
    closeSync(openSync(filePath, 'w'));
    truncateSync(filePath, fileSize);
    let totalBytes = 0;
    let largestChunk = 0;
    const fileManagementService = {
      uploadFileAndCreateRecord: vi.fn(async (fileData) => {
        expect(fileData.stream).toBeInstanceOf(Readable);
        for await (const chunk of fileData.stream) {
          const size = Buffer.byteLength(chunk);
          totalBytes += size;
          largestChunk = Math.max(largestChunk, size);
        }
        return { data: [{ id: 1 }] };
      }),
    };
    const helper = new UploadFileHelper({
      fileManagementService: fileManagementService as any,
    });

    try {
      const uploadFile = helper.createStorageHelper(makeContext()).$upload;

      await uploadFile({
        file: {
          originalname: 'large-request-upload.bin',
          mimetype: 'application/octet-stream',
          encoding: '7bit',
          path: filePath,
          size: fileSize,
          fieldname: 'file',
        },
      });

      const fileData =
        fileManagementService.uploadFileAndCreateRecord.mock.calls[0][0];
      expect(fileData).not.toHaveProperty('signatureBuffer');
      expect(totalBytes).toBe(fileSize);
      expect(largestChunk).toBeLessThan(1024 * 1024);
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  });

  it('keeps buffer uploads available for generated files', async () => {
    const buffer = Buffer.from('generated file');
    const fileManagementService = {
      uploadFileAndCreateRecord: vi.fn(async (fileData) => {
        expect(fileData.stream).toBeInstanceOf(Readable);
        return { data: [{ id: 1 }] };
      }),
    };
    const helper = new UploadFileHelper({
      fileManagementService: fileManagementService as any,
    });
    const uploadFile = helper.createStorageHelper(makeContext()).$upload;

    await uploadFile({
      filename: 'generated.txt',
      mimetype: 'text/plain',
      buffer,
    });

    expect(
      fileManagementService.uploadFileAndCreateRecord,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        filename: 'generated.txt',
        mimetype: 'text/plain',
        signatureBuffer: buffer,
        size: buffer.length,
      }),
      expect.anything(),
      expect.anything(),
    );
  });

  it('passes upload progress callbacks to file management uploads', async () => {
    const onProgress = vi.fn();
    const fileManagementService = {
      uploadFileAndCreateRecord: vi.fn(async (fileData) => {
        await fileData.onProgress?.({
          phase: 'storing',
          loaded: 4,
          total: 8,
          percent: 50,
          fileName: fileData.filename,
        });
        return { data: [{ id: 1 }] };
      }),
    };
    const helper = new UploadFileHelper({
      fileManagementService: fileManagementService as any,
    });

    await helper.createStorageHelper(makeContext()).$upload({
      filename: 'generated.txt',
      mimetype: 'text/plain',
      buffer: Buffer.from('generated file'),
      onProgress,
    });

    expect(onProgress).toHaveBeenCalledWith({
      phase: 'storing',
      loaded: 4,
      total: 8,
      percent: 50,
      fileName: 'generated.txt',
    });
  });

  it('rejects ambiguous uploads that pass both a request file and a buffer', async () => {
    const helper = new UploadFileHelper({
      fileManagementService: {
        uploadFileAndCreateRecord: vi.fn(),
      } as any,
    });
    const uploadFile = helper.createStorageHelper(makeContext()).$upload;

    await expect(
      uploadFile({
        file: {
          originalname: 'request-upload.txt',
          mimetype: 'text/plain',
          encoding: '7bit',
          path: '/tmp/request-upload.txt',
          size: 12,
          fieldname: 'file',
        },
        filename: 'generated.txt',
        mimetype: 'text/plain',
        buffer: Buffer.from('generated file'),
      }),
    ).rejects.toThrow(
      'Pass either file or buffer to $storage.$upload, not both',
    );
  });

  it('registers an already-uploaded storage object without uploading bytes', async () => {
    const fileManagementService = {
      registerExternalFileRecord: vi.fn(async () => ({ data: [{ id: 12 }] })),
    };
    const helper = new UploadFileHelper({
      fileManagementService: fileManagementService as any,
    });

    const result = await helper
      .createStorageHelper(makeContext())
      .$registerFile({
        filename: 'backup.sql.gz',
        mimetype: 'application/gzip',
        location: 'backups/project-1/backup.sql.gz',
        size: 1024,
        storageConfig: 7,
        verifyExists: true,
      });

    expect(result).toEqual({ data: [{ id: 12 }] });
    expect(
      fileManagementService.registerExternalFileRecord,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        filename: 'backup.sql.gz',
        mimetype: 'application/gzip',
        location: 'backups/project-1/backup.sql.gz',
        size: 1024,
      }),
      expect.objectContaining({
        storageConfig: 7,
        verifyExists: true,
      }),
      expect.anything(),
    );
  });
});
