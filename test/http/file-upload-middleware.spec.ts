import { describe, expect, it, vi } from 'vitest';
import {
  emitUploadProgress,
  resolveUploadFileSizeLimitBytes,
  UPLOAD_PROGRESS_EVENT,
} from '../../src/http/middlewares/file-upload.middleware';

describe('file upload middleware limit resolution', () => {
  it('uses the global upload limit when the route has no override', () => {
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, null)).toBe(
      10 * 1024 * 1024,
    );
  });

  it('uses a positive route upload limit override in MB', () => {
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, 128)).toBe(
      128 * 1024 * 1024,
    );
  });

  it('ignores invalid route upload limit overrides', () => {
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, 0)).toBe(
      10 * 1024 * 1024,
    );
    expect(resolveUploadFileSizeLimitBytes(10 * 1024 * 1024, 'abc')).toBe(
      10 * 1024 * 1024,
    );
  });

  it('emits authenticated upload progress with the client supplied upload id', () => {
    const dynamicWebSocketGateway = {
      emitToUser: vi.fn(),
    };
    const req = {
      uploadProgressId: 'client-upload-1',
      user: { id: 'user-1' },
      routeData: { path: '/files/upload' },
      method: 'POST',
    };

    emitUploadProgress(req, dynamicWebSocketGateway as any, {
      phase: 'receiving',
      loaded: 25,
      total: 100,
      percent: 25.4,
      fileName: 'avatar.png',
    });

    expect(dynamicWebSocketGateway.emitToUser).toHaveBeenCalledWith(
      'user-1',
      UPLOAD_PROGRESS_EVENT,
      {
        uploadId: 'client-upload-1',
        phase: 'receiving',
        loaded: 25,
        total: 100,
        percent: 25,
        fileName: 'avatar.png',
        route: '/files/upload',
        method: 'POST',
      },
    );
  });

  it('does not emit progress without an upload id', () => {
    const dynamicWebSocketGateway = {
      emitToUser: vi.fn(),
    };

    emitUploadProgress(
      { user: { id: 'user-1' }, method: 'POST' },
      dynamicWebSocketGateway as any,
      {
        phase: 'receiving',
        loaded: 25,
        total: 100,
        percent: 25,
      },
    );

    expect(dynamicWebSocketGateway.emitToUser).not.toHaveBeenCalled();
  });
});
