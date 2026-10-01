import { createServer, type Server } from 'node:http';
import { mkdtemp, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterEach, describe, expect, it, vi } from 'vitest';
import express from 'express';
import {
  fileUploadMiddleware,
  emitUploadProgress,
  resolveUploadFileSizeLimitBytes,
  resolveMultipartUploadConfig,
  validateMultipartFiles,
  UPLOAD_PROGRESS_EVENT,
} from '../../src/http/middlewares/file-upload.middleware';

describe('multipart HTTP upload', () => {
  const servers: Server[] = [];
  const testDirectories: string[] = [];
  const temporaryFiles: string[] = [];
  afterEach(async () => {
    await Promise.all(servers.splice(0).map(server => new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))));
    for (const path of temporaryFiles.splice(0)) await rm(path, { force: true });
    for (const path of testDirectories.splice(0)) await rm(path, { recursive: true, force: true });
  });

  async function send(method: 'POST' | 'PATCH' | 'PUT', config: object, form: FormData, replacementPath?: string) {
    const app = express();
    app.use((req: any, _res, next) => {
      req.routeData = { path: '/upload-test', routeMethodConfig: config, context: { $body: {} } };
      next();
    });
    app.use(fileUploadMiddleware({ getMaxUploadFileSizeBytes: () => 10 * 1024 * 1024 } as any));
    app.use(async (req: any, res) => {
      const files = req.routeData.context.$uploadFile;
      const paths = Object.values(files ?? {}).flat().map((file: any) => file.path);
      temporaryFiles.push(...paths);
      const contents = await Promise.all(paths.map(path => readFile(path, 'utf8')));
      if (replacementPath) {
        for (const file of Object.values(req.files ?? {}).flat() as Express.Multer.File[]) file.path = replacementPath;
      }
      res.json({ body: req.routeData.context.$body, files, contents, paths });
    });
    app.use((error: Error, _req: express.Request, res: express.Response, _next: express.NextFunction) => res.status(400).json({ message: error.message }));
    const server = createServer(app);
    servers.push(server);
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing test server address');
    const response = await fetch(`http://127.0.0.1:${address.port}/upload-test`, { method, body: form });
    return { status: response.status, result: await response.json() as any };
  }

  it('receives the default file key and form fields, then cleans up the temporary file', async () => {
    const form = new FormData();
    form.set('title', 'proposal');
    form.set('file', new Blob(['hello'], { type: 'text/plain' }), 'proposal.txt');
    const { status, result } = await send('POST', { requestBodyType: 'multipart', fileFields: [] }, form);
    expect(status).toBe(200);
    expect(result.body).toEqual({ title: 'proposal' });
    expect(result.files.file).toMatchObject({ originalname: 'proposal.txt', fieldname: 'file' });
    expect(result.contents).toEqual(['hello']);
    await vi.waitFor(async () => expect(await stat(result.paths[0]).then(() => true, () => false)).toBe(false));
  });

  it('cleans only request-owned temporary files even when file metadata paths change', async () => {
    const directory = await mkdtemp(join(tmpdir(), 'enfyra-upload-test-'));
    testDirectories.push(directory);
    const unrelatedPath = join(directory, 'sentinel.txt');
    await writeFile(unrelatedPath, 'preserve');
    const form = new FormData();
    form.set('file', new Blob(['upload']), '../../sentinel.txt');
    const { status, result } = await send('POST', { requestBodyType: 'multipart', fileFields: [] }, form, unrelatedPath);
    expect(status).toBe(200);
    await vi.waitFor(async () => expect(await stat(result.paths[0]).then(() => true, () => false)).toBe(false));
    expect(await readFile(unrelatedPath, 'utf8')).toBe('preserve');
  });

  it('accepts multiple named files on PATCH and rejects an unconfigured field by key', async () => {
    const config = { requestBodyType: 'multipart', fileFields: [{ name: 'attachments', maxCount: 2, required: true }] };
    const form = new FormData();
    form.append('attachments', new Blob(['first']), 'first.txt');
    form.append('attachments', new Blob(['second']), 'second.txt');
    const response = await send('PATCH', config, form);
    expect(response.status).toBe(200);
    expect(response.result.files.attachments).toHaveLength(2);
    expect(response.result.contents).toEqual(['first', 'second']);
    const unknown = new FormData();
    unknown.set('wrong', new Blob(['wrong']), 'wrong.txt');
    const rejected = await send('PUT', config, unknown);
    expect(rejected.status).toBe(400);
    expect(rejected.result.message).toContain('wrong');
  });

  it('enforces per-field MIME types, count and size while naming the field', async () => {
    const config = { requestBodyType: 'multipart', maxFiles: 2, fileFields: [{ name: 'avatar', maxCount: 1, maxFileSize: 1, allowedMimeTypes: ['image/png'] }] };
    const wrongMime = new FormData();
    wrongMime.set('avatar', new Blob(['text'], { type: 'text/plain' }), 'avatar.txt');
    const mimeResponse = await send('POST', config, wrongMime);
    expect(mimeResponse.status).toBe(400);
    expect(mimeResponse.result.message).toContain('avatar');
    const tooMany = new FormData();
    tooMany.append('avatar', new Blob(['one'], { type: 'image/png' }), 'one.png');
    tooMany.append('avatar', new Blob(['two'], { type: 'image/png' }), 'two.png');
    const countResponse = await send('PATCH', config, tooMany);
    expect(countResponse.status).toBe(400);
    expect(countResponse.result.message).toContain('avatar');
    const oversized = new FormData();
    oversized.set('avatar', new Blob([new Uint8Array(1024 * 1024 + 1)], { type: 'image/png' }), 'large.png');
    const sizeResponse = await send('PUT', config, oversized);
    expect(sizeResponse.status).toBe(400);
    expect(sizeResponse.result.message).toContain('avatar');
  });

  it('rejects missing required field by name', async () => {
    const form = new FormData();
    form.set('title', 'empty');
    const response = await send('POST', { requestBodyType: 'multipart', fileFields: [{ name: 'invoice', required: true, maxCount: 1 }] }, form);
    expect(response.status).toBe(400);
    expect(response.result.message).toContain('invoice');
  });
});

describe('file upload middleware limit resolution', () => {
  it('resolves a default file field and method-specific multipart fields', () => {
    expect(resolveMultipartUploadConfig({ routeMethodConfig: { requestBodyType: 'multipart', fileFields: [] } }))
      .toMatchObject({ fields: [{ name: 'file', maxCount: 1 }], maxFiles: 1 });
    expect(resolveMultipartUploadConfig({ routeMethodConfig: {
      requestBodyType: 'multipart', maxFiles: 3,
      fileFields: [{ name: 'attachments', maxCount: 2, required: true }, { name: 'avatar', maxCount: 1 }],
    } })).toMatchObject({ fields: [{ name: 'attachments', maxCount: 2 }, { name: 'avatar', maxCount: 1 }], maxFiles: 3 });
  });

  it('rejects missing required file fields with their exact key', () => {
    const config = resolveMultipartUploadConfig({ routeMethodConfig: {
      requestBodyType: 'multipart', fileFields: [{ name: 'attachments', maxCount: 2, required: true }],
    } });
    expect(() => validateMultipartFiles({}, config)).toThrow('attachments');
  });
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
