import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';

jest.mock('multer', () => {
  const multerMock: any = jest.fn((opts: any) => ({
    single: (field: string) => (req: any, _res: any, cb: any) => {
      if (req.__simulateFile) {
        const tmpPath = path.join(os.tmpdir(), `test-upload-${Date.now()}`);
        fs.writeFileSync(tmpPath, req.__simulateFile.content);
        req.file = {
          path: tmpPath,
          originalname: req.__simulateFile.originalname ?? 'test.txt',
          mimetype: 'text/plain',
          encoding: 'utf8',
          size: req.__simulateFile.content.length,
          fieldname: field,
        };
      }
      cb(null);
    },
  }));
  multerMock.diskStorage = jest.fn(() => ({}));
  multerMock.memoryStorage = jest.fn(() => ({}));
  return multerMock;
});

import { FileUploadMiddleware } from '../../src/shared/middleware/file-upload.middleware';

function makeRes() {
  return {} as any;
}

function makeReq(overrides: any = {}): any {
  return {
    method: 'POST',
    headers: { 'content-type': 'multipart/form-data; boundary=---' },
    routeData: { context: { $body: {} } },
    ...overrides,
  };
}

describe('FileUploadMiddleware — disk storage', () => {
  let middleware: FileUploadMiddleware;

  beforeEach(() => {
    const mockSettingCache = {
      getMaxUploadFileSizeBytes: () => 10 * 1024 * 1024,
    } as any;
    middleware = new FileUploadMiddleware(mockSettingCache);
  });

  it('reads buffer from temp file and deletes the temp file', async () => {
    const fileContent = Buffer.from('hello world');
    const req = makeReq({
      __simulateFile: { content: fileContent, originalname: 'test.txt' },
    });

    await new Promise<void>((resolve, reject) => {
      middleware.use(req, makeRes(), (err?: any) =>
        err ? reject(err) : resolve(),
      );
    });

    expect(req.file).toBeDefined();
    expect(Buffer.isBuffer(req.file.buffer)).toBe(true);
    expect(req.file.buffer.toString()).toBe('hello world');

    expect(fs.existsSync(req.file.path ?? '')).toBe(false);
  });

  it('sets $uploadedFile on context when routeData is present', async () => {
    const req = makeReq({
      __simulateFile: { content: Buffer.from('data'), originalname: 'img.png' },
    });

    await new Promise<void>((resolve, reject) => {
      middleware.use(req, makeRes(), (err?: any) =>
        err ? reject(err) : resolve(),
      );
    });

    expect(req.routeData.context.$uploadedFile).toBeDefined();
    expect(req.routeData.context.$uploadedFile.originalname).toBe('img.png');
    expect(Buffer.isBuffer(req.routeData.context.$uploadedFile.buffer)).toBe(
      true,
    );
  });

  it('skips processing for non-multipart requests', async () => {
    const req: any = {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
    };
    const next = jest.fn();

    middleware.use(req, makeRes(), next);

    expect(next).toHaveBeenCalledWith();
    expect(req.file).toBeUndefined();
  });

  it('skips processing for GET requests', async () => {
    const req: any = {
      method: 'GET',
      headers: { 'content-type': 'multipart/form-data; boundary=---' },
    };
    const next = jest.fn();

    middleware.use(req, makeRes(), next);

    expect(next).toHaveBeenCalledWith();
  });

  it('does not throw when temp file cleanup fails', async () => {
    const fileContent = Buffer.from('test');
    const req = makeReq({ __simulateFile: { content: fileContent } });
    const unlinkSpy = jest.spyOn(fs, 'unlinkSync').mockImplementation(() => {
      throw new Error('Permission denied');
    });

    await expect(
      new Promise<void>((resolve, reject) => {
        middleware.use(req, makeRes(), (err?: any) =>
          err ? reject(err) : resolve(),
        );
      }),
    ).resolves.toBeUndefined();

    expect(req.file.buffer).toBeDefined();
    unlinkSpy.mockRestore();
  });
});
