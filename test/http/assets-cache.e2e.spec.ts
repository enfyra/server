import express from 'express';
import { EventEmitter2 } from 'eventemitter2';
import { createServer, type Server } from 'http';
import { AddressInfo } from 'net';
import { Readable, PassThrough } from 'stream';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FileAssetsService } from '../../src/modules/file-management';
import { registerAssetsRoutes } from '../../src/http/routes/assets.routes';
import { CACHE_EVENTS } from '../../src/shared/utils/cache-events.constants';

function makeFile(overrides: Record<string, any> = {}) {
  return {
    id: 'file-1',
    filename: 'avatar.txt',
    mimetype: 'text/plain',
    type: 'document',
    location: '/uploads/avatar.txt',
    filesize: Buffer.byteLength('asset-body'),
    isPublic: true,
    storageConfig: null,
    ...overrides,
  };
}

function makePermission(overrides: Record<string, any> = {}) {
  return {
    id: 'perm-1',
    isEnabled: true,
    file: { id: 'file-1' },
    role: { id: 'role-1', name: 'member' },
    allowedUsers: [],
    ...overrides,
  };
}

function makeQueryBuilder(state: { files: any[]; permissions: any[] }) {
  return {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn(async (args: any) => {
      if (args.table === 'enfyra_file') {
        const id = args.filter?.id?._eq;
        return {
          data: state.files.filter((file) => String(file.id) === String(id)),
        };
      }

      if (args.table === 'enfyra_file_permission') {
        const ids = args.filter?.id?._in;
        if (ids) {
          const set = new Set(ids.map(String));
          return {
            data: state.permissions.filter((permission) =>
              set.has(String(permission.id)),
            ),
          };
        }

        const fileId = args.filter?._and?.find((entry: any) => entry.file)?.file
          ?.id?._eq;
        return {
          data: state.permissions.filter((permission) => {
            const permissionFileId =
              permission.file?.id ?? permission.file?._id ?? permission.file;
            return (
              permission.isEnabled !== false &&
              String(permissionFileId) === String(fileId)
            );
          }),
        };
      }

      if (args.table === 'enfyra_role') {
        return { data: [] };
      }

      return { data: [] };
    }),
    findOne: vi.fn(async () => null),
  } as any;
}

function makeStorageFactory() {
  return {
    getStorageService: vi.fn(() => ({
      getStream: vi.fn(
        async (_location: string, _config: any, options?: any) => {
          const body = Buffer.from('asset-body');
          const payload = options?.range
            ? body.subarray(options.range.start, options.range.end + 1)
            : body;
          const stream = Readable.from([payload]);
          (stream as any).contentLength = payload.length;
          return stream;
        },
      ),
    })),
  } as any;
}

function makeService(state: { files: any[]; permissions: any[] }) {
  const eventEmitter = new EventEmitter2();
  const queryBuilderService = makeQueryBuilder(state);
  const storageFactoryService = makeStorageFactory();
  const fileManagementService = {
    getStorageConfigById: vi.fn(async () => ({
      id: 'local',
      type: 'Local Storage',
      isEnabled: true,
    })),
  } as any;

  const service = new FileAssetsService({
    queryBuilderService,
    fileManagementService,
    storageFactoryService,
    eventEmitter,
  });

  return { service, eventEmitter, queryBuilderService };
}

function countFinds(queryBuilderService: any, table: string) {
  return queryBuilderService.find.mock.calls.filter(
    ([args]: any[]) => args.table === table,
  ).length;
}

async function listen(app: express.Express): Promise<Server> {
  const server = createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  return server;
}

function close(server: Server): Promise<void> {
  return new Promise((resolve, reject) =>
    server.close((error) => (error ? reject(error) : resolve())),
  );
}

function makeResponse() {
  const res = new PassThrough() as any;
  res.headers = {};
  res.statusCode = 200;
  res.setHeader = vi.fn((name: string, value: any) => {
    res.headers[name.toLowerCase()] = value;
  });
  res.status = vi.fn((statusCode: number) => {
    res.statusCode = statusCode;
    return res;
  });
  res.json = vi.fn((body: any) => {
    res.body = body;
    res.end(JSON.stringify(body));
    return res;
  });
  res.resume();
  return res;
}

describe('assets route cache e2e', () => {
  let server: Server | null = null;

  afterEach(async () => {
    if (server) {
      await close(server);
      server = null;
    }
  });

  it('caches public file metadata lazily after the first HTTP asset request', async () => {
    const state = { files: [makeFile()], permissions: [] };
    const { service, queryBuilderService } = makeService(state);
    const app = express();
    registerAssetsRoutes(app, {
      cradle: { fileAssetsService: service },
    } as any);
    server = await listen(app);

    const { port } = server.address() as AddressInfo;
    const first = await fetch(`http://127.0.0.1:${port}/assets/file-1`);
    const second = await fetch(`http://127.0.0.1:${port}/assets/file-1`);

    expect(first.status).toBe(200);
    expect(second.status).toBe(200);
    expect(await second.text()).toBe('asset-body');
    expect(countFinds(queryBuilderService, 'enfyra_file')).toBe(1);
  });

  it('streams byte ranges for video playback', async () => {
    const state = {
      files: [
        makeFile({
          filename: 'clip.mp4',
          mimetype: 'video/mp4',
          type: 'video',
        }),
      ],
      permissions: [],
    };
    const { service } = makeService(state);
    const app = express();
    registerAssetsRoutes(app, {
      cradle: { fileAssetsService: service },
    } as any);
    server = await listen(app);

    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/assets/file-1`, {
      headers: { Range: 'bytes=0-4' },
    });

    expect(response.status).toBe(206);
    expect(response.headers.get('accept-ranges')).toBe('bytes');
    expect(response.headers.get('content-range')).toBe('bytes 0-4/10');
    expect(response.headers.get('content-length')).toBe('5');
    expect(response.headers.get('content-type')).toBe('video/mp4');
    expect(await response.text()).toBe('asset');
  });

  it('rejects invalid asset byte ranges', async () => {
    const state = {
      files: [
        makeFile({
          filename: 'clip.mp4',
          mimetype: 'video/mp4',
          type: 'video',
        }),
      ],
      permissions: [],
    };
    const { service } = makeService(state);
    const app = express();
    registerAssetsRoutes(app, {
      cradle: { fileAssetsService: service },
    } as any);
    server = await listen(app);

    const { port } = server.address() as AddressInfo;
    const response = await fetch(`http://127.0.0.1:${port}/assets/file-1`, {
      headers: { Range: 'bytes=99-100' },
    });

    expect(response.status).toBe(416);
    expect(response.headers.get('content-range')).toBe('bytes */10');
  });

  it('invalidates cached file metadata when that file is reloaded', async () => {
    const state = { files: [makeFile()], permissions: [] };
    const { service, eventEmitter, queryBuilderService } = makeService(state);
    const req = { params: { id: 'file-1' }, query: {} };

    await service.streamFile(req, makeResponse());
    await service.streamFile(req, makeResponse());
    expect(countFinds(queryBuilderService, 'enfyra_file')).toBe(1);

    await eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, {
      table: 'enfyra_file',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: ['file-1'],
    });

    await service.streamFile(req, makeResponse());
    expect(countFinds(queryBuilderService, 'enfyra_file')).toBe(2);
  });

  it('caches private file permissions by file and invalidates by changed permission id', async () => {
    const state = {
      files: [makeFile({ isPublic: false })],
      permissions: [makePermission()],
    };
    const { service, eventEmitter, queryBuilderService } = makeService(state);
    const req = {
      params: { id: 'file-1' },
      query: {},
      user: { id: 'user-1', role: { id: 'role-1' } },
    };

    await service.streamFile(req, makeResponse());
    await service.streamFile(req, makeResponse());
    expect(countFinds(queryBuilderService, 'enfyra_file')).toBe(1);
    expect(countFinds(queryBuilderService, 'enfyra_file_permission')).toBe(
      1,
    );

    await eventEmitter.emitAsync(CACHE_EVENTS.INVALIDATE, {
      table: 'enfyra_file_permission',
      action: 'reload',
      timestamp: Date.now(),
      scope: 'partial',
      ids: ['perm-1'],
    });

    await service.streamFile(req, makeResponse());
    expect(countFinds(queryBuilderService, 'enfyra_file')).toBe(1);
    expect(countFinds(queryBuilderService, 'enfyra_file_permission')).toBe(
      3,
    );
  });

  it('allows root admin to stream private files without file permissions', async () => {
    const state = {
      files: [makeFile({ isPublic: false })],
      permissions: [],
    };
    const { service, queryBuilderService } = makeService(state);
    const req = {
      params: { id: 'file-1' },
      query: {},
      user: { _id: 'user-1', isRootAdmin: true },
    };

    await service.streamFile(req, makeResponse());

    expect(countFinds(queryBuilderService, 'enfyra_file')).toBe(1);
    expect(countFinds(queryBuilderService, 'enfyra_file_permission')).toBe(
      0,
    );
  });
});
