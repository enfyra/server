import { PassThrough, Readable } from 'stream';
import { describe, expect, it, vi } from 'vitest';

function makeQueryBuilder() {
  return {
    getPkField: vi.fn(() => 'id'),
    find: vi.fn(async () => ({
      data: [
        {
          id: 'file-1',
          filename: 'asset.txt',
          mimetype: 'text/plain',
          type: 'document',
          location: '/uploads/asset.txt',
          isPublished: true,
          storageConfig: null,
        },
      ],
    })),
  } as any;
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

describe('asset cache memory pressure', () => {
  it('does not add new asset metadata cache entries while RSS is above the configured threshold', async () => {
    vi.resetModules();
    vi.stubEnv('ASSET_CACHE_MEMORY_PRESSURE_RATIO', '0.000001');
    vi.stubEnv('ASSET_CACHE_MIN_FREE_MEMORY_MB', '0');

    const { FileAssetsService } =
      await import('../../src/modules/file-management');
    const queryBuilderService = makeQueryBuilder();
    const service = new FileAssetsService({
      queryBuilderService,
      fileManagementService: {
        getStorageConfigById: vi.fn(async () => ({
          id: 'local',
          type: 'Local Storage',
          isEnabled: true,
        })),
      } as any,
      storageFactoryService: {
        getStorageService: vi.fn(() => ({
          getStream: vi.fn(async () => Readable.from(['asset-body'])),
        })),
      } as any,
    });
    const req = { params: { id: 'file-1' }, query: {} };

    await service.streamFile(req, makeResponse());
    await service.streamFile(req, makeResponse());

    expect(queryBuilderService.find).toHaveBeenCalledTimes(2);
  });
});
