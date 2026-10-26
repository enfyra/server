import { PassThrough } from 'node:stream';
import { once } from 'node:events';
import { expect, it, vi } from 'vitest';
import { StreamHelper } from '../../src/modules/file-management/utils/stream.helper';
import { FileAssetsService } from '../../src/modules/file-management/services/file-assets.service';

function response() {
  const res = new PassThrough() as any;
  res.headers = {};
  res.setHeader = (name: string, value: unknown) => { res.headers[name.toLowerCase()] = value; };
  res.getHeader = (name: string) => res.headers[name.toLowerCase()];
  res.status = () => res;
  res.json = (body: unknown) => { res.end(JSON.stringify(body)); return res; };
  res.resume();
  return res;
}

it('destroys the cloud source when the response closes early', async () => {
  const source = new PassThrough();
  const res = response();
  try {
    await new StreamHelper().streamCloudFile(source, res, 'file.txt', 'text/plain');
    const closed = once(res, 'close');
    res.destroy();
    await closed;
    expect(source.destroyed).toBe(true);
  } finally { source.destroy(); res.destroy(); }
});

it.each([false, true])('classifies delivery cache policy at the authorized asset boundary (public=%s)', async isPublic => {
  const service = new FileAssetsService({ queryBuilderService: {} as any, fileManagementService: {} as any, storageFactoryService: {} as any });
  (service as any).fileAssetAccessService.resolveAuthorizedFile = async () => ({ id: 1, filename: 'image.png', mimetype: 'image/png', type: 'image', location: 'uploads/image.png', isPublic, storageConfig: { id: 'cloud', type: 'Amazon S3' } });
  (service as any).fileAssetAccessService.getFileSize = () => undefined;
  const processImage = vi.spyOn((service as any).imageAssetProcessorService, 'processImageWithQuery').mockImplementation(async (...args: any[]) => { args[2].end(); });
  const res = response();
  await service.streamFile({ params: { id: '1' }, headers: {}, query: { format: 'webp', cache: '31536000' } }, res);
  expect(processImage).toHaveBeenCalled();
  if (!isPublic) expect(res.headers['cache-control']).toBe('private, no-store');
  expect(processImage.mock.calls[0][5]).toBe(isPublic);
  res.destroy();
});

it('tears down the complete transformed-image chain on response close', async () => {
  const source = new PassThrough();
  const transform = new PassThrough();
  const res = response();
  const helper = new StreamHelper();
  try {
    helper.handleStreamError(source, res, 'source failed');
    helper.setupImageStream(source.pipe(transform) as any, res, false);
    const closed = once(res, 'close');
    res.destroy();
    await closed;
    expect(source.destroyed).toBe(true);
    expect(transform.destroyed).toBe(true);
  } finally { source.destroy(); transform.destroy(); res.destroy(); }
});
