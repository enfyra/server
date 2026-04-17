import { BaseCacheService } from '../../src/infrastructure/cache/services/base-cache.service';
import { CACHE_IDENTIFIERS } from '../../src/shared/utils/cache-events.constants';

class TestCache extends BaseCacheService<Map<string, string>> {
  loadCalls = 0;
  partialCalls = 0;
  failPartial = false;
  lastPayload: { ids?: string[] } | null = null;

  constructor() {
    super({
      cacheIdentifier: CACHE_IDENTIFIERS.SETTING,
      colorCode: '',
      cacheName: 'TestCache',
    });
  }

  supportsPartialReload(): boolean {
    return true;
  }

  protected async loadFromDb(): Promise<any> {
    this.loadCalls++;
    return { k: 'v' };
  }

  protected transformData(raw: any): Map<string, string> {
    return new Map(Object.entries(raw));
  }

  protected async applyPartialUpdate(payload: {
    ids?: string[];
  }): Promise<void> {
    this.partialCalls++;
    this.lastPayload = payload;
    if (this.failPartial) {
      throw new Error('partial failed');
    }
  }
}

describe('BaseCacheService', () => {
  it('deduplicates concurrent reload() to one loadFromDb', async () => {
    const c = new TestCache();
    const a = c.reload(false);
    const b = c.reload(false);
    await Promise.all([a, b]);
    expect(c.loadCalls).toBe(1);
  });

  it('partialReload on failure falls back to full reload', async () => {
    const c = new TestCache();
    c.failPartial = true;
    await c.reload(false);
    expect(c.loadCalls).toBe(1);
    await c.partialReload({ ids: ['x'] }, false);
    expect(c.partialCalls).toBe(1);
    expect(c.loadCalls).toBe(2);
  });

  it('partialReload after successful reload does not trigger another full load', async () => {
    const c = new TestCache();
    await c.reload(false);
    expect(c.loadCalls).toBe(1);
    await c.partialReload({ ids: ['a', 'b'] }, false);
    expect(c.partialCalls).toBe(1);
    expect(c.loadCalls).toBe(1);
    expect(c.lastPayload).toEqual({ ids: ['a', 'b'] });
  });
});
