import { describe, expect, it, vi } from 'vitest';
import { EventEmitter2 } from 'eventemitter2';
import { WebsocketCacheBuilder } from '../../src/engines/cache';

function makeQb(gateways: any[], events: any[]) {
  return {
    isMongoDb: vi.fn(() => false),
    find: vi.fn(async (args: any) => {
      const table = args?.table;
      const ids =
        args?.filter?.id?._in ??
        args?.filter?._and?.find((item: any) => item?.id?._in)?.id?._in;
      if (table === 'enfyra_websocket') {
        let rows = gateways.filter((gateway) => gateway.isEnabled !== false);
        if (ids) {
          const idSet = new Set(ids.map(String));
          rows = rows.filter((gateway) => idSet.has(String(gateway.id)));
        }
        return { data: rows.map((row) => ({ ...row })) };
      }
      if (table === 'enfyra_websocket_event') {
        let rows = events;
        if (ids) {
          const idSet = new Set(ids.map(String));
          rows = rows.filter((event) => idSet.has(String(event.id)));
        }
        return { data: rows.map((row) => ({ ...row })) };
      }
      return { data: [] };
    }),
    update: vi.fn(async () => ({})),
  } as any;
}

function makeService(gateways: any[], events: any[]) {
  const qb = makeQb(gateways, events);
  const svc = new WebsocketCacheBuilder({
    queryBuilderService: qb,
    eventEmitter: new EventEmitter2(),
  });
  return { svc, qb };
}

describe('WebsocketCacheBuilder partial reload', () => {
  it('supports partial reload', () => {
    const { svc } = makeService([], []);
    expect(svc.supportsPartialReload()).toBe(true);
  });

  it('reloads only the gateway owning a changed event', async () => {
    const gateways = [
      {
        id: 1,
        path: '/chat',
        isEnabled: true,
        events: [{ id: 10, eventName: 'chat:message', gateway: { id: 1 } }],
      },
      {
        id: 2,
        path: '/admin',
        isEnabled: true,
        events: [{ id: 20, eventName: 'admin:ping', gateway: { id: 2 } }],
      },
    ];
    const events = [
      { id: 10, eventName: 'chat:message', gateway: { id: 1 } },
      { id: 20, eventName: 'admin:ping', gateway: { id: 2 } },
    ];
    const { svc, qb } = makeService(gateways, events);
    await svc.reload(false);

    gateways[0].events = [
      { id: 10, eventName: 'chat:new', gateway: { id: 1 } },
    ];
    events[0].eventName = 'chat:new';
    qb.find.mockClear();

    await svc.partialReload(
      {
        table: 'enfyra_websocket_event',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [10],
      },
      false,
    );

    const cached = await svc.getCacheAsync();
    expect(cached).toHaveLength(2);
    expect(
      cached.find((gateway) => gateway.id === 1)?.events[0].eventName,
    ).toBe('chat:new');
    expect(
      cached.find((gateway) => gateway.id === 2)?.events[0].eventName,
    ).toBe('admin:ping');
    expect(qb.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_websocket_event',
        filter: { id: { _in: [10] } },
      }),
    );
    expect(qb.find).toHaveBeenCalledWith(
      expect.objectContaining({
        table: 'enfyra_websocket',
        filter: {
          _and: [{ isEnabled: { _eq: true } }, { id: { _in: [1] } }],
        },
      }),
    );
  });

  it('removes a disabled gateway from cache', async () => {
    const gateways = [
      { id: 1, path: '/chat', isEnabled: true, events: [] },
      { id: 2, path: '/admin', isEnabled: true, events: [] },
    ];
    const { svc } = makeService(gateways, []);
    await svc.reload(false);

    gateways[0].isEnabled = false;

    await svc.partialReload(
      {
        table: 'enfyra_websocket',
        action: 'reload',
        timestamp: 0,
        scope: 'partial',
        ids: [1],
      },
      false,
    );

    const cached = await svc.getCacheAsync();
    expect(cached.map((gateway) => gateway.id)).toEqual([2]);
  });
});
