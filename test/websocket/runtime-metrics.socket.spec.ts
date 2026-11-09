import { describe, expect, it, vi } from 'vitest';
import { DynamicWebSocketGateway } from '../../src/modules/websocket';

describe('runtime metrics socket requests', () => {
  it('acknowledges a failed snapshot read without broadcasting it', async () => {
    const handlers = new Map<string, (...args: any[]) => Promise<void>>();
    const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
    gateway.lazyRef = {
      runtimeMonitorService: {
        getClusterSnapshot: vi
          .fn()
          .mockRejectedValue(new Error('Telemetry unavailable')),
      },
    };
    gateway.redisAdminService = { getOverview: vi.fn().mockResolvedValue({}) };
    vi.spyOn(gateway, 'loadSocketUser').mockResolvedValue({
      isRootAdmin: true,
    });
    const socket = {
      data: {},
      join: vi.fn(),
      emit: vi.fn(),
      on: vi.fn((event, handler) => handlers.set(event, handler)),
    };
    await gateway.setupAdminSocket(socket);
    const ack = vi.fn();
    await handlers.get('$system:runtime:metrics:get')?.({}, ack);
    expect(ack).toHaveBeenCalledWith({
      success: false,
      error: { message: 'Telemetry unavailable' },
    });
    expect(socket.emit).not.toHaveBeenCalledWith(
      '$system:runtime:metrics',
      expect.anything(),
    );
  });

  it('does not collect Redis overview on an ordinary root admin connection', async () => {
    const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
    gateway.redisAdminService = { getOverview: vi.fn().mockResolvedValue({}) };
    vi.spyOn(gateway, 'loadSocketUser').mockResolvedValue({
      isRootAdmin: true,
    });
    await gateway.setupAdminSocket({
      data: {},
      join: vi.fn(),
      emit: vi.fn(),
      on: vi.fn(),
    });
    expect(gateway.redisAdminService.getOverview).not.toHaveBeenCalled();
  });

  it.each([true, false, undefined])(
    'requires root access for runtime subscription (%s)',
    async (isRootAdmin) => {
      const handlers = new Map<string, (...args: any[]) => Promise<void>>();
      const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
      gateway.redisAdminService = {
        getOverview: vi.fn().mockResolvedValue({}),
      };
      vi.spyOn(gateway, 'loadSocketUser').mockResolvedValue(
        isRootAdmin === undefined ? null : { isRootAdmin },
      );
      const socket = {
        data: {},
        join: vi.fn(),
        leave: vi.fn(),
        emit: vi.fn(),
        on: vi.fn((name, handler) => handlers.set(name, handler)),
      };
      await gateway.setupAdminSocket(socket);
      const ack = vi.fn();
      const subscribe = handlers.get('$system:runtime:subscribe');
      expect(subscribe).toBeTypeOf('function');
      await subscribe?.({}, ack);
      if (isRootAdmin) {
        expect(socket.join).toHaveBeenCalledWith('$system:runtime-monitor');
        expect(ack).toHaveBeenCalledWith({ success: true });
        await handlers.get('$system:runtime:unsubscribe')?.({}, vi.fn());
        expect(socket.leave).toHaveBeenCalledWith('$system:runtime-monitor');
      } else {
        expect(socket.join).not.toHaveBeenCalledWith('$system:runtime-monitor');
        expect(ack).toHaveBeenCalledWith({
          success: false,
          error: { message: 'Root admin access required' },
        });
      }
      expect(gateway.redisAdminService.getOverview).not.toHaveBeenCalled();
    },
  );

  it('does not rejoin the monitor room when unsubscribe wins an in-flight subscribe', async () => {
    const handlers = new Map<string, (...args: any[]) => Promise<void>>();
    const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
    gateway.redisAdminService = { getOverview: vi.fn().mockResolvedValue({}) };
    vi.spyOn(gateway, 'loadSocketUser').mockResolvedValue({
      isRootAdmin: true,
    });
    const socket = {
      data: {},
      join: vi.fn(),
      leave: vi.fn(),
      emit: vi.fn(),
      on: vi.fn((name, handler) => handlers.set(name, handler)),
    };
    await gateway.setupAdminSocket(socket);
    let resolveUser: (value: unknown) => void;
    gateway.loadSocketUser.mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          resolveUser = resolve;
        }),
    );
    const subscribing = handlers.get('$system:runtime:subscribe')?.(
      {},
      vi.fn(),
    );
    await handlers.get('$system:runtime:unsubscribe')?.({}, vi.fn());
    resolveUser!({ isRootAdmin: true });
    await subscribing;
    expect(socket.join).not.toHaveBeenCalledWith('$system:runtime-monitor');
  });

  it('keeps monitor fanout local to the subscriber-owning replica', () => {
    const emit = vi.fn();
    const to = vi.fn(() => ({ emit }));
    const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
    gateway.server = {
      of: vi.fn(() => ({
        adapter: {
          rooms: new Map([['$system:runtime-monitor', new Set(['socket'])]]),
        },
        local: { to },
      })),
    };
    expect(
      gateway.localNamespaceRoomSize(
        '/enfyra-admin',
        '$system:runtime-monitor',
      ),
    ).toBe(1);
    expect(gateway.localNamespaceRoomSize('/enfyra-admin', 'absent')).toBe(0);
    gateway.emitToLocalNamespaceRoom(
      '/enfyra-admin',
      '$system:runtime-monitor',
      '$system:redis:overview',
      {},
    );
    expect(to).toHaveBeenCalledWith('$system:runtime-monitor');
    expect(emit).toHaveBeenCalledWith('$system:redis:overview', {});
  });

  it.each([true, false, undefined])(
    'limits cluster snapshots to root admins (%s)',
    async (isRootAdmin) => {
      const handlers = new Map<string, (...args: any[]) => Promise<void>>();
      const snapshot = {
        intervalMs: 2000,
        instances: [{ instance: { id: 'a' } }],
      };
      const getClusterSnapshot = vi.fn().mockResolvedValue(snapshot);
      const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
      gateway.lazyRef = { runtimeMonitorService: { getClusterSnapshot } };
      gateway.redisAdminService = {
        getOverview: vi.fn().mockResolvedValue({}),
      };
      vi.spyOn(gateway, 'loadSocketUser').mockResolvedValue(
        isRootAdmin === undefined ? null : { id: 'user', isRootAdmin },
      );
      const socket = {
        data: {},
        join: vi.fn(),
        emit: vi.fn(),
        on: vi.fn((event, handler) => handlers.set(event, handler)),
      };
      await gateway.setupAdminSocket(socket);
      const handler = handlers.get('$system:runtime:metrics:get');
      expect(handler).toBeTypeOf('function');
      const ack = vi.fn();
      await handler?.({}, ack);

      if (isRootAdmin) {
        expect(getClusterSnapshot).toHaveBeenCalledOnce();
        expect(ack).toHaveBeenCalledWith({ success: true, data: snapshot });
      } else {
        expect(getClusterSnapshot).not.toHaveBeenCalled();
        expect(ack).toHaveBeenCalledWith({
          success: false,
          error: { message: 'Root admin access required' },
        });
      }
      expect(socket.emit).not.toHaveBeenCalledWith(
        '$system:runtime:metrics',
        expect.anything(),
      );
    },
  );
});
