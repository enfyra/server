import { describe, expect, it, vi } from 'vitest';
import { DynamicWebSocketGateway } from '../../src/modules/websocket';

describe('runtime metrics socket requests', () => {
  it('acknowledges a failed snapshot read without broadcasting it', async () => {
    const handlers = new Map<string, (...args: any[]) => Promise<void>>();
    const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
    gateway.lazyRef = { runtimeMonitorService: { getClusterSnapshot: vi.fn().mockRejectedValue(new Error('Telemetry unavailable')) } };
    gateway.redisAdminService = { getOverview: vi.fn().mockResolvedValue({}) };
    vi.spyOn(gateway, 'loadSocketUser').mockResolvedValue({ isRootAdmin: true });
    const socket = { data: {}, join: vi.fn(), emit: vi.fn(), on: vi.fn((event, handler) => handlers.set(event, handler)) };
    await gateway.setupAdminSocket(socket);
    const ack = vi.fn();
    await handlers.get('$system:runtime:metrics:get')?.({}, ack);
    expect(ack).toHaveBeenCalledWith({ success: false, error: { message: 'Telemetry unavailable' } });
    expect(socket.emit).not.toHaveBeenCalledWith('$system:runtime:metrics', expect.anything());
  });

  it.each([true, false, undefined])('limits cluster snapshots to root admins (%s)', async (isRootAdmin) => {
    const handlers = new Map<string, (...args: any[]) => Promise<void>>();
    const snapshot = { intervalMs: 2000, instances: [{ instance: { id: 'a' } }] };
    const getClusterSnapshot = vi.fn().mockResolvedValue(snapshot);
    const gateway = Object.create(DynamicWebSocketGateway.prototype) as any;
    gateway.lazyRef = { runtimeMonitorService: { getClusterSnapshot } };
    gateway.redisAdminService = { getOverview: vi.fn().mockResolvedValue({}) };
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
        success: false, error: { message: 'Root admin access required' },
      });
    }
    expect(socket.emit).not.toHaveBeenCalledWith('$system:runtime:metrics', expect.anything());
  });
});
