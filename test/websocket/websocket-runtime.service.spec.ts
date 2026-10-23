import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { WebsocketRuntimeService } from '../../src/modules/websocket';
import {
  CACHE_EVENTS,
  CACHE_IDENTIFIERS,
} from '../../src/shared/utils/cache-events.constants';

function createRuntime() {
  const eventEmitter = new EventEmitter2();
  const dynamicWebSocketGateway = {
    refreshGateways: vi.fn(async () => undefined),
  };
  const service = new WebsocketRuntimeService({
    eventEmitter,
    dynamicWebSocketGateway: dynamicWebSocketGateway as any,
  });

  return { eventEmitter, dynamicWebSocketGateway, service };
}

describe('WebsocketRuntimeService', () => {
  it('refreshes process-local gateways when websocket cache publishes', async () => {
    const { eventEmitter, dynamicWebSocketGateway, service } = createRuntime();

    service.init();
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.WEBSOCKET,
    });

    await vi.waitFor(() => {
      expect(dynamicWebSocketGateway.refreshGateways).toHaveBeenCalledTimes(1);
    });
    expect(service.getStatus().lastRefresh).toEqual(
      expect.objectContaining({ status: 'ok' }),
    );
  });

  it('runs a trailing refresh for activations received during an in-flight refresh', async () => {
    const { dynamicWebSocketGateway, service } = createRuntime();
    let entered!: () => void;
    let release!: () => void;
    const started = new Promise<void>((resolve) => { entered = resolve; });
    const blocked = new Promise<void>((resolve) => { release = resolve; });
    dynamicWebSocketGateway.refreshGateways.mockImplementationOnce(async () => { entered(); await blocked; });
    const first = service.refreshGateways();
    await started;
    const second = service.refreshGateways();
    release();
    await Promise.all([first, second]);
    expect(dynamicWebSocketGateway.refreshGateways).toHaveBeenCalledTimes(2);
    expect(service.getStatus().lastRefresh?.status).toBe('ok');
  });

  it('marks websocket runtime refresh as degraded when gateway refresh fails', async () => {
    const { eventEmitter, dynamicWebSocketGateway, service } = createRuntime();
    dynamicWebSocketGateway.refreshGateways.mockRejectedValueOnce(
      new Error('socket refresh failed'),
    );

    service.init();
    eventEmitter.emit(CACHE_EVENTS.RUNTIME_CACHE_ACTIVATED, {
      identifier: CACHE_IDENTIFIERS.WEBSOCKET,
    });

    await vi.waitFor(() => {
      expect(service.getStatus().lastRefresh).toEqual(
        expect.objectContaining({
          status: 'degraded',
          error: 'socket refresh failed',
        }),
      );
    });
  });
});
