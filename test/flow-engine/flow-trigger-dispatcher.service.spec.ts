import { EventEmitter2 } from 'eventemitter2';
import { describe, expect, it, vi } from 'vitest';
import { FlowTriggerDispatcherService } from '../../src/modules/flow/services/flow-trigger-dispatcher.service';
import { CACHE_IDENTIFIERS, DATA_EVENTS } from '../../src/shared/utils/cache-events.constants';
import type { FlowDefinition } from '../../src/shared/types/flow.types';

function createDispatcher() {
  const eventEmitter = new EventEmitter2();
  const trigger = vi.fn(async () => undefined);
  const flows: FlowDefinition[] = [{
    id: 1,
    name: 'Route flow',
    isEnabled: true,
    steps: [],
    triggers: [
      { id: 10, type: 'webhook', isEnabled: true, routePath: '/orders', config: { method: 'POST' } },
      { id: 11, type: 'webhook', isEnabled: true, routePath: '/orders', config: {} },
    ],
  }];
  const service = new FlowTriggerDispatcherService({
    eventEmitter,
    runtimeRegistryService: { requireActiveData: (identifier: string) => identifier === CACHE_IDENTIFIERS.FLOW ? flows : [] } as any,
    flowService: { trigger } as any,
  });
  service.init();
  return { eventEmitter, trigger };
}

describe('FlowTriggerDispatcherService webhook method targeting', () => {
  it('runs method-specific triggers only for their method and preserves legacy route-wide triggers', async () => {
    const { eventEmitter, trigger } = createDispatcher();
    eventEmitter.emit(DATA_EVENTS.ROUTE_EXECUTED, { routePath: '/orders', method: 'GET', userId: 7, result: {} });
    await vi.waitFor(() => expect(trigger).toHaveBeenCalledTimes(1));
    expect(trigger.mock.calls[0]?.[1]).toMatchObject({ triggerId: 11, method: 'GET' });

    eventEmitter.emit(DATA_EVENTS.ROUTE_EXECUTED, { routePath: '/orders', method: 'POST', userId: 7, result: {} });
    await vi.waitFor(() => expect(trigger).toHaveBeenCalledTimes(2));
    expect(trigger.mock.calls[1]?.[1]).toMatchObject({ triggerId: 10, method: 'POST' });
  });
});
