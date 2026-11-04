import { describe, expect, it } from 'vitest';
import {
  resolveRouteMethodConfig,
  resolveRouteOperation,
} from '../../src/shared/utils/route-operation.util';

describe('route operation selection', () => {
  it('returns the first exact available config without copying its route', () => {
    const config = {
      available: true,
      method: { name: 'PATCH' },
      routePermissions: [{ id: 1 }],
    };
    const route = {
      path: '/items',
      methodConfigs: [
        { available: false, method: 'PATCH' },
        config,
        { available: true, method: 'PATCH' },
      ],
    };
    expect(resolveRouteMethodConfig(route, 'PATCH')).toBe(config);
    expect(resolveRouteOperation(route, 'PATCH')).toEqual({
      ...route,
      routeMethodConfig: config,
      routePermissions: config.routePermissions,
    });
    expect(resolveRouteOperation(route, 'PATCH')).not.toBe(route);
  });

  it.each([
    { isEnabled: false, methodConfigs: [{ available: true, method: 'GET' }] },
    { methodConfigs: [{ available: false, method: 'GET' }] },
    { methodConfigs: [{ method: 'GET' }] },
    { methodConfigs: [{ available: true, method: 'get' }] },
    { methodConfigs: [] },
    {},
  ])(
    'rejects a missing, disabled or noncanonical method binding %j',
    (route) => {
      expect(resolveRouteMethodConfig(route, 'GET')).toBeNull();
      expect(resolveRouteOperation(route, 'GET')).toBeNull();
    },
  );

  it('preserves an empty permissions view without changing the published route', () => {
    const route = { methodConfigs: [{ available: true, method: 'GET' }] };
    expect(resolveRouteOperation(route, 'GET')?.routePermissions).toEqual([]);
    expect(route).not.toHaveProperty('routePermissions');
  });
});
