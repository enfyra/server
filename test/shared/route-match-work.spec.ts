import { describe, expect, it, vi } from 'vitest';
import {
  matchRouteInRoutes,
  matchRouteIndexEntry,
} from '../../src/shared/utils/route-match.util';

describe('route matching request work', () => {
  it.each(['routes', 'index'])(
    'splits the request path once across %s candidates',
    (mode) => {
      const path = '/target/AbC';
      const split = vi.spyOn(String.prototype, 'split');
      let result;
      try {
        const entries = Array.from({ length: 100 }, (_, i) => ({
          path: i === 99 ? '/target/:id' : `/other${i}/:id`,
          methods: ['GET'],
        }));
        result =
          mode === 'routes'
            ? matchRouteInRoutes(entries, 'GET', path)
            : matchRouteIndexEntry(entries, 'GET', path);
        expect(
          split.mock.contexts.filter((value) => String(value) === path),
        ).toHaveLength(1);
      } finally {
        split.mockRestore();
      }
      expect(result?.params).toEqual({ id: 'AbC' });
    },
  );

  it('observes method, enablement and path edits on the next lookup without retaining a route decision', () => {
    const config = { available: true, method: 'GET' };
    const route = {
      path: '/items/:id',
      isEnabled: true,
      methodConfigs: [config],
    };
    const routes = [route];
    expect(matchRouteInRoutes(routes, 'GET', '/items/first')?.params).toEqual({
      id: 'first',
    });
    config.available = false;
    expect(matchRouteInRoutes(routes, 'GET', '/items/first')).toBeNull();
    config.available = true;
    route.isEnabled = false;
    expect(matchRouteInRoutes(routes, 'GET', '/items/first')).toBeNull();
    route.isEnabled = true;
    route.path = '/changed/:id';
    expect(matchRouteInRoutes(routes, 'GET', '/items/first')).toBeNull();
    expect(
      matchRouteInRoutes(routes, 'GET', '/changed/second')?.params,
    ).toEqual({ id: 'second' });
  });

  it('returns independent params and keeps first-route and explicit index tie ordering', () => {
    const routes = [
      { path: '/items/:id', methods: ['GET'], order: 2 },
      { path: '/items/:slug', methods: ['GET'], order: 1 },
    ];
    const first = matchRouteInRoutes(routes, 'GET', '/items/AbC')!;
    first.params.id = 'mutated';
    expect(matchRouteInRoutes(routes, 'GET', '/items/AbC')?.params).toEqual({
      id: 'AbC',
    });
    expect(first.route).toBe(routes[0]);
    expect(matchRouteIndexEntry(routes, 'GET', '/items/AbC')?.entry).toBe(
      routes[1],
    );
  });
});
