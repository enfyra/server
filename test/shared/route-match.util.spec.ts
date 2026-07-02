import { describe, expect, it } from 'vitest';
import {
  matchRouteInRoutes,
  matchRouteIndexEntry,
} from '../../src/shared/utils/route-match.util';

describe('route-match util', () => {
  it('matches static routes before parameter routes with the same method', () => {
    const match = matchRouteInRoutes(
      [
        { path: '/posts/:id', availableMethods: [{ name: 'GET' }] },
        { path: '/posts/archive', availableMethods: [{ name: 'GET' }] },
      ],
      'GET',
      '/posts/archive',
    );

    expect(match?.route.path).toBe('/posts/archive');
    expect(match?.params).toEqual({});
  });

  it('adds implicit id paths for PATCH and DELETE table routes', () => {
    const match = matchRouteInRoutes(
      [{ path: '/posts', availableMethods: [{ name: 'PATCH' }] }],
      'PATCH',
      '/posts/42',
    );

    expect(match?.route.path).toBe('/posts');
    expect(match?.params).toEqual({ id: '42' });
  });

  it('uses index order as the tie breaker for Redis route index entries', () => {
    const match = matchRouteIndexEntry(
      [
        { path: '/posts/:id', methods: ['GET'], order: 2 },
        { path: '/posts/:slug', methods: ['GET'], order: 1 },
      ],
      'GET',
      '/posts/hello',
    );

    expect(match?.entry.path).toBe('/posts/:slug');
    expect(match?.params).toEqual({ slug: 'hello' });
  });
});
