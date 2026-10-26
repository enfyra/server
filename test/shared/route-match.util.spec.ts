import { describe, expect, it } from 'vitest';
import {
  matchRouteInRoutes,
  matchRouteIndexEntry,
  matchRoutePattern,
} from '../../src/shared/utils/route-match.util';

describe('route-match util', () => {
  it('preserves wildcard, missing-segment and malformed-escape matching', () => {
    expect(matchRoutePattern('/files/*', '/files')).toEqual({ splat: '' });
    expect(matchRoutePattern('/files/*', '/files/a/b')).toEqual({ splat: 'a/b' });
    expect(matchRoutePattern('/files/:id', '/files')).toBeNull();
    expect(matchRoutePattern('/files/:id', '/files/%ZZ')).toEqual({ id: '%ZZ' });
    expect(matchRouteIndexEntry([], 'GET', '/files')).toBeNull();
  });

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

  it('matches only available route-method operations', () => {
    const match = matchRouteInRoutes(
      [
        {
          path: '/posts',
          methodConfigs: [
            { available: false, method: { name: 'GET' } },
            { available: true, method: { name: 'POST' } },
          ],
        },
      ],
      'GET',
      '/posts',
    );

    expect(match).toBeNull();

    const postMatch = matchRouteInRoutes(
      [
        {
          path: '/posts',
          methodConfigs: [
            { available: false, method: { name: 'GET' } },
            { available: true, method: { name: 'POST' } },
          ],
        },
      ],
      'POST',
      '/posts',
    );

    expect(postMatch?.route.path).toBe('/posts');
  });

  it('matches literal casing like Express while preserving parameter case', () => {
    const match = matchRouteInRoutes([{ path: '/Files/:id', methodConfigs: [{ available: true, method: 'GET' }] }], 'GET', '/FILES/AbC/');
    expect(match?.route.path).toBe('/Files/:id');
    expect(match?.params).toEqual({ id: 'AbC' });
  });

  it('does not bind a disabled route', () => {
    expect(matchRouteInRoutes([{ path: '/files', isEnabled: false, methodConfigs: [{ available: true, method: 'GET' }] }], 'GET', '/files')).toBeNull();
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
