import { EnfyraRouteEngine } from '../../src/shared/utils/enfyra-route-engine';

describe('EnfyraRouteEngine', () => {
  it('matches static segments', () => {
    const engine = new EnfyraRouteEngine(false);
    const handler = { id: 1 };
    engine.insert('GET', '/api/items', handler);
    const hit = engine.find('GET', '/api/items');
    expect(hit?.route).toBe(handler);
    expect(hit?.params).toEqual({});
  });

  it('captures named params', () => {
    const engine = new EnfyraRouteEngine(false);
    const handler = { id: 2 };
    engine.insert('GET', '/api/items/:id', handler);
    const hit = engine.find('GET', '/api/items/42');
    expect(hit?.route).toBe(handler);
    expect(hit?.params).toEqual({ id: '42' });
  });

  it('decodes param segments', () => {
    const engine = new EnfyraRouteEngine(false);
    engine.insert('GET', '/x/:slug', { id: 3 });
    const hit = engine.find('GET', '/x/hello%20world');
    expect(hit?.params).toEqual({ slug: 'hello world' });
  });

  it('wildcard captures splat', () => {
    const engine = new EnfyraRouteEngine(false);
    const handler = { id: 4 };
    engine.insert('GET', '/files/*', handler);
    const hit = engine.find('GET', '/files/a/b/c');
    expect(hit?.route).toBe(handler);
    expect(hit?.params).toEqual({ splat: 'a/b/c' });
  });

  it('returns null for unknown method', () => {
    const engine = new EnfyraRouteEngine(false);
    engine.insert('GET', '/x', {});
    expect(engine.find('POST', '/x')).toBeNull();
  });

  it('normalizes path without leading slash and trailing slash', () => {
    const engine = new EnfyraRouteEngine(false);
    const h = { id: 5 };
    engine.insert('GET', 'api/test/', h);
    expect(engine.find('GET', 'api/test')?.route).toBe(h);
  });

  it('falls through failed static and parameter branches without leaking captures', () => {
    const engine = new EnfyraRouteEngine();
    engine.insert('GET', '/files/fixed/end', { id: 'static' });
    engine.insert('GET', '/files/:name/end', { id: 'param' });
    engine.insert('GET', '/files/*', { id: 'wildcard' });

    expect(engine.find('GET', '/files/fixed/other')).toEqual({
      route: { id: 'wildcard' },
      params: { splat: 'fixed/other' },
    });
    expect(engine.find('GET', '/files/fixed/end')).toEqual({
      route: { id: 'static' },
      params: {},
    });
    expect(engine.find('GET', '/files/other/end')).toEqual({
      route: { id: 'param' },
      params: { name: 'other' },
    });
  });

  it('preserves empty parameter names and malformed URI fallback', () => {
    const engine = new EnfyraRouteEngine();
    engine.insert('GET', '/items/:', { id: 'empty-name' });

    expect(engine.find('GET', '/items/%E0%A4%A')).toEqual({
      route: { id: 'empty-name' },
      params: { '': '%E0%A4%A' },
    });
  });

  it('getStats reflects inserts', () => {
    const engine = new EnfyraRouteEngine(false);
    engine.insert('GET', '/a', {});
    engine.insert('POST', '/b', {});
    const s = engine.getStats();
    expect(s.totalRoutes).toBe(2);
    expect(s.methods.sort()).toEqual(['GET', 'POST']);
  });
});
