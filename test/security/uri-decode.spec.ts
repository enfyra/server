import { EnfyraRouteEngine } from '../../src/shared/utils/enfyra-route-engine';

describe('EnfyraRouteEngine – URI decode safety', () => {
  let engine: EnfyraRouteEngine;

  beforeEach(() => {
    engine = new EnfyraRouteEngine();
    engine.insert('GET', '/items/:id', { name: 'item-route' });
    engine.insert('GET', '/users/:userId/posts/:postId', {
      name: 'user-posts',
    });
  });

  describe('malformed URI segments', () => {
    it('falls back to raw segment when decodeURIComponent throws (%ZZ)', () => {
      const result = engine.find('GET', '/items/%ZZ');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('%ZZ');
    });

    it('falls back to raw segment for truncated percent-encoding (%)', () => {
      const result = engine.find('GET', '/items/%');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('%');
    });

    it('falls back to raw segment for partial percent-encoding (%G0)', () => {
      const result = engine.find('GET', '/items/%G0');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('%G0');
    });

    it('does not throw on multiple malformed segments', () => {
      engine.insert('GET', '/a/:x/b/:y', { name: 'two-params' });
      expect(() => engine.find('GET', '/a/%ZZ/b/%XX')).not.toThrow();
      const result = engine.find('GET', '/a/%ZZ/b/%XX');
      expect(result).not.toBeNull();
      expect(result!.params.x).toBe('%ZZ');
      expect(result!.params.y).toBe('%XX');
    });
  });

  describe('valid URI encoding', () => {
    it('correctly decodes a valid percent-encoded segment', () => {
      const result = engine.find('GET', '/items/hello%20world');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('hello world');
    });

    it('decodes %2F in param (encoded slash)', () => {
      const result = engine.find('GET', '/items/foo%2Fbar');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('foo/bar');
    });

    it('decodes unicode characters', () => {
      const result = engine.find('GET', '/items/%E4%B8%AD%E6%96%87');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('中文');
    });

    it('handles plain alphanumeric segments normally', () => {
      const result = engine.find('GET', '/items/abc123');
      expect(result).not.toBeNull();
      expect(result!.params.id).toBe('abc123');
    });
  });

  describe('route matching correctness after fallback', () => {
    it('still matches route and returns handler with malformed segment', () => {
      const result = engine.find('GET', '/items/%ZZ');
      expect(result).not.toBeNull();
      expect(result!.route.name).toBe('item-route');
    });

    it('returns null for unregistered path even with malformed segment', () => {
      const result = engine.find('GET', '/nonexistent/%ZZ');
      expect(result).toBeNull();
    });

    it('matches multi-param route with first param malformed', () => {
      const result = engine.find('GET', '/users/%ZZ/posts/42');
      expect(result).not.toBeNull();
      expect(result!.params.userId).toBe('%ZZ');
      expect(result!.params.postId).toBe('42');
    });
  });

  describe('edge cases', () => {
    it('handles empty string segment gracefully', () => {
      const result = engine.find('GET', '/items/');
      expect(result).toBeNull();
    });

    it('handles path with no param segments', () => {
      engine.insert('GET', '/health', { name: 'health' });
      const result = engine.find('GET', '/health');
      expect(result).not.toBeNull();
      expect(result!.route.name).toBe('health');
    });
  });
});
