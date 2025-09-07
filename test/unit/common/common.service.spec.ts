import { Test, TestingModule } from '@nestjs/testing';
import { CommonService } from '../../../src/shared/common/services/common.service';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';

describe('CommonService', () => {
  let service: CommonService;
  let dataSourceService: jest.Mocked<DataSourceService>;

  beforeEach(async () => {
    const mockDataSourceService = {
      loadDynamicEntities: jest.fn(),
      entityClassMap: new Map(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        CommonService,
        { provide: DataSourceService, useValue: mockDataSourceService },
      ],
    }).compile();

    service = module.get<CommonService>(CommonService);
    dataSourceService = module.get(DataSourceService);
  });

  describe('isRouteMatched', () => {
    it('should match exact routes', () => {
      const result = service.isRouteMatched({
        routePath: '/users',
        reqPath: '/users',
      });

      expect(result).toEqual({ params: {} });
    });

    it('should match routes with parameters', () => {
      const result = service.isRouteMatched({
        routePath: '/users/:id',
        reqPath: '/users/123',
      });

      expect(result).toEqual({ params: { id: '123' } });
    });

    it('should match routes with multiple parameters', () => {
      const result = service.isRouteMatched({
        routePath: '/users/:userId/posts/:postId',
        reqPath: '/users/123/posts/456',
      });

      expect(result).toEqual({
        params: { userId: '123', postId: '456' },
      });
    });

    it('should return null for non-matching routes', () => {
      const result = service.isRouteMatched({
        routePath: '/users',
        reqPath: '/posts',
      });

      expect(result).toBeNull();
    });

    it('should handle trailing slashes', () => {
      const result1 = service.isRouteMatched({
        routePath: '/users/',
        reqPath: '/users',
      });

      const result2 = service.isRouteMatched({
        routePath: '/users',
        reqPath: '/users/',
      });

      expect(result1).toEqual({ params: {} });
      expect(result2).toEqual({ params: {} });
    });

    it('should handle query parameters in request path', () => {
      const result = service.isRouteMatched({
        routePath: '/users/:id',
        reqPath: '/users/123?include=posts',
      });

      expect(result).toEqual({ params: { id: '123' } });
    });

    it('should handle wildcard routes', () => {
      const result = service.isRouteMatched({
        routePath: '/api/*',
        reqPath: '/api/v1/users',
      });

      expect(result).toBeTruthy();
    });
  });

  describe('parseRouteParams', () => {
    it('should extract parameters from route paths', () => {
      const params = service.parseRouteParams('/users/:id/posts/:postId');
      expect(params).toEqual(['id', 'postId']);
    });

    it('should return empty array for routes without parameters', () => {
      const params = service.parseRouteParams('/users');
      expect(params).toEqual([]);
    });

    it('should handle optional parameters', () => {
      const params = service.parseRouteParams('/users/:id?');
      expect(params).toEqual(['id']);
    });
  });

  describe('normalizeRoutePath', () => {
    it('should normalize route paths consistently', () => {
      expect(service.normalizeRoutePath('/users')).toBe('/users');
      expect(service.normalizeRoutePath('users')).toBe('/users');
      expect(service.normalizeRoutePath('/users/')).toBe('/users');
      expect(service.normalizeRoutePath('')).toBe('/');
    });
  });

  describe('validateIdentifier', () => {
    it('should validate safe SQL identifiers', () => {
      expect(service.validateIdentifier('users')).toBe(true);
      expect(service.validateIdentifier('user_table')).toBe(true);
      expect(service.validateIdentifier('table123')).toBe(true);
    });

    it('should reject unsafe identifiers', () => {
      expect(service.validateIdentifier('user-table')).toBe(false);
      expect(service.validateIdentifier('123table')).toBe(false);
      expect(service.validateIdentifier('table name')).toBe(false);
      expect(service.validateIdentifier('table;drop')).toBe(false);
    });

    it('should handle reserved keywords', () => {
      expect(service.validateIdentifier('select')).toBe(false);
      expect(service.validateIdentifier('table')).toBe(false);
      expect(service.validateIdentifier('order')).toBe(false);
    });
  });

  describe('sanitizeInput', () => {
    it('should sanitize dangerous input', () => {
      const dangerous = "'; DROP TABLE users; --";
      const safe = service.sanitizeInput(dangerous);
      expect(safe).not.toContain('DROP TABLE');
      expect(safe).not.toContain(';');
    });

    it('should preserve safe input', () => {
      const safe = 'John Doe';
      expect(service.sanitizeInput(safe)).toBe(safe);
    });

    it('should handle special characters safely', () => {
      const input = 'user@domain.com';
      const result = service.sanitizeInput(input);
      expect(result).toBeDefined();
    });
  });

  describe('Error Handling', () => {
    it('should handle malformed route paths gracefully', () => {
      expect(() => {
        service.isRouteMatched({
          routePath: '/users/[invalid',
          reqPath: '/users/123',
        });
      }).not.toThrow();
    });

    it('should handle null/undefined inputs', () => {
      expect(
        service.isRouteMatched({
          routePath: null as any,
          reqPath: '/users',
        }),
      ).toBeNull();

      expect(
        service.isRouteMatched({
          routePath: '/users',
          reqPath: null as any,
        }),
      ).toBeNull();
    });
  });

  describe('Performance', () => {
    it('should handle route matching efficiently', () => {
      const startTime = Date.now();

      for (let i = 0; i < 1000; i++) {
        service.isRouteMatched({
          routePath: '/users/:id',
          reqPath: `/users/${i}`,
        });
      }

      const duration = Date.now() - startTime;
      expect(duration).toBeLessThan(100); // Should complete in under 100ms
    });
  });
});
