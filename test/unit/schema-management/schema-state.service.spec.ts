import { Test, TestingModule } from '@nestjs/testing';
import { SchemaStateService } from '../../../src/modules/schema-management/services/schema-state.service';

describe('SchemaStateService', () => {
  let service: SchemaStateService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [SchemaStateService],
    }).compile();

    service = module.get<SchemaStateService>(SchemaStateService);

    // Suppress console logs during tests
    jest.spyOn(console, 'log').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('getVersion', () => {
    it('should return current schema version', () => {
      service.setVersion(5);

      const result = service.getVersion();

      expect(result).toBe(5);
    });

    it('should return undefined when no version is set', () => {
      const result = service.getVersion();

      expect(result).toBeUndefined();
    });
  });

  describe('setVersion', () => {
    it('should set schema version', () => {
      service.setVersion(10);

      expect(service.getVersion()).toBe(10);
    });

    it('should update existing version', () => {
      service.setVersion(5);
      service.setVersion(15);

      expect(service.getVersion()).toBe(15);
    });

    it('should handle zero version', () => {
      service.setVersion(0);

      expect(service.getVersion()).toBe(0);
    });

    it('should handle negative version', () => {
      service.setVersion(-1);

      expect(service.getVersion()).toBe(-1);
    });
  });

  describe('Edge Cases', () => {
    it('should handle floating point versions', () => {
      service.setVersion(1.5);

      expect(service.getVersion()).toBe(1.5);
    });

    it('should handle very large version numbers', () => {
      const largeVersion = Number.MAX_SAFE_INTEGER;
      service.setVersion(largeVersion);

      expect(service.getVersion()).toBe(largeVersion);
    });

    it('should persist version across multiple operations', () => {
      service.setVersion(42);

      // Multiple get calls should return same value
      expect(service.getVersion()).toBe(42);
      expect(service.getVersion()).toBe(42);
      expect(service.getVersion()).toBe(42);
    });
  });
});
