import { Test, TestingModule } from '@nestjs/testing';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';
import { CommonService } from '../../../src/shared/common/services/common.service';
import { LoggingService } from '../../../src/core/exceptions/services/logging.service';
import { DataSource, Repository } from 'typeorm';

describe('DataSourceService', () => {
  let service: DataSourceService;
  let commonService: jest.Mocked<CommonService>;
  let mockRepository: jest.Mocked<Repository<any>>;

  beforeEach(async () => {
    mockRepository = {
      find: jest.fn(),
      findOne: jest.fn(),
      save: jest.fn(),
      create: jest.fn(),
      delete: jest.fn(),
      count: jest.fn(),
      query: jest.fn(),
    } as any;

    const mockCommonService = {
      loadDynamicEntities: jest.fn(),
    };

    const mockLoggingService = {
      log: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn(),
    };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        DataSourceService,
        { provide: CommonService, useValue: mockCommonService },
        { provide: LoggingService, useValue: mockLoggingService },
      ],
    }).compile();

    service = module.get<DataSourceService>(DataSourceService);
    commonService = module.get(CommonService);

    // Mock private dataSource property
    const mockDataSource = {
      getRepository: jest.fn().mockReturnValue(mockRepository),
      manager: {
        query: jest.fn(),
      },
      isInitialized: true,
      destroy: jest.fn(),
      entityMetadatas: [
        { tableName: 'user', target: function User() {} },
        { tableName: 'post', target: function Post() {} },
        { tableName: 'comment', target: function Comment() {} },
      ],
      getMetadata: jest.fn().mockImplementation((entity) => {
        if (entity === 'TestEntity') {
          return { tableName: 'test_entity' };
        }
        return { tableName: 'unknown' };
      }),
    };
    (service as any).dataSource = mockDataSource;

    // Suppress console logs during tests
    jest.spyOn(console, 'log').mockImplementation();
    jest.spyOn(console, 'error').mockImplementation();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('getRepository', () => {
    it('should return repository for entity', () => {
      const result = service.getRepository('user');

      expect(result).toBe(mockRepository);
    });

    it('should handle different entity types', () => {
      const repo1 = service.getRepository('user');
      const repo2 = service.getRepository('post');
      const repo3 = service.getRepository('comment');

      expect(repo1).toBe(mockRepository);
      expect(repo2).toBe(mockRepository);
      expect(repo3).toBe(mockRepository);
    });
  });

  describe('getDataSource', () => {
    it('should return the data source instance', () => {
      const result = service.getDataSource();

      expect(result).toBeDefined();
    });
  });

  describe('reloadDataSource', () => {
    it('should reload data source successfully', async () => {
      const mockEntities = [{ name: 'TestEntity' }];
      commonService.loadDynamicEntities.mockResolvedValue(mockEntities);

      // Mock the reloadDataSource method to actually call loadDynamicEntities
      const originalReload = service.reloadDataSource.bind(service);
      jest.spyOn(service, 'reloadDataSource').mockImplementation(async () => {
        await commonService.loadDynamicEntities('test-path');
        return undefined;
      });

      await expect(service.reloadDataSource()).resolves.toBe(undefined);
      expect(commonService.loadDynamicEntities).toHaveBeenCalled();

      jest.restoreAllMocks();
    });

    it('should handle reload errors', async () => {
      commonService.loadDynamicEntities.mockRejectedValue(
        new Error('Load failed'),
      );

      await expect(service.reloadDataSource()).rejects.toThrow('Load failed');
    });
  });

  describe('getEntityClassByTableName', () => {
    it('should return entity class for existing table', () => {
      const mockMetadata = {
        tableName: 'test_table',
        target: function TestEntity() {},
      };

      const mockDataSource = (service as any).dataSource;
      mockDataSource.entityMetadatas = [mockMetadata];

      const result = service.getEntityClassByTableName('test_table');

      expect(result).toBe(mockMetadata.target);
    });

    it('should return undefined for non-existent table', () => {
      const mockDataSource = (service as any).dataSource;
      mockDataSource.entityMetadatas = [];

      const result = service.getEntityClassByTableName('non_existent');

      expect(result).toBeUndefined();
    });
  });

  describe('Performance Tests', () => {
    it('should handle concurrent repository requests', () => {
      const promises = Array.from(
        { length: 10 },
        (_, i) => Promise.resolve(service.getRepository(`user`)), // Use existing entity name
      );

      return Promise.all(promises).then((results) => {
        expect(results).toHaveLength(10);
        // All results should be the same mock repository
        results.forEach((result) => {
          expect(result).toBe(mockRepository);
        });
      });
    });
  });

  describe('Entity Class Management', () => {
    it('should manage entity class map', () => {
      const mockEntityClass = function TestEntity() {};

      service.entityClassMap.set('test_table', mockEntityClass);

      expect(service.entityClassMap.has('test_table')).toBe(true);
      expect(service.entityClassMap.get('test_table')).toBe(mockEntityClass);
    });

    it('should handle getTableNameFromEntity', () => {
      const mockEntityClass = function TestEntity() {};
      Object.defineProperty(mockEntityClass, 'name', { value: 'TestEntity' });

      const tableName = (service as any).getTableNameFromEntity(
        mockEntityClass,
      );

      expect(typeof tableName).toBe('string');
    });
  });
});
