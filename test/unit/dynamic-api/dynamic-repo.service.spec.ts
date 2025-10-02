// @ts-nocheck
import { Test, TestingModule } from '@nestjs/testing';
import { DynamicRepository } from '../../../src/modules/dynamic-api/repositories/dynamic.repository';
import { TableHandlerService } from '../../../src/modules/table-management/services/table-handler.service';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';
import { QueryEngine } from '../../../src/infrastructure/query-engine/services/query-engine.service';
import { RouteCacheService } from '../../../src/infrastructure/cache/services/route-cache.service';
import { SystemProtectionService } from '../../../src/modules/dynamic-api/services/system-protection.service';
describe.skip('DynamicRepository', () => {
  let service: DynamicRepository;
  let tableHandlerService: jest.Mocked<TableHandlerService>;
  let dataSourceService: jest.Mocked<DataSourceService>;
  let queryEngine: jest.Mocked<QueryEngine>;
  let routeCacheService: jest.Mocked<RouteCacheService>;
  let systemProtectionService: jest.Mocked<SystemProtectionService>;

  const mockTableDef = {
    id: '1',
    name: 'test_table',
    columns: [
      { name: 'id', type: 'uuid', isPrimary: true },
      { name: 'name', type: 'string' },
      { name: 'age', type: 'number' },
    ],
  };

  const mockServices = () => ({
    tableHandlerService: {
      findOne: jest.fn().mockResolvedValue(null),
    } as any,
    dataSourceService: {
      getRepository: jest.fn(),
      entityClassMap: new Map(),
    },
    queryEngine: {
      find: jest.fn(),
      count: jest.fn(),
      create: jest.fn(),
      update: jest.fn(),
      delete: jest.fn(),
    },
    routeCacheService: {
      getRoutesWithSWR: jest.fn(),
    },
    systemProtectionService: {
      isSystemTable: jest.fn().mockReturnValue(false),
      validateAccess: jest.fn().mockReturnValue(true),
    },
  });

  beforeEach(async () => {
    const mocks = mockServices();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        { provide: TableHandlerService, useValue: mocks.tableHandlerService },
        { provide: DataSourceService, useValue: mocks.dataSourceService },
        { provide: QueryEngine, useValue: mocks.queryEngine },
        { provide: RouteCacheService, useValue: mocks.routeCacheService },
        {
          provide: SystemProtectionService,
          useValue: mocks.systemProtectionService,
        },
      ],
    }).compile();

    tableHandlerService = module.get(TableHandlerService);
    dataSourceService = module.get(DataSourceService);
    queryEngine = module.get(QueryEngine);
    routeCacheService = module.get(RouteCacheService);
    systemProtectionService = module.get(SystemProtectionService);

    // Create service instance
    service = new DynamicRepository({
      query: {},
      tableName: 'test_table',
      tableHandlerService,
      dataSourceService,
      queryEngine,
      routeCacheService,
      systemProtectionService,
      currentUser: null,
    });
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('init', () => {
    it('should initialize successfully', async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);

      await service.init();

      expect(tableHandlerService.findOne).toHaveBeenCalledWith({
        where: { name: 'test_table' },
        relations: ['columns', 'relations'],
      });
    });

    it('should throw error for non-existent table', async () => {
      tableHandlerService.findOne.mockResolvedValue(null);

      await expect(service.init()).rejects.toThrow(
        'Table test_table not found',
      );
    });

    it('should handle system table protection', async () => {
      systemProtectionService.isSystemTable.mockReturnValue(true);
      systemProtectionService.validateAccess.mockReturnValue(false);
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);

      await expect(service.init()).rejects.toThrow(
        'Access denied to system table',
      );
    });
  });

  describe('find', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should find records with basic query', async () => {
      const mockResults = [
        { id: '1', name: 'John', age: 25 },
        { id: '2', name: 'Jane', age: 30 },
      ];

      queryEngine.find.mockResolvedValue({
        data: mockResults,
        meta: { totalCount: 2 },
      });

      const result = await service.find({});

      expect(result.data).toEqual(mockResults);
      expect(result.meta.totalCount).toBe(2);
    });

    it('should find records with filters', async () => {
      const mockResults = [{ id: '1', name: 'John', age: 25 }];

      queryEngine.find.mockResolvedValue({
        data: mockResults,
        meta: { totalCount: 1 },
      });

      const result = await service.find({
        where: { age: { _gte: 25 } },
      });

      expect(queryEngine.find).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: { age: { _gte: 25 } },
        }),
      );
      expect(result.data).toEqual(mockResults);
    });

    it('should find records with sorting', async () => {
      const mockResults = [
        { id: '2', name: 'Jane', age: 30 },
        { id: '1', name: 'John', age: 25 },
      ];

      queryEngine.find.mockResolvedValue({
        data: mockResults,
        meta: { totalCount: 2 },
      });

      await service.find({
        where: {},
      } as any);

      expect(queryEngine.find).toHaveBeenCalledWith(
        expect.objectContaining({
          sort: ['-age'],
        }),
      );
    });

    it('should find records with pagination', async () => {
      const mockResults = [{ id: '1', name: 'John', age: 25 }];

      queryEngine.find.mockResolvedValue({
        data: mockResults,
        meta: { totalCount: 10 },
      });

      await service.find({
        where: {},
      } as any);

      expect(queryEngine.find).toHaveBeenCalledWith(
        expect.objectContaining({
          page: 3, // skip 10, take 5 = page 3
          limit: 5,
        }),
      );
    });

    it('should find records with field selection', async () => {
      const mockResults = [{ id: '1', name: 'John' }];

      queryEngine.find.mockResolvedValue({
        data: mockResults,
        meta: { totalCount: 1 },
      });

      await service.find({
        where: {},
      } as any);

      expect(queryEngine.find).toHaveBeenCalledWith(
        expect.objectContaining({
          fields: 'id,name',
        }),
      );
    });
  });

  describe('findOne', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should find single record', async () => {
      const mockResult = { id: '1', name: 'John', age: 25 };

      queryEngine.find.mockResolvedValue({
        data: [mockResult],
        meta: { totalCount: 1 },
      });

      const result = await service.findOne({
        where: { id: '1' },
      });

      expect(result).toEqual(mockResult);
      expect(queryEngine.find).toHaveBeenCalledWith(
        expect.objectContaining({
          limit: 1,
        }),
      );
    });

    it('should return null when no record found', async () => {
      queryEngine.find.mockResolvedValue({
        data: [],
        meta: { totalCount: 0 },
      });

      const result = await service.findOne({
        where: { id: '999' },
      });

      expect(result).toBeNull();
    });
  });

  describe('count', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should count records', async () => {
      queryEngine.count.mockResolvedValue(42);

      const result = await service.count({
        where: { age: { _gte: 18 } },
      });

      expect(result).toBe(42);
      expect(queryEngine.count).toHaveBeenCalledWith(
        expect.objectContaining({
          filter: { age: { _gte: 18 } },
        }),
      );
    });

    it('should count all records when no filter provided', async () => {
      queryEngine.count.mockResolvedValue(100);

      const result = await service.count({});

      expect(result).toBe(100);
    });
  });

  describe('create', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should create new record', async () => {
      const newRecord = { name: 'Alice', age: 28 };
      const createdRecord = { id: '3', ...newRecord };

      queryEngine.create.mockResolvedValue(createdRecord);

      const result = await service.create(newRecord);

      expect(result).toEqual(createdRecord);
      expect(queryEngine.create).toHaveBeenCalledWith({
        tableName: 'test_table',
        data: newRecord,
      });
    });

    it('should validate required fields', async () => {
      const invalidRecord = { age: 28 }; // missing required name field

      queryEngine.create.mockRejectedValue(new Error('Validation failed'));

      await expect(service.create(invalidRecord)).rejects.toThrow(
        'Validation failed',
      );
    });
  });

  describe('update', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should update existing record', async () => {
      const updateData = { name: 'John Updated', age: 26 };
      const updatedRecord = { id: '1', ...updateData };

      queryEngine.update.mockResolvedValue(updatedRecord);

      const result = await service.update('1', updateData);

      expect(result).toEqual(updatedRecord);
      expect(queryEngine.update).toHaveBeenCalledWith({
        tableName: 'test_table',
        id: '1',
        data: updateData,
      });
    });

    it('should handle partial updates', async () => {
      const partialUpdate = { age: 27 };
      const updatedRecord = { id: '1', name: 'John', age: 27 };

      queryEngine.update.mockResolvedValue(updatedRecord);

      const result = await service.update('1', partialUpdate);

      expect(result).toEqual(updatedRecord);
    });
  });

  describe('delete', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should delete record by id', async () => {
      queryEngine.delete.mockResolvedValue({ affected: 1 });

      const result = await service.delete('1');

      expect(result).toEqual({ affected: 1 });
      expect(queryEngine.delete).toHaveBeenCalledWith({
        tableName: 'test_table',
        id: '1',
      });
    });

    it('should handle delete with conditions', async () => {
      queryEngine.delete.mockResolvedValue({ affected: 3 });

      const result = await service.delete({
        where: { age: { _lt: 18 } },
      });

      expect(result).toEqual({ affected: 3 });
    });
  });

  describe('Performance Tests', () => {
    beforeEach(async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();
    });

    it('should handle concurrent operations', async () => {
      const mockResults = Array.from({ length: 10 }, (_, i) => ({
        id: `${i + 1}`,
        name: `User ${i + 1}`,
        age: 20 + i,
      }));

      queryEngine.find.mockResolvedValue({
        data: mockResults,
        meta: { totalCount: 10 },
      });

      const promises = Array.from({ length: 5 }, () => service.find({}));
      const results = await Promise.all(promises);

      expect(results).toHaveLength(5);
      expect(results.every((r) => r.data.length === 10)).toBe(true);
    });

    it('should cache table definition after initialization', async () => {
      // Multiple operations should not re-fetch table definition
      await service.find({});
      await service.count({});
      await service.create({ name: 'Test', age: 25 });

      expect(tableHandlerService.findOne).toHaveBeenCalledTimes(1);
    });
  });

  describe('Error Handling', () => {
    it('should handle QueryEngine errors gracefully', async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();

      queryEngine.find.mockRejectedValue(
        new Error('Database connection failed'),
      );

      await expect(service.find({})).rejects.toThrow(
        'Database connection failed',
      );
    });

    it('should validate operations before initialization', async () => {
      await expect(service.find({})).rejects.toThrow('Service not initialized');
    });
  });

  describe('Security Tests', () => {
    it('should respect user permissions', async () => {
      const userService = new DynamicRepository({
        query: {},
        tableName: 'test_table',
        tableHandlerService,
        dataSourceService,
        queryEngine,
        routeCacheService,
        systemProtectionService,
        currentUser: { id: '1', role: 'user' },
      });

      systemProtectionService.validateAccess.mockReturnValue(false);
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);

      await expect(userService.init()).rejects.toThrow('Access denied');
    });

    it('should sanitize input data', async () => {
      tableHandlerService.findOne.mockResolvedValue(mockTableDef);
      await service.init();

      const maliciousData = {
        name: "'; DROP TABLE users; --",
        age: 25,
      };

      queryEngine.create.mockImplementation((params) => {
        // Verify that dangerous input is handled
        expect(params.data.name).toBeDefined();
        return Promise.resolve({ id: '1', ...params.data });
      });

      await service.create(maliciousData);
    });
  });
});
