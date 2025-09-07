import { Test, TestingModule } from '@nestjs/testing';
import { GraphqlService } from '../../../src/modules/graphql/services/graphql.service';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';
import { DynamicResolver } from '../../../src/modules/graphql/resolvers/dynamic.resolver';
import { createYoga } from 'graphql-yoga';
import { makeExecutableSchema } from '@graphql-tools/schema';

jest.mock('graphql-yoga');
jest.mock('@graphql-tools/schema');

describe('GraphqlService', () => {
  let service: GraphqlService;
  let dataSourceService: jest.Mocked<DataSourceService>;
  let dynamicResolver: jest.Mocked<DynamicResolver>;

  const mockEntityMetadata = {
    tableName: 'test_table',
    columns: [
      { propertyName: 'id', type: 'uuid', isPrimary: true },
      { propertyName: 'name', type: 'varchar' },
      { propertyName: 'createdAt', type: 'timestamp' },
      { propertyName: 'updatedAt', type: 'timestamp' },
    ],
    relations: [], // This needs to be iterable
  };

  const mockTable = {
    id: 1,
    name: 'test_table',
    columns: [
      { name: 'id', type: 'uuid', isPrimary: true, isNullable: false },
      { name: 'name', type: 'varchar', isNullable: true },
    ],
    relations: [
      {
        propertyName: 'user',
        targetTable: { name: 'user_table' },
      },
    ],
  };

  beforeEach(async () => {
    const mockQueryBuilder = {
      leftJoinAndSelect: jest.fn().mockReturnThis(),
      getMany: jest.fn().mockResolvedValue([mockTable]),
    };

    const mockTableRepo = {
      createQueryBuilder: jest.fn().mockReturnValue(mockQueryBuilder),
    };

    const mockDataSource = {
      getRepository: jest.fn().mockReturnValue(mockTableRepo),
      getMetadata: jest.fn().mockReturnValue(mockEntityMetadata),
      entityMetadatas: [mockEntityMetadata],
    };

    const mockDataSourceService = {
      getDataSource: jest.fn().mockReturnValue(mockDataSource),
    };

    const mockDynamicResolver = {
      dynamicResolver: jest.fn(),
    };

    const mockYogaApp = {
      handle: jest.fn(),
    };

    (createYoga as jest.Mock).mockReturnValue(mockYogaApp);
    (makeExecutableSchema as jest.Mock).mockReturnValue({});

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        GraphqlService,
        { provide: DataSourceService, useValue: mockDataSourceService },
        { provide: DynamicResolver, useValue: mockDynamicResolver },
      ],
    }).compile();

    service = module.get<GraphqlService>(GraphqlService);
    dataSourceService = module.get(DataSourceService);
    dynamicResolver = module.get(DynamicResolver);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('onApplicationBootstrap', () => {
    it('should reload schema on application bootstrap', async () => {
      const reloadSchemaSpy = jest.spyOn(service, 'reloadSchema');
      await service.onApplicationBootstrap();
      expect(reloadSchemaSpy).toHaveBeenCalled();
    });
  });

  describe('reloadSchema', () => {
    it('should generate schema and create yoga instance', async () => {
      await service.reloadSchema();

      expect(makeExecutableSchema).toHaveBeenCalledWith({
        typeDefs: expect.any(String),
        resolvers: expect.any(Object),
      });

      expect(createYoga).toHaveBeenCalledWith({
        schema: expect.any(Object),
        graphqlEndpoint: '/graphql',
        graphiql: true,
      });
    });

    it('should handle schema generation errors', async () => {
      const error = new Error('Schema generation failed');
      (makeExecutableSchema as jest.Mock).mockImplementationOnce(() => {
        throw error;
      });

      await expect(service.reloadSchema()).rejects.toThrow(error);
    });
  });

  describe('getYogaInstance', () => {
    it('should return yoga instance after initialization', async () => {
      await service.reloadSchema();
      const yogaInstance = service.getYogaInstance();
      expect(yogaInstance).toBeDefined();
    });

    it('should throw error if yoga instance not initialized', () => {
      expect(() => service.getYogaInstance()).toThrow(
        'GraphQL Yoga instance not initialized. Call reloadSchema() first.',
      );
    });
  });

  describe('pullMetadataFromDb', () => {
    it('should fetch all tables with relations', async () => {
      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();

      await service.reloadSchema();

      expect(tableRepo.createQueryBuilder).toHaveBeenCalledWith('table');
      expect(queryBuilder.leftJoinAndSelect).toHaveBeenCalledWith(
        'table.columns',
        'columns',
      );
      expect(queryBuilder.leftJoinAndSelect).toHaveBeenCalledWith(
        'table.relations',
        'relations',
      );
    });

    it('should handle nested relations correctly', async () => {
      const mockNestedTable = {
        ...mockTable,
        relations: [
          {
            propertyName: 'user',
            targetTable: {
              name: 'user_table',
              relations: [
                {
                  propertyName: 'profile',
                  targetTable: { name: 'profile_table' },
                },
              ],
            },
          },
        ],
      };

      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest.fn().mockResolvedValue([mockNestedTable]);

      await service.reloadSchema();

      expect(queryBuilder.leftJoinAndSelect).toHaveBeenCalled();
    });
  });

  describe('schemaGenerator', () => {
    it('should generate valid GraphQL schema from tables', async () => {
      await service.reloadSchema();

      const [callArgs] = (makeExecutableSchema as jest.Mock).mock.calls[0];
      const typeDefs = callArgs.typeDefs;

      // Check for scalar JSON
      expect(typeDefs).toContain('scalar JSON');

      // Check for type definition
      expect(typeDefs).toContain('type test_table');

      // Check for Query type
      expect(typeDefs).toContain('type Query');

      // Check for Result type
      expect(typeDefs).toContain('type test_tableResult');
    });

    it('should create resolvers with dynamic resolver proxy', async () => {
      await service.reloadSchema();

      const [callArgs] = (makeExecutableSchema as jest.Mock).mock.calls[0];
      const resolvers = callArgs.resolvers;

      expect(resolvers).toHaveProperty('Query');
      expect(resolvers.Query).toBeDefined();
    });

    it('should handle tables without relations', async () => {
      const simpleTable = {
        id: 1,
        name: 'simple_table',
        columns: [
          { name: 'id', type: 'int', isPrimary: true, isNullable: false },
        ],
        relations: [],
      };

      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest.fn().mockResolvedValue([simpleTable]);

      await service.reloadSchema();

      expect(makeExecutableSchema).toHaveBeenCalled();
    });
  });

  describe('Type Generation', () => {
    it('should map column types correctly', async () => {
      const tableWithTypes = {
        id: 1,
        name: 'typed_table',
        columns: [
          { name: 'id', type: 'uuid', isPrimary: true, isNullable: false },
          { name: 'count', type: 'int', isNullable: true },
          { name: 'price', type: 'float', isNullable: false },
          { name: 'active', type: 'boolean', isNullable: false },
          { name: 'data', type: 'json', isNullable: true },
          { name: 'created', type: 'timestamp', isNullable: false },
        ],
        relations: [],
      };

      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest.fn().mockResolvedValue([tableWithTypes]);

      await service.reloadSchema();

      const [callArgs] = (makeExecutableSchema as jest.Mock).mock.calls[0];
      const typeDefs = callArgs.typeDefs;

      expect(typeDefs).toContain('id: ID!');
      expect(typeDefs).toContain('count: Int');
      expect(typeDefs).toContain('price: Float!');
      expect(typeDefs).toContain('active: Boolean!');
      expect(typeDefs).toContain('data: JSON');
      expect(typeDefs).toContain('created: String!');
    });

    it('should handle relation types correctly', async () => {
      // Create mock data with actual relations
      const tableWithRelations = {
        id: 1,
        name: 'test_table',
        columns: [{ name: 'id', type: 'uuid', isPrimary: true }],
        relations: [
          {
            propertyName: 'user',
            inverseEntityMetadata: { tableName: 'user_table' },
            isOneToMany: false,
            isManyToMany: false,
          },
        ],
      };

      // Update entity metadata to have relations too
      const entityWithRelations = {
        ...mockEntityMetadata,
        relations: [
          {
            propertyName: 'user',
            inverseEntityMetadata: { tableName: 'user_table' },
            isOneToMany: false,
            isManyToMany: false,
          },
        ],
      };

      // Mock the dataSource more completely
      const dataSource = dataSourceService.getDataSource();
      dataSource.getMetadata = jest.fn().mockReturnValue(entityWithRelations);
      Object.defineProperty(dataSource, 'entityMetadatas', {
        value: [entityWithRelations],
        writable: true,
      });

      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest.fn().mockResolvedValue([tableWithRelations]);

      await service.reloadSchema();

      const [callArgs] = (makeExecutableSchema as jest.Mock).mock.calls[0];
      const typeDefs = callArgs.typeDefs;

      // Should have the relation field
      expect(typeDefs).toContain('type test_table');
      expect(typeDefs).toContain('user: user_table');
    });
  });

  describe('Error Handling', () => {
    it('should handle empty table list', async () => {
      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest.fn().mockResolvedValue([]);

      await service.reloadSchema();

      expect(makeExecutableSchema).toHaveBeenCalled();
      expect(createYoga).toHaveBeenCalled();
    });

    it('should handle database connection errors', async () => {
      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest.fn().mockRejectedValue(new Error('DB Error'));

      await expect(service.reloadSchema()).rejects.toThrow('DB Error');
    });

    it('should skip tables with invalid names', async () => {
      const invalidTable = {
        id: 1,
        name: null,
        columns: [],
        relations: [],
      };

      const dataSource = dataSourceService.getDataSource();
      const tableRepo = dataSource.getRepository('table_definition');
      const queryBuilder = tableRepo.createQueryBuilder();
      queryBuilder.getMany = jest
        .fn()
        .mockResolvedValue([invalidTable, mockTable]);

      await service.reloadSchema();

      const [callArgs] = (makeExecutableSchema as jest.Mock).mock.calls[0];
      const typeDefs = callArgs.typeDefs;

      expect(typeDefs).toContain('type test_table');
      expect(typeDefs).not.toContain('type null');
    });
  });

  describe('Resolver Proxy', () => {
    it('should proxy resolver calls to dynamic resolver', async () => {
      await service.reloadSchema();

      const [callArgs] = (makeExecutableSchema as jest.Mock).mock.calls[0];
      const resolvers = callArgs.resolvers;
      const queryProxy = resolvers.Query;

      // Simulate GraphQL calling the resolver
      const parent = {};
      const args = { filter: {}, page: 1, limit: 10 };
      const ctx = { user: { id: 1 } };
      const info = { fieldNodes: [] };

      // Access a property on the proxy
      const testTableResolver = queryProxy.test_table;
      await testTableResolver(parent, args, ctx, info);

      expect(dynamicResolver.dynamicResolver).toHaveBeenCalledWith(
        'test_table',
        args,
        ctx,
        info,
      );
    });
  });
});
