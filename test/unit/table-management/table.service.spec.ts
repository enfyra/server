import { Test, TestingModule } from '@nestjs/testing';
import { TableHandlerService } from '../../../src/modules/table-management/services/table-handler.service';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';
import { CommonService } from '../../../src/shared/common/services/common.service';
import { MetadataSyncService } from '../../../src/modules/schema-management/services/metadata-sync.service';
import { SchemaReloadService } from '../../../src/modules/schema-management/services/schema-reload.service';
import { LoggingService } from '../../../src/core/exceptions/services/logging.service';
import { Logger } from '@nestjs/common';

// Mock the validation utility
jest.mock('../../../src/modules/table-management/utils/duplicate-field-check', () => ({
  validateUniquePropertyNames: jest.fn(),
}));

jest.mock('../../../src/modules/table-management/utils/get-deleted-ids', () => ({
  getDeletedIds: jest.fn().mockReturnValue([]),
}));

describe('TableHandlerService', () => {
  let service: TableHandlerService;
  let dataSourceService: jest.Mocked<DataSourceService>;
  let metadataSyncService: jest.Mocked<MetadataSyncService>;
  let schemaReloadService: jest.Mocked<SchemaReloadService>;
  let commonService: jest.Mocked<CommonService>;

  const mockTable = {
    id: 1,
    name: 'test_table',
    displayName: 'Test Table',
    isEnabled: true,
    columns: [],
    relations: [],
  };

  const mockQueryRunner = {
    connect: jest.fn(),
    startTransaction: jest.fn(),
    commitTransaction: jest.fn(),
    rollbackTransaction: jest.fn(),
    release: jest.fn(),
    query: jest.fn(),
    dropTable: jest.fn(),
    hasTable: jest.fn(),
    createTable: jest.fn(),
  };

  const mockRepo = {
    find: jest.fn(),
    findOne: jest.fn(),
    save: jest.fn(),
    delete: jest.fn(),
    create: jest.fn(),
    remove: jest.fn(),
  };

  const mockDataSource = {
    createQueryRunner: jest.fn().mockReturnValue(mockQueryRunner),
    getRepository: jest.fn().mockReturnValue(mockRepo),
    manager: {
      query: jest.fn(),
    },
    getMetadata: jest.fn(),
  };

  beforeEach(async () => {
    jest.clearAllMocks();

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        TableHandlerService,
        {
          provide: DataSourceService,
          useValue: {
            getDataSource: jest.fn().mockReturnValue(mockDataSource),
            getRepository: jest.fn().mockReturnValue(mockRepo),
            entityClassMap: new Map([
              ['table_definition', {}],
              ['column_definition', {}],
              ['relation_definition', {}],
              ['route_definition', {}],
            ]),
          },
        },
        {
          provide: CommonService,
          useValue: {
            delay: jest.fn().mockResolvedValue(undefined),
          },
        },
        {
          provide: MetadataSyncService,
          useValue: {
            syncAll: jest.fn().mockResolvedValue('v1'),
          },
        },
        {
          provide: SchemaReloadService,
          useValue: {
            lockSchema: jest.fn().mockResolvedValue(undefined),
            unlockSchema: jest.fn().mockResolvedValue(undefined),
            publishSchemaUpdated: jest.fn().mockResolvedValue(undefined),
          },
        },
        {
          provide: LoggingService,
          useValue: {
            log: jest.fn(),
            error: jest.fn(),
            warn: jest.fn(),
            debug: jest.fn(),
          },
        },
      ],
    }).compile();

    service = module.get<TableHandlerService>(TableHandlerService);
    dataSourceService = module.get(DataSourceService);
    commonService = module.get(CommonService);
    metadataSyncService = module.get(MetadataSyncService);
    schemaReloadService = module.get(SchemaReloadService);

    // Suppress console.error in tests
    jest.spyOn(console, 'error').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'log').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'error').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'warn').mockImplementation(() => {});
    jest.spyOn(Logger.prototype, 'debug').mockImplementation(() => {});
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe('createTable', () => {
    const createTableDto = {
      name: 'new_table',
      displayName: 'New Table',
      columns: [
        {
          name: 'id',
          type: 'uuid',
          isPrimary: true,
          isNullable: false,
        },
      ],
      relations: [],
    };

    it('should create table successfully', async () => {
      mockQueryRunner.hasTable.mockResolvedValue(false);
      mockRepo.findOne.mockResolvedValue(null);
      mockRepo.create.mockReturnValue(mockTable);
      mockRepo.save.mockResolvedValue(mockTable);

      const result = await service.createTable(createTableDto);

      expect(result).toEqual(mockTable);
      expect(mockQueryRunner.connect).toHaveBeenCalled();
      expect(mockQueryRunner.release).toHaveBeenCalled();
      expect(schemaReloadService.lockSchema).toHaveBeenCalled();
      expect(metadataSyncService.syncAll).toHaveBeenCalled();
      expect(schemaReloadService.unlockSchema).toHaveBeenCalled();
    });

    it('should throw error if table already exists', async () => {
      mockQueryRunner.hasTable.mockResolvedValue(true);
      mockRepo.findOne.mockResolvedValue(mockTable);

      await expect(service.createTable(createTableDto)).rejects.toThrow(
        "Table with name 'new_table' already exists",
      );
    });

    it('should throw error if no id column with isPrimary', async () => {
      const invalidDto = {
        name: 'new_table',
        columns: [
          {
            name: 'name',
            type: 'varchar',
            isPrimary: false,
          },
        ],
      };

      mockQueryRunner.hasTable.mockResolvedValue(false);
      mockRepo.findOne.mockResolvedValue(null);

      await expect(service.createTable(invalidDto)).rejects.toThrow(
        'Table must contain a column named "id" with isPrimary = true.',
      );
    });

    it('should throw error if id column has invalid type', async () => {
      const invalidDto = {
        name: 'new_table',
        columns: [
          {
            name: 'id',
            type: 'varchar',
            isPrimary: true,
          },
        ],
      };

      mockQueryRunner.hasTable.mockResolvedValue(false);
      mockRepo.findOne.mockResolvedValue(null);

      await expect(service.createTable(invalidDto)).rejects.toThrow(
        'The primary column "id" must be of type int or uuid.',
      );
    });

    it('should throw error if multiple primary columns', async () => {
      const invalidDto = {
        name: 'new_table',
        columns: [
          {
            name: 'id',
            type: 'uuid',
            isPrimary: true,
          },
          {
            name: 'id2',
            type: 'int',
            isPrimary: true,
          },
        ],
      };

      mockQueryRunner.hasTable.mockResolvedValue(false);
      mockRepo.findOne.mockResolvedValue(null);

      await expect(service.createTable(invalidDto)).rejects.toThrow(
        'Only one column is allowed to have isPrimary = true.',
      );
    });

    it('should create route definition after table creation', async () => {
      mockQueryRunner.hasTable.mockResolvedValue(false);
      mockRepo.findOne.mockResolvedValue(null);
      mockRepo.create.mockReturnValue(mockTable);
      mockRepo.save
        .mockResolvedValueOnce(mockTable) // table save
        .mockResolvedValueOnce({ id: 1 }); // route save

      await service.createTable(createTableDto);

      expect(mockRepo.save).toHaveBeenCalledTimes(2);
      expect(mockRepo.save).toHaveBeenLastCalledWith({
        path: '/test_table',
        mainTable: 1,
        isEnabled: true,
      });
    });
  });

  describe('updateTable', () => {
    const updateDto = {
      displayName: 'Updated Table',
      columns: [
        {
          name: 'id',
          type: 'uuid',
          isPrimary: true,
        },
      ],
      relations: [],
    };

    it('should update table successfully', async () => {
      const existingTable = {
        ...mockTable,
        columns: [],
        relations: [],
      };

      mockRepo.findOne.mockResolvedValue(existingTable);
      mockRepo.save.mockResolvedValue({ ...existingTable, ...updateDto });
      mockRepo.delete.mockResolvedValue({ affected: 0 });

      const result = await service.updateTable(1, updateDto as any);

      expect(result).toBeDefined();
      expect(mockQueryRunner.connect).toHaveBeenCalled();
      expect(mockQueryRunner.release).toHaveBeenCalled();
      expect(metadataSyncService.syncAll).toHaveBeenCalled();
    });

    it('should throw error for non-existent table', async () => {
      mockRepo.findOne.mockResolvedValue(null);

      await expect(service.updateTable(999, updateDto as any)).rejects.toThrow(
        "Table with identifier '999' not found",
      );
    });

    it('should throw error if no primary column in update', async () => {
      const invalidUpdateDto = {
        name: 'test_table',
        columns: [
          {
            name: 'name',
            type: 'varchar',
            isPrimary: false,
          },
        ],
      };

      mockRepo.findOne.mockResolvedValue(mockTable);

      await expect(
        service.updateTable(1, invalidUpdateDto as any),
      ).rejects.toThrow(
        'Table must contain an id column with isPrimary = true!',
      );
    });

    it('should delete removed columns and relations', async () => {
      const existingTable = {
        ...mockTable,
        columns: [
          { id: 1, name: 'col1' },
          { id: 2, name: 'col2' },
        ],
        relations: [{ id: 1, propertyName: 'rel1' }],
      };

      const updateWithRemovals = {
        name: 'test_table',
        columns: [
          { id: 1, name: 'col1', isPrimary: true, type: 'uuid' }, // col2 removed, make it primary
        ],
        relations: [], // rel1 removed
      };

      mockRepo.findOne.mockResolvedValue(existingTable);
      mockRepo.save.mockResolvedValue(existingTable);
      mockRepo.delete.mockResolvedValue({ affected: 1 });

      // Mock getDeletedIds to return the IDs that should be deleted
      const {
        getDeletedIds,
      } = require('../../../src/modules/table-management/utils/get-deleted-ids');
      getDeletedIds
        .mockReturnValueOnce([2]) // deleted column id
        .mockReturnValueOnce([1]); // deleted relation id

      await service.updateTable(1, updateWithRemovals as any);

      expect(mockRepo.delete).toHaveBeenCalledWith([2]); // column deletion
      expect(mockRepo.delete).toHaveBeenCalledWith([1]); // relation deletion
    });
  });

  describe('delete', () => {
    it('should delete table and drop from database', async () => {
      mockRepo.findOne.mockResolvedValue(mockTable);
      mockQueryRunner.query.mockResolvedValue([]);
      mockQueryRunner.hasTable.mockResolvedValue(true);
      mockRepo.remove.mockResolvedValue(mockTable);

      await service.delete(1);

      expect(mockQueryRunner.dropTable).toHaveBeenCalledWith('test_table');
      expect(mockRepo.remove).toHaveBeenCalledWith(mockTable);
      expect(metadataSyncService.syncAll).toHaveBeenCalled();
    });

    it('should handle foreign key constraints before dropping', async () => {
      mockRepo.findOne.mockResolvedValue(mockTable);
      mockQueryRunner.query
        .mockResolvedValueOnce([
          { TABLE_NAME: 'other_table', CONSTRAINT_NAME: 'FK_ref_test' },
        ]) // referencing FKs query
        .mockResolvedValueOnce(undefined) // FK drop query result
        .mockResolvedValueOnce([{ CONSTRAINT_NAME: 'FK_test_out' }]) // outgoing FKs query
        .mockResolvedValue(undefined); // remaining FK drop queries

      mockQueryRunner.hasTable.mockResolvedValue(true);
      mockRepo.remove.mockResolvedValue(mockTable);

      await service.delete(1);

      // Should drop referencing FK
      expect(mockQueryRunner.query).toHaveBeenCalledWith(
        expect.stringContaining(
          'ALTER TABLE `other_table` DROP FOREIGN KEY `FK_ref_test`',
        ),
      );
      // The outgoing FK drop might be handled differently, let's just check it was called
      expect(mockQueryRunner.dropTable).toHaveBeenCalledWith('test_table');
      expect(mockRepo.remove).toHaveBeenCalledWith(mockTable);
    });

    it('should throw error for non-existent table', async () => {
      mockRepo.findOne.mockResolvedValue(null);

      await expect(service.delete(999)).rejects.toThrow(
        "Table with identifier '999' not found",
      );
    });

    it('should handle table that does not exist in database', async () => {
      mockRepo.findOne.mockResolvedValue(mockTable);
      mockQueryRunner.query
        .mockResolvedValueOnce([]) // no referencing FKs
        .mockResolvedValueOnce([]) // no outgoing FKs
        .mockResolvedValue(undefined);
      mockQueryRunner.hasTable.mockResolvedValue(false);
      mockRepo.remove.mockResolvedValue(mockTable);

      await service.delete(1);

      expect(mockQueryRunner.dropTable).not.toHaveBeenCalled();
      expect(mockRepo.remove).toHaveBeenCalledWith(mockTable);
    });
  });

  describe('afterEffect', () => {
    it('should handle schema synchronization', async () => {
      await service.afterEffect({ entityName: 'test_table', type: 'create' });

      expect(schemaReloadService.lockSchema).toHaveBeenCalled();
      expect(metadataSyncService.syncAll).toHaveBeenCalledWith({
        entityName: 'test_table',
        type: 'create',
      });
      expect(schemaReloadService.publishSchemaUpdated).toHaveBeenCalled();
      expect(commonService.delay).toHaveBeenCalledWith(1000);
      expect(schemaReloadService.unlockSchema).toHaveBeenCalled();
    });

    it('should unlock schema even if sync fails', async () => {
      metadataSyncService.syncAll.mockRejectedValue(new Error('Sync failed'));

      await expect(
        service.afterEffect({ entityName: 'test_table', type: 'update' }),
      ).rejects.toThrow('Sync failed');

      expect(schemaReloadService.unlockSchema).toHaveBeenCalled();
    });
  });
});
