import { ObjectId } from 'mongodb';
import { GraphQLDefinitionProcessor } from '../../src/core/bootstrap/processors/graphql-definition.processor';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

describe('GraphQLDefinitionProcessor', () => {
  let processor: GraphQLDefinitionProcessor;
  let queryBuilder: any;

  beforeEach(() => {
    queryBuilder = {
      findOne: jest.fn(),
    };
    processor = new GraphQLDefinitionProcessor(queryBuilder);
  });

  afterEach(() => {
    DatabaseConfigService.resetForTesting();
  });

  describe('transformRecords — SQL', () => {
    beforeEach(() => {
      DatabaseConfigService.overrideForTesting('mysql');
    });

    it('resolves table string to tableId', async () => {
      queryBuilder.findOne.mockResolvedValue({ id: 42, name: 'tasks' });

      const result = await processor.transformRecords([
        { table: 'tasks', isEnabled: true, isSystem: false, description: null },
      ]);

      expect(result).toHaveLength(1);
      expect(result[0].tableId).toBe(42);
      expect(result[0].table).toBeUndefined();
      expect(queryBuilder.findOne).toHaveBeenCalledWith({
        table: 'table_definition',
        where: { name: 'tasks' },
      });
    });

    it('sets default values for missing fields', async () => {
      queryBuilder.findOne.mockResolvedValue({ id: 1, name: 'tasks' });

      const result = await processor.transformRecords([{ table: 'tasks' }]);

      expect(result[0].description).toBeNull();
      expect(result[0].isSystem).toBe(false);
      expect(result[0].isEnabled).toBe(true);
    });

    it('preserves explicit values over defaults', async () => {
      queryBuilder.findOne.mockResolvedValue({ id: 1, name: 'tasks' });

      const result = await processor.transformRecords([
        {
          table: 'tasks',
          isEnabled: false,
          isSystem: true,
          description: 'Custom',
        },
      ]);

      expect(result[0].isEnabled).toBe(false);
      expect(result[0].isSystem).toBe(true);
      expect(result[0].description).toBe('Custom');
    });

    it('skips records with missing table reference', async () => {
      queryBuilder.findOne.mockResolvedValue(null);

      const result = await processor.transformRecords([
        { table: 'nonexistent', isEnabled: true },
      ]);

      expect(result).toHaveLength(0);
    });

    it('handles records without table field', async () => {
      const result = await processor.transformRecords([
        { isEnabled: true, description: 'no table' },
      ]);

      expect(result).toHaveLength(1);
      expect(queryBuilder.findOne).not.toHaveBeenCalled();
    });

    it('processes multiple records in parallel', async () => {
      queryBuilder.findOne
        .mockResolvedValueOnce({ id: 1, name: 'tasks' })
        .mockResolvedValueOnce({ id: 2, name: 'users' })
        .mockResolvedValueOnce({ id: 3, name: 'orders' });

      const result = await processor.transformRecords([
        { table: 'tasks' },
        { table: 'users' },
        { table: 'orders' },
      ]);

      expect(result).toHaveLength(3);
      expect(result.map((r: any) => r.tableId).sort()).toEqual([1, 2, 3]);
    });

    it('filters out nulls from missing tables while keeping valid ones', async () => {
      queryBuilder.findOne
        .mockResolvedValueOnce({ id: 1, name: 'tasks' })
        .mockResolvedValueOnce(null)
        .mockResolvedValueOnce({ id: 3, name: 'orders' });

      const result = await processor.transformRecords([
        { table: 'tasks' },
        { table: 'missing' },
        { table: 'orders' },
      ]);

      expect(result).toHaveLength(2);
      expect(result[0].tableId).toBe(1);
      expect(result[1].tableId).toBe(3);
    });

    it('preserves metadata field', async () => {
      queryBuilder.findOne.mockResolvedValue({ id: 1, name: 'tasks' });

      const result = await processor.transformRecords([
        { table: 'tasks', metadata: { maxDepth: 10, customField: 'val' } },
      ]);

      expect(result[0].metadata).toEqual({ maxDepth: 10, customField: 'val' });
    });

    it('sets description to null when undefined', async () => {
      queryBuilder.findOne.mockResolvedValue({ id: 1, name: 'tasks' });

      const result = await processor.transformRecords([{ table: 'tasks' }]);

      expect(result[0].description).toBeNull();
    });

    it('keeps description as empty string when explicitly set', async () => {
      queryBuilder.findOne.mockResolvedValue({ id: 1, name: 'tasks' });

      const result = await processor.transformRecords([
        { table: 'tasks', description: '' },
      ]);

      expect(result[0].description).toBe('');
    });
  });

  describe('transformRecords — MongoDB', () => {
    beforeEach(() => {
      DatabaseConfigService.overrideForTesting('mongodb');
    });

    it('resolves table string to ObjectId', async () => {
      const oid = new ObjectId();
      queryBuilder.findOne.mockResolvedValue({
        _id: oid.toString(),
        name: 'tasks',
      });

      const result = await processor.transformRecords([
        { table: 'tasks', isEnabled: true },
      ]);

      expect(result).toHaveLength(1);
      expect(result[0].table).toBeInstanceOf(ObjectId);
      expect(result[0].table.toString()).toBe(oid.toString());
    });

    it('sets createdAt and updatedAt when missing', async () => {
      const oid = new ObjectId();
      queryBuilder.findOne.mockResolvedValue({
        _id: oid.toString(),
        name: 'tasks',
      });

      const result = await processor.transformRecords([{ table: 'tasks' }]);

      expect(result[0].createdAt).toBeInstanceOf(Date);
      expect(result[0].updatedAt).toBeInstanceOf(Date);
    });

    it('preserves existing createdAt and updatedAt', async () => {
      const oid = new ObjectId();
      const existingDate = new Date('2025-01-01');
      queryBuilder.findOne.mockResolvedValue({
        _id: oid.toString(),
        name: 'tasks',
      });

      const result = await processor.transformRecords([
        { table: 'tasks', createdAt: existingDate, updatedAt: existingDate },
      ]);

      expect(result[0].createdAt).toBe(existingDate);
      expect(result[0].updatedAt).toBe(existingDate);
    });

    it('handles ObjectId returned directly (not string)', async () => {
      const oid = new ObjectId();
      queryBuilder.findOne.mockResolvedValue({ _id: oid, name: 'tasks' });

      const result = await processor.transformRecords([{ table: 'tasks' }]);

      expect(result[0].table).toBeInstanceOf(ObjectId);
    });

    it('skips records with missing table reference', async () => {
      queryBuilder.findOne.mockResolvedValue(null);

      const result = await processor.transformRecords([
        { table: 'nonexistent' },
      ]);

      expect(result).toHaveLength(0);
    });

    it('sets defaults for MongoDB records', async () => {
      const oid = new ObjectId();
      queryBuilder.findOne.mockResolvedValue({
        _id: oid.toString(),
        name: 'tasks',
      });

      const result = await processor.transformRecords([{ table: 'tasks' }]);

      expect(result[0].isEnabled).toBe(true);
      expect(result[0].isSystem).toBe(false);
      expect(result[0].description).toBeNull();
    });
  });

  describe('getUniqueIdentifier()', () => {
    it('returns tableId for SQL records', () => {
      DatabaseConfigService.overrideForTesting('mysql');
      const record = { tableId: 42, isEnabled: true };
      const identifier = processor.getUniqueIdentifier(record);
      expect(identifier).toEqual({ tableId: 42 });
    });

    it('returns table ObjectId for Mongo records', () => {
      DatabaseConfigService.overrideForTesting('mongodb');
      const oid = new ObjectId();
      const record = { table: oid, isEnabled: true };
      const identifier = processor.getUniqueIdentifier(record);
      expect(identifier).toEqual({ table: oid });
    });
  });

  describe('getCompareFields()', () => {
    it('returns the expected fields', () => {
      const fields = (processor as any).getCompareFields();
      expect(fields).toEqual(['table', 'isEnabled', 'description', 'metadata']);
    });
  });

  describe('getRecordIdentifier()', () => {
    it('uses tableId for SQL transformed record', () => {
      const record = { tableId: 5 };
      const id = (processor as any).getRecordIdentifier(record);
      expect(id).toBe('[GqlDefinition] 5');
    });

    it('uses table ObjectId for Mongo transformed record', () => {
      const oid = new ObjectId();
      const record = { table: oid };
      const id = (processor as any).getRecordIdentifier(record);
      expect(id).toContain('[GqlDefinition]');
    });

    it('handles original record with table string', () => {
      const record = { table: 'tasks' };
      const id = (processor as any).getRecordIdentifier(record);
      expect(id).toBe('[GqlDefinition] tasks');
    });
  });

  describe('idempotent upsert behavior', () => {
    it('same record transformed twice produces identical output', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      queryBuilder.findOne.mockResolvedValue({ id: 10, name: 'tasks' });

      const record = {
        table: 'tasks',
        isEnabled: true,
        isSystem: false,
        description: 'desc',
        metadata: null,
      };
      const [first] = await processor.transformRecords([record]);
      const [second] = await processor.transformRecords([record]);

      expect(first).toEqual(second);
    });

    it('different metadata produces different output', async () => {
      DatabaseConfigService.overrideForTesting('mysql');
      queryBuilder.findOne.mockResolvedValue({ id: 10, name: 'tasks' });

      const [first] = await processor.transformRecords([
        { table: 'tasks', metadata: { v: 1 } },
      ]);
      const [second] = await processor.transformRecords([
        { table: 'tasks', metadata: { v: 2 } },
      ]);

      expect(first.metadata).not.toEqual(second.metadata);
    });
  });
});
