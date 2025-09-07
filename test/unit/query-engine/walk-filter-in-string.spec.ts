import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  DataSource,
} from 'typeorm';
import { describe, beforeAll, afterAll, it, expect } from '@jest/globals';
import { walkFilter } from '../../../src/infrastructure/query-engine/utils/walk-filter';

@Entity('test_item')
class TestItem {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @Column('int')
  categoryId: number;

  @Column()
  status: string;

  @Column('float')
  price: number;
}

describe('walkFilter - _in operator string parsing tests', () => {
  let dataSource: DataSource;

  beforeAll(async () => {
    dataSource = new DataSource({
      type: 'sqlite',
      database: ':memory:',
      dropSchema: true,
      synchronize: true,
      entities: [TestItem],
    });
    await dataSource.initialize();
  });

  afterAll(async () => {
    if (dataSource?.isInitialized) {
      await dataSource.destroy();
    }
  });

  describe('_in operator with string inputs', () => {
    it('should handle comma-separated string without spaces', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _in: "1,2,3" }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.categoryId IN');
      expect(part.params).toHaveProperty('p1_0', 1);
      expect(part.params).toHaveProperty('p1_1', 2);
      expect(part.params).toHaveProperty('p1_2', 3);
      expect(typeof part.params.p1_0).toBe('number');
      expect(typeof part.params.p1_1).toBe('number');
      expect(typeof part.params.p1_2).toBe('number');
    });

    it('should handle comma-separated string with spaces', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _in: "1, 2, 3" }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.categoryId IN');
      expect(part.params).toHaveProperty('p1_0', 1);
      expect(part.params).toHaveProperty('p1_1', 2);
      expect(part.params).toHaveProperty('p1_2', 3);
      expect(typeof part.params.p1_0).toBe('number');
      expect(typeof part.params.p1_1).toBe('number');
      expect(typeof part.params.p1_2).toBe('number');
    });

    it('should handle JSON array string format', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _in: "[1,2,3]" }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.categoryId IN');
      expect(part.params).toHaveProperty('p1_0', 1);
      expect(part.params).toHaveProperty('p1_1', 2);
      expect(part.params).toHaveProperty('p1_2', 3);
      expect(typeof part.params.p1_0).toBe('number');
      expect(typeof part.params.p1_1).toBe('number');
      expect(typeof part.params.p1_2).toBe('number');
    });

    it('should handle string values for text fields', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        status: { _in: "active,pending,completed" }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.status IN');
      expect(part.params).toHaveProperty('p1_0', 'active');
      expect(part.params).toHaveProperty('p1_1', 'pending');
      expect(part.params).toHaveProperty('p1_2', 'completed');
      expect(typeof part.params.p1_0).toBe('string');
      expect(typeof part.params.p1_1).toBe('string');
      expect(typeof part.params.p1_2).toBe('string');
    });

    it('should handle regular array input (backwards compatibility)', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _in: [1, 2, 3] }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.categoryId IN');
      expect(part.params).toHaveProperty('p1_0', 1);
      expect(part.params).toHaveProperty('p1_1', 2);
      expect(part.params).toHaveProperty('p1_2', 3);
    });
  });

  describe('_not_in operator with string inputs', () => {
    it('should handle comma-separated string', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _not_in: "1, 2, 3" }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.categoryId NOT IN');
      expect(part.params).toHaveProperty('p1_0', 1);
      expect(part.params).toHaveProperty('p1_1', 2);
      expect(part.params).toHaveProperty('p1_2', 3);
    });

    it('should handle JSON array string format', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        status: { _not_in: '["active", "pending"]' }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      const part = result.parts[0];
      
      expect(part.sql).toContain('item.status NOT IN');
      expect(part.params).toHaveProperty('p1_0', 'active');
      expect(part.params).toHaveProperty('p1_1', 'pending');
    });
  });

  describe('Edge cases', () => {
    it('should handle empty string gracefully', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _in: "" }
      };

      const result = walkFilter({
        filter,
        currentMeta: itemMeta,
        currentAlias: 'item',
      });

      expect(result.parts).toHaveLength(1);
      expect(result.parts[0].sql).toBe('1 = 0');
    });

    it('should throw error for non-array/string input', () => {
      const itemMeta = dataSource.getMetadata(TestItem);
      const filter = {
        categoryId: { _in: 123 }
      };

      expect(() => {
        walkFilter({
          filter,
          currentMeta: itemMeta,
          currentAlias: 'item',
        });
      }).toThrow('_in operator requires an array');
    });
  });
});