import { Entity, PrimaryGeneratedColumn, Column, DataSource } from 'typeorm';
import { describe, beforeAll, afterAll, it, expect } from '@jest/globals';
import { QueryEngine } from '../../../src/infrastructure/query-engine/services/query-engine.service';
import { DataSourceService } from '../../../src/core/database/data-source/data-source.service';
import { LoggingService } from '../../../src/core/exceptions/services/logging.service';

@Entity('test_product')
class TestProduct {
  @PrimaryGeneratedColumn()
  id: number;

  @Column()
  name: string;

  @Column('int')
  price: number;

  @Column('float')
  rating: number;

  @Column('datetime')
  createdAt: Date;

  @Column('date')
  releaseDate: Date;
}

describe('QueryEngine - _between operator tests', () => {
  let dataSource: DataSource;
  let queryEngine: QueryEngine;

  beforeAll(async () => {
    dataSource = new DataSource({
      type: 'sqlite',
      database: ':memory:',
      dropSchema: true,
      synchronize: true,
      entities: [TestProduct],
    });
    await dataSource.initialize();

    // Seed test data
    const productRepo = dataSource.getRepository(TestProduct);
    const products: TestProduct[] = [];

    // Create products with various prices, ratings, and dates
    for (let i = 1; i <= 20; i++) {
      const product = new TestProduct();
      product.name = `Product ${i}`;
      product.price = i * 50; // 50, 100, 150, ..., 1000
      product.rating = 1 + (i % 5) * 0.5; // 1.0, 1.5, 2.0, 2.5, 3.0, 1.0, ...
      product.createdAt = new Date(2024, 0, i); // Jan 1-20, 2024
      product.releaseDate = new Date(2024, i % 12, 1); // Various months in 2024
      products.push(product);
    }

    await productRepo.save(products);

    // Create DataSourceService
    const fakeCommonService = {
      loadDynamicEntities: async () => [TestProduct],
    };
    const mockLoggingService = {
      log: jest.fn(),
      error: jest.fn(),
      warn: jest.fn(),
      debug: jest.fn(),
    };

    const dsService = new DataSourceService(
      fakeCommonService as any,
      mockLoggingService as any,
    );
    (dsService as any).dataSource = dataSource;
    dsService.entityClassMap.set('test_product', TestProduct);

    queryEngine = new QueryEngine(dsService, mockLoggingService as any);
  });

  afterAll(async () => {
    await dataSource.destroy();
  });

  describe('_between with numeric values', () => {
    it('should handle _between with array format for integers', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { price: { _between: [200, 500] } },
        sort: ['price'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBe(7); // Products 4-10 (prices 200-500)
      expect(result.data.every((p) => p.price >= 200 && p.price <= 500)).toBe(
        true,
      );
      expect(result.data[0].price).toBe(200);
      expect(result.data[result.data.length - 1].price).toBe(500);
    });

    it('should handle _between with string format for integers', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { price: { _between: '200,500' } },
        sort: ['price'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBe(7); // Products 4-10 (prices 200-500)
      expect(result.data.every((p) => p.price >= 200 && p.price <= 500)).toBe(
        true,
      );
    });

    it('should handle _between with float values (array format)', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { rating: { _between: [1.5, 2.5] } },
        sort: ['rating'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.every((p) => p.rating >= 1.5 && p.rating <= 2.5)).toBe(
        true,
      );
    });

    it('should handle _between with float values (string format)', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { rating: { _between: '1.5,2.5' } },
        sort: ['rating'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.every((p) => p.rating >= 1.5 && p.rating <= 2.5)).toBe(
        true,
      );
    });

    it('should handle _between with spaces in string format', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { price: { _between: ' 300 , 700 ' } },
        sort: ['price'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.every((p) => p.price >= 300 && p.price <= 700)).toBe(
        true,
      );
    });

    it('should return empty when no values match _between range', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { price: { _between: [2000, 3000] } },
      });

      expect(result.data).toBeDefined();
      expect(result.data).toEqual([]);
    });

    it('should handle edge cases with same values', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { price: { _between: [300, 300] } },
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBe(1);
      expect(result.data[0].price).toBe(300);
    });
  });

  describe('_between with date values', () => {
    it('should handle _between with Date array for datetime fields', async () => {
      const startDate = new Date(2024, 0, 5); // Jan 5, 2024
      const endDate = new Date(2024, 0, 15); // Jan 15, 2024

      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { createdAt: { _between: [startDate, endDate] } },
        sort: ['createdAt'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBe(11); // Products 5-15
      expect(result.data[0].name).toBe('Product 5');
      expect(result.data[result.data.length - 1].name).toBe('Product 15');
    });

    it('should handle _between with date strings (ISO format)', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { createdAt: { _between: '2024-01-05,2024-01-15' } },
        sort: ['createdAt'],
      });

      expect(result.data).toBeDefined();
      // Since dates in SQLite might have time components, check the actual range
      expect(result.data.length).toBeGreaterThanOrEqual(10); // At least products 5-14
    });

    it('should handle _between with date strings (array format)', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { createdAt: { _between: ['2024-01-05', '2024-01-15'] } },
        sort: ['createdAt'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBeGreaterThanOrEqual(10); // At least products 5-14
    });

    it('should handle _between with different date formats', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { releaseDate: { _between: '2024-03-01,2024-06-01' } },
        sort: ['releaseDate'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBeGreaterThan(0);
    });
  });

  describe('_between error handling', () => {
    it('should throw error for invalid string format (not 2 values)', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { price: { _between: '100' } },
        }),
      ).rejects.toThrow(
        '_between operator requires exactly 2 comma-separated values',
      );
    });

    it('should throw error for invalid string format (too many values)', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { price: { _between: '100,200,300' } },
        }),
      ).rejects.toThrow(
        '_between operator requires exactly 2 comma-separated values',
      );
    });

    it('should throw error for invalid array format (not 2 values)', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { price: { _between: [100] } },
        }),
      ).rejects.toThrow('_between operator requires exactly 2 values');
    });

    it('should throw error for invalid array format (too many values)', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { price: { _between: [100, 200, 300] } },
        }),
      ).rejects.toThrow('_between operator requires exactly 2 values');
    });

    it('should throw error for invalid type (not string or array)', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { price: { _between: 100 } },
        }),
      ).rejects.toThrow(
        '_between operator requires either a comma-separated string or array of 2 values',
      );
    });

    it('should throw error for invalid numeric values', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { price: { _between: 'abc,xyz' } },
        }),
      ).rejects.toThrow('_between operator requires valid numeric values');
    });

    it('should throw error for invalid date values', async () => {
      await expect(
        queryEngine.find({
          tableName: 'test_product',
          filter: { createdAt: { _between: 'invalid-date,2024-01-15' } },
        }),
      ).rejects.toThrow('Invalid date value');
    });
  });

  describe('_between with complex queries', () => {
    it('should work with other filters', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: {
          _and: [
            { price: { _between: [200, 600] } },
            { rating: { _gte: 2.0 } },
          ],
        },
        sort: ['price'],
      });

      expect(result.data).toBeDefined();
      expect(
        result.data.every(
          (p) => p.price >= 200 && p.price <= 600 && p.rating >= 2.0,
        ),
      ).toBe(true);
    });

    it('should work with OR conditions', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: {
          _or: [
            { price: { _between: [100, 200] } },
            { rating: { _between: '2.5,3.0' } },
          ],
        },
        sort: ['price'],
      });

      expect(result.data).toBeDefined();
      expect(
        result.data.every(
          (p) =>
            (p.price >= 100 && p.price <= 200) ||
            (p.rating >= 2.5 && p.rating <= 3.0),
        ),
      ).toBe(true);
    });

    it('should work with multiple _between conditions', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: {
          price: { _between: [200, 800] },
          rating: { _between: '1.5,2.5' },
          createdAt: { _between: ['2024-01-05', '2024-01-15'] },
        },
        sort: ['price'],
      });

      expect(result.data).toBeDefined();
      expect(
        result.data.every(
          (p) =>
            p.price >= 200 &&
            p.price <= 800 &&
            p.rating >= 1.5 &&
            p.rating <= 2.5 &&
            new Date(p.createdAt) >= new Date('2024-01-05') &&
            new Date(p.createdAt) <= new Date('2024-01-15'),
        ),
      ).toBe(true);
    });

    it('should work with pagination', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { price: { _between: '100,1000' } },
        sort: ['price'],
        page: 2,
        limit: 5,
        meta: 'filterCount,totalCount',
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBe(5);
      // Products have prices: 50, 100, 150, ..., 1000 (20 products total)
      // _between '100,1000' includes 100 and 1000, so products 2-20 (19 products)
      expect(result.meta.filterCount).toBe(19); // Products 2-20 match
      expect(result.meta.totalCount).toBe(20);
    });
  });

  describe('_between with text fields', () => {
    it('should work with text comparisons (lexicographic)', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { name: { _between: ['Product 10', 'Product 19'] } },
        sort: ['name'],
      });

      expect(result.data).toBeDefined();
      // Lexicographic comparison: "Product 10" to "Product 19" includes 10-19
      expect(result.data.length).toBe(10);
    });

    it('should work with text comparisons (string format)', async () => {
      const result = await queryEngine.find({
        tableName: 'test_product',
        filter: { name: { _between: 'Product 10,Product 19' } },
        sort: ['name'],
      });

      expect(result.data).toBeDefined();
      expect(result.data.length).toBe(10);
    });
  });
});
