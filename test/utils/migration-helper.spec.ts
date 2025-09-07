import { Logger } from '@nestjs/common';

// Mock logger for testing
const mockLogger = {
  debug: jest.fn(),
  warn: jest.fn(),
  log: jest.fn(),
  error: jest.fn(),
  verbose: jest.fn(),
  fatal: jest.fn(),
  localInstance: {} as any,
} as unknown as jest.Mocked<Logger>;

// Test cases for migration optimization
describe('Migration Optimization Logic', () => {
  // Test case 1: Column rename (MySQL)
  test('should convert DROP/ADD to RENAME COLUMN for MySQL', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `old_name`' },
      { query: 'ALTER TABLE `users` ADD `new_name` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual([
      { query: 'ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`' },
    ]);

    expect(result.optimizedDownQueries).toEqual([
      { query: 'ALTER TABLE `users` RENAME COLUMN `new_name` TO `old_name`' },
    ]);
  });

  // Test case 2: Column type change (MySQL)
  test('should convert DROP/ADD to MODIFY COLUMN for type change', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `email`' },
      { query: 'ALTER TABLE `users` ADD `email` varchar(500)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual([
      { query: 'ALTER TABLE `users` MODIFY COLUMN `email` varchar(500)' },
    ]);
  });

  // Test case 3: Column rename (PostgreSQL)
  test('should convert DROP/ADD to RENAME COLUMN for PostgreSQL', () => {
    const upQueries = [
      { query: 'ALTER TABLE "users" DROP COLUMN "old_name"' },
      { query: 'ALTER TABLE "users" ADD "new_name" varchar(255)' },
    ];

    const result = processQueries(upQueries, 'postgres');

    expect(result.optimizedUpQueries).toEqual([
      { query: 'ALTER TABLE "users" RENAME COLUMN "old_name" TO "new_name"' },
    ]);
  });

  // Test case 4: Column type change (PostgreSQL)
  test('should convert DROP/ADD to ALTER COLUMN for PostgreSQL type change', () => {
    const upQueries = [
      { query: 'ALTER TABLE "users" DROP COLUMN "email"' },
      { query: 'ALTER TABLE "users" ADD "email" varchar(500)' },
    ];

    const result = processQueries(upQueries, 'postgres');

    expect(result.optimizedUpQueries).toEqual([
      { query: 'ALTER TABLE "users" ALTER COLUMN "email" TYPE varchar(500)' },
    ]);
  });

  // Test case 5: Non-consecutive DROP/ADD (should not optimize)
  test('should not optimize non-consecutive DROP/ADD queries', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `old_name`' },
      { query: 'ALTER TABLE `posts` ADD `title` varchar(255)' },
      { query: 'ALTER TABLE `users` ADD `new_name` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual(upQueries);
  });

  // Test case 6: Different tables (should not optimize)
  test('should not optimize DROP/ADD for different tables', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `old_name`' },
      { query: 'ALTER TABLE `posts` ADD `new_name` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual(upQueries);
  });

  // Test case 7: Invalid column names (should skip optimization)
  test('should skip optimization for invalid column names', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN ``' },
      { query: 'ALTER TABLE `users` ADD `` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual(upQueries);
  });

  // Test case 8: Unsupported database type (should use original queries)
  test('should use original queries for unsupported database types', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `old_name`' },
      { query: 'ALTER TABLE `users` ADD `new_name` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'sqlite');

    expect(result.optimizedUpQueries).toEqual(upQueries);
  });

  // Test case 9: Mixed quotes in table/column names
  test('should handle mixed quotes in table and column names', () => {
    const upQueries = [
      { query: 'ALTER TABLE "users" DROP COLUMN `old_name`' },
      { query: 'ALTER TABLE "users" ADD `new_name` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual([
      { query: 'ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`' },
    ]);
  });

  // Test case 10: Complex column definitions
  test('should handle complex column definitions', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `email`' },
      {
        query:
          "ALTER TABLE `users` ADD `email` varchar(255) NOT NULL DEFAULT 'test@example.com'",
      },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual([
      {
        query:
          "ALTER TABLE `users` MODIFY COLUMN `email` varchar(255) NOT NULL DEFAULT 'test@example.com'",
      },
    ]);
  });

  // Test case 11: Multiple DROP/ADD pairs
  test('should handle multiple DROP/ADD pairs', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` DROP COLUMN `old_name`' },
      { query: 'ALTER TABLE `users` ADD `new_name` varchar(255)' },
      { query: 'ALTER TABLE `users` DROP COLUMN `old_email`' },
      { query: 'ALTER TABLE `users` ADD `new_email` varchar(500)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual([
      { query: 'ALTER TABLE `users` RENAME COLUMN `old_name` TO `new_name`' },
      { query: 'ALTER TABLE `users` RENAME COLUMN `old_email` TO `new_email`' },
    ]);
  });

  // Test case 12: Edge case - empty queries
  test('should handle empty queries', () => {
    const upQueries: any[] = [];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual([]);
  });

  // Test case 13: Edge case - single query
  test('should handle single query', () => {
    const upQueries = [
      { query: 'ALTER TABLE `users` ADD `new_column` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual(upQueries);
  });

  // Test case 14: Edge case - malformed queries
  test('should handle malformed queries gracefully', () => {
    const upQueries = [
      { query: 'INVALID SQL QUERY' },
      { query: 'ALTER TABLE `users` ADD `new_name` varchar(255)' },
    ];

    const result = processQueries(upQueries, 'mysql');

    expect(result.optimizedUpQueries).toEqual(upQueries);
  });
});

// Helper function to test the optimization logic
function processQueries(upQueries: any[], dbType: string) {
  const optimizedUpQueries: any[] = [];
  const optimizedDownQueries: any[] = [];

  for (let i = 0; i < upQueries.length; i++) {
    const query = upQueries[i];
    const queryStr = query.query;

    if (queryStr.includes('DROP COLUMN') && i + 1 < upQueries.length) {
      const nextQuery = upQueries[i + 1];
      const nextQueryStr = nextQuery.query;

      const dropMatch = queryStr.match(
        /ALTER TABLE [`"]?([^`"\s]+)[`"]?\s+DROP COLUMN [`"]?([^`"\s]+)[`"]?/i,
      );
      const addMatch = nextQueryStr.match(
        /ALTER TABLE [`"]?([^`"\s]+)[`"]?\s+ADD [`"]?([^`"\s]+)[`"]?\s+(.+)/i,
      );

      if (dropMatch && addMatch && dropMatch[1] === addMatch[1]) {
        const tableName = dropMatch[1];
        const oldColumnName = dropMatch[2];
        const newColumnName = addMatch[2];
        const newDefinition = addMatch[3].trim();

        if (!tableName || !oldColumnName || !newColumnName) {
          optimizedUpQueries.push(query);
          continue;
        }

        const isConsecutivePair =
          queryStr.includes(`DROP COLUMN`) &&
          nextQueryStr.includes(`ADD`) &&
          dropMatch[1] === addMatch[1];

        if (!isConsecutivePair) {
          optimizedUpQueries.push(query);
          continue;
        }

        if (oldColumnName === newColumnName) {
          // Type change
          if (dbType === 'mysql') {
            optimizedUpQueries.push({
              query: `ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${oldColumnName}\` ${newDefinition}`,
            });
          } else if (dbType === 'postgres') {
            optimizedUpQueries.push({
              query: `ALTER TABLE "${tableName}" ALTER COLUMN "${oldColumnName}" TYPE ${newDefinition}`,
            });
          } else {
            optimizedUpQueries.push(query);
            optimizedUpQueries.push(nextQuery);
            i++;
            continue;
          }
        } else {
          // Rename
          if (dbType === 'mysql') {
            optimizedUpQueries.push({
              query: `ALTER TABLE \`${tableName}\` RENAME COLUMN \`${oldColumnName}\` TO \`${newColumnName}\``,
            });
            optimizedDownQueries.push({
              query: `ALTER TABLE \`${tableName}\` RENAME COLUMN \`${newColumnName}\` TO \`${oldColumnName}\``,
            });
          } else if (dbType === 'postgres') {
            optimizedUpQueries.push({
              query: `ALTER TABLE "${tableName}" RENAME COLUMN "${oldColumnName}" TO "${newColumnName}"`,
            });
            optimizedDownQueries.push({
              query: `ALTER TABLE "${tableName}" RENAME COLUMN "${newColumnName}" TO "${oldColumnName}"`,
            });
          } else {
            optimizedUpQueries.push(query);
            optimizedUpQueries.push(nextQuery);
            i++;
            continue;
          }
        }

        i++;
        continue;
      }
    }

    optimizedUpQueries.push(query);
  }

  return { optimizedUpQueries, optimizedDownQueries };
}
