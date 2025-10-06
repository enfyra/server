import * as path from 'path';
import * as fs from 'fs';
import { Logger } from '@nestjs/common';
import { createDataSource } from '../../../core/database/data-source/data-source';
import { CommonService } from '../../../shared/common/services/common.service';
import { DataSource } from 'typeorm';
const logger = new Logger('MigrationHelper');

// Helper function to clean up orphaned tables and constraints
async function cleanupOrphanedTables(dataSource: any) {
  try {
    logger.log('🧹 Checking for orphaned database tables...');

    const queryRunner = dataSource.createQueryRunner();
    const dbType = dataSource.options.type;

    // Get all tables in database
    let databaseTables: any[] = [];
    if (dbType === 'postgres') {
      databaseTables = await queryRunner.query(`
        SELECT tablename AS table_name
        FROM pg_tables
        WHERE schemaname = current_schema()
          AND tablename NOT IN ('migrations', 'schema_history')
      `);
    } else {
      databaseTables = await queryRunner.query(`
        SELECT TABLE_NAME AS table_name
        FROM information_schema.TABLES 
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_TYPE = 'BASE TABLE'
          AND TABLE_NAME NOT IN ('migrations', 'schema_history')
      `);
    }

    // Get entity table names from current entities
    const entityTableNames = dataSource.entityMetadatas.map(
      (meta: any) => meta.tableName,
    );

    // Find orphaned tables (exist in DB but not in entities)
    const orphanedTables = databaseTables.filter(
      (dbTable: any) => !entityTableNames.includes(dbTable.table_name),
    );

    if (orphanedTables.length > 0) {
      logger.warn(
        `Found ${orphanedTables.length} orphaned table(s) to clean up:`,
      );

      for (const table of orphanedTables) {
        const tableName = table.table_name;
        logger.warn(`  - ${tableName}`);

        try {
          if (dbType === 'postgres') {
            // Drop table with CASCADE in Postgres
            await queryRunner.query(`DROP TABLE IF EXISTS "${tableName}" CASCADE`);
            logger.log(`🗑️ Dropped orphaned table (PG): ${tableName}`);
          } else {
            // Drop all foreign keys referencing this table first (MySQL)
            const referencingFKs = await queryRunner.query(`
              SELECT DISTINCT TABLE_NAME, CONSTRAINT_NAME
              FROM information_schema.KEY_COLUMN_USAGE 
              WHERE CONSTRAINT_SCHEMA = DATABASE()
                AND REFERENCED_TABLE_NAME = '${tableName}'
                AND CONSTRAINT_NAME LIKE 'FK_%'
            `);

            for (const fk of referencingFKs) {
              try {
                await queryRunner.query(
                  `ALTER TABLE \`${fk.TABLE_NAME}\` DROP FOREIGN KEY \`${fk.CONSTRAINT_NAME}\``,
                );
                logger.debug(
                  `  → Dropped FK ${fk.CONSTRAINT_NAME} from ${fk.TABLE_NAME}`,
                );
              } catch (fkError) {
                logger.warn(
                  `  → Failed to drop FK ${fk.CONSTRAINT_NAME}: ${fkError.message}`,
                );
              }
            }

            // Drop foreign keys FROM this table
            const outgoingFKs = await queryRunner.query(`
              SELECT CONSTRAINT_NAME 
              FROM information_schema.KEY_COLUMN_USAGE 
              WHERE CONSTRAINT_SCHEMA = DATABASE()
                AND TABLE_NAME = '${tableName}'
                AND REFERENCED_TABLE_NAME IS NOT NULL
            `);

            for (const fk of outgoingFKs) {
              try {
                await queryRunner.query(
                  `ALTER TABLE \`${tableName}\` DROP FOREIGN KEY \`${fk.CONSTRAINT_NAME}\``,
                );
                logger.debug(`  → Dropped outgoing FK ${fk.CONSTRAINT_NAME}`);
              } catch (fkError) {
                logger.warn(
                  `  → Failed to drop outgoing FK ${fk.CONSTRAINT_NAME}: ${fkError.message}`,
                );
              }
            }

            // Drop the table
            await queryRunner.dropTable(tableName);
            logger.log(`🗑️ Dropped orphaned table: ${tableName}`);
          }
        } catch (dropError: any) {
          logger.error(
            `❌ Failed to drop table ${tableName}: ${dropError.message}`,
          );
        }
      }

      logger.log('✅ Orphaned table cleanup completed');
    } else {
      logger.debug('✅ No orphaned tables found');
    }

    await queryRunner.release();
  } catch (error: any) {
    logger.error('❌ Error during table cleanup:', error.message);
    // Don't throw - continue with migration even if cleanup fails
  }
}

export async function generateMigrationFile() {
  await generateMigrationFileDirect();
}

async function generateMigrationFileDirect() {
  const migrationDir = path.resolve(
    'dist',
    'src',
    'core',
    'database',
    'migrations',
    'AutoMigration',
  );
  const needDeleteDir = path.resolve(
    'dist',
    'src',
    'core',
    'database',
    'migrations',
  );
  const entityDir = path.resolve('dist', 'src', 'core', 'database', 'entities');

  logger.log('🚀 Generating migration using DataSource API...');

  try {
    // Clean up existing migrations
    if (fs.existsSync(needDeleteDir)) {
      fs.rmSync(needDeleteDir, { recursive: true, force: true });
      logger.log(`Successfully deleted directory ${needDeleteDir}`);
    }

    fs.mkdirSync(migrationDir, { recursive: true });
    logger.log(`Successfully created directory ${migrationDir}`);

    // Load entities and create DataSource
    const commonService = new CommonService();
    const entities = await commonService.loadDynamicEntities(entityDir);
    const dataSource = createDataSource(entities);

    await dataSource.initialize();
    logger.debug('✅ DataSource initialized for migration generation');

    // ✅ Clean up orphaned tables and constraints first
    await cleanupOrphanedTables(dataSource);

    // Use TypeORM's migration generator
    const sqlInMemory = await dataSource.driver.createSchemaBuilder().log();

    // Process queries to optimize column type changes and renames
    const optimizedUpQueries = [];
    const optimizedDownQueries = [];

    for (let i = 0; i < sqlInMemory.upQueries.length; i++) {
      const query = sqlInMemory.upQueries[i];
      const queryStr = query.query;

      // Check if this is a DROP COLUMN followed by ADD COLUMN for the same column (type change or rename)
      if (
        queryStr.includes('DROP COLUMN') &&
        i + 1 < sqlInMemory.upQueries.length
      ) {
        const nextQuery = sqlInMemory.upQueries[i + 1];
        const nextQueryStr = nextQuery.query;

        // Extract table and column names with more robust regex
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

          // Validate that we have valid names
          if (!tableName || !oldColumnName || !newColumnName) {
            logger.warn(
              `Invalid column names detected, skipping optimization: ${queryStr}`,
            );
            optimizedUpQueries.push(query);
            if (sqlInMemory.downQueries[i]) {
              optimizedDownQueries.push(sqlInMemory.downQueries[i]);
            }
            continue;
          }

          // Check if this is a valid DROP/ADD pair (same table, consecutive queries)
          const isConsecutivePair =
            queryStr.includes(`DROP COLUMN`) &&
            nextQueryStr.includes(`ADD`) &&
            dropMatch[1] === addMatch[1];

          if (!isConsecutivePair) {
            logger.debug(
              `Not a consecutive DROP/ADD pair, keeping original query`,
            );
            optimizedUpQueries.push(query);
            if (sqlInMemory.downQueries[i]) {
              optimizedDownQueries.push(sqlInMemory.downQueries[i]);
            }
            continue;
          }

          if (oldColumnName === newColumnName) {
            // Same column name - this is a type change, convert to MODIFY COLUMN
            logger.debug(
              `Converting DROP/ADD to MODIFY for ${tableName}.${oldColumnName}`,
            );

            // Handle different database types
            const dbType = dataSource.options.type;
            let modifyQuery: string;

            if (dbType === 'mysql') {
              modifyQuery = `ALTER TABLE \`${tableName}\` MODIFY COLUMN \`${oldColumnName}\` ${newDefinition}`;
            } else if (dbType === 'postgres') {
              modifyQuery = `ALTER TABLE "${tableName}" ALTER COLUMN "${oldColumnName}" TYPE ${newDefinition}`;
            } else {
              // Fallback to original queries for unsupported database types
              logger.warn(
                `Unsupported database type ${dbType} for column modification, using original queries`,
              );
              optimizedUpQueries.push(query);
              optimizedUpQueries.push(nextQuery);
              if (sqlInMemory.downQueries[i]) {
                optimizedDownQueries.push(sqlInMemory.downQueries[i]);
              }
              if (sqlInMemory.downQueries[i + 1]) {
                optimizedDownQueries.push(sqlInMemory.downQueries[i + 1]);
              }
              i++; // Skip the next query
              continue;
            }

            optimizedUpQueries.push({ query: modifyQuery });

            // For down query, we need to reverse the type change
            // This is a simplified approach - in production you'd want to track the old type
            optimizedDownQueries.push({
              query: `-- Reverse migration for ${tableName}.${oldColumnName} type change (manual intervention required)`,
            });
          } else {
            // Different column names - this is a rename, convert to RENAME COLUMN
            logger.debug(
              `Converting DROP/ADD to RENAME for ${tableName}.${oldColumnName} -> ${newColumnName}`,
            );

            // Handle different database types for RENAME COLUMN
            const dbType = dataSource.options.type;
            let renameQuery: string;
            let reverseRenameQuery: string;

            if (dbType === 'mysql') {
              // MySQL 8.0+ supports RENAME COLUMN
              renameQuery = `ALTER TABLE \`${tableName}\` RENAME COLUMN \`${oldColumnName}\` TO \`${newColumnName}\``;
              reverseRenameQuery = `ALTER TABLE \`${tableName}\` RENAME COLUMN \`${newColumnName}\` TO \`${oldColumnName}\``;
            } else if (dbType === 'postgres') {
              // PostgreSQL supports RENAME COLUMN
              renameQuery = `ALTER TABLE "${tableName}" RENAME COLUMN "${oldColumnName}" TO "${newColumnName}"`;
              reverseRenameQuery = `ALTER TABLE "${tableName}" RENAME COLUMN "${newColumnName}" TO "${oldColumnName}"`;
            } else {
              // For unsupported database types, use the original DROP/ADD approach
              logger.warn(
                `Unsupported database type ${dbType} for column rename, using original DROP/ADD approach`,
              );
              optimizedUpQueries.push(query);
              optimizedUpQueries.push(nextQuery);
              if (sqlInMemory.downQueries[i]) {
                optimizedDownQueries.push(sqlInMemory.downQueries[i]);
              }
              if (sqlInMemory.downQueries[i + 1]) {
                optimizedDownQueries.push(sqlInMemory.downQueries[i + 1]);
              }
              i++; // Skip the next query
              continue;
            }

            optimizedUpQueries.push({ query: renameQuery });
            optimizedDownQueries.push({ query: reverseRenameQuery });
          }

          i++; // Skip the next ADD query since we've handled it
          continue;
        }
      }

      // Keep original query if not a DROP/ADD pair
      optimizedUpQueries.push(query);
      if (sqlInMemory.downQueries[i]) {
        optimizedDownQueries.push(sqlInMemory.downQueries[i]);
      }
    }

    if (optimizedUpQueries.length === 0) {
      logger.warn('⏭️ No changes to generate migration. Skipping.');
      await dataSource.destroy();
      return;
    }

    // Use optimized queries instead of original
    sqlInMemory.upQueries = optimizedUpQueries;
    sqlInMemory.downQueries = optimizedDownQueries;

    // Generate migration file as JS (not TS) for direct execution
    const timestamp = Date.now();
    const migrationName = `AutoMigration${timestamp}`;
    const migrationPath = path.join(migrationDir, `${migrationName}.js`);

    const upQueries = sqlInMemory.upQueries
      .map((query) => {
        // Escape backticks, backslashes, and other problematic characters
        const escapedQuery = query.query
          .replace(/\\/g, '\\\\') // Escape backslashes first
          .replace(/`/g, '\\`') // Escape backticks
          .replace(/\${/g, '\\${'); // Escape template literal variables
        return `        await queryRunner.query(\`${escapedQuery}\`);`;
      })
      .join('\n');

    const downQueries = sqlInMemory.downQueries
      .map((query) => {
        // Escape backticks, backslashes, and other problematic characters
        const escapedQuery = query.query
          .replace(/\\/g, '\\\\') // Escape backslashes first
          .replace(/`/g, '\\`') // Escape backticks
          .replace(/\${/g, '\\${'); // Escape template literal variables
        return `        await queryRunner.query(\`${escapedQuery}\`);`;
      })
      .join('\n');

    const migrationTemplate = `const { MigrationInterface } = require("typeorm");

class ${migrationName}${timestamp} {
    name = '${migrationName}${timestamp}'

    async up(queryRunner) {
${upQueries}
    }

    async down(queryRunner) {
${downQueries}
    }
}

module.exports = { ${migrationName}${timestamp} };
`;

    fs.writeFileSync(migrationPath, migrationTemplate);
    logger.log(`✅ Migration file generated: ${migrationPath}`);

    await dataSource.destroy();
    logger.debug('✅ Migration file generation successful via DataSource API!');
  } catch (error: any) {
    logger.error('❌ Error in DataSource migration generation:', error);
    throw error;
  }
}

export async function runMigration() {
  await runMigrationDirect();
}

async function runMigrationDirect() {
  const entityDir = path.resolve('dist', 'src', 'core', 'database', 'entities');
  const migrationDir = path.resolve(
    'dist',
    'src',
    'core',
    'database',
    'migrations',
  );

  logger.log('🚀 Running migration using DataSource API...');

  try {
    // Load entities and create DataSource with proper migration path
    const commonService = new CommonService();
    const entities = await commonService.loadDynamicEntities(entityDir);

    // Create DataSource with explicit migration configuration
    const dataSource = new DataSource({
      type: process.env.DB_TYPE as 'mysql',
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT || '3306'),
      username: process.env.DB_USERNAME,
      password: process.env.DB_PASSWORD,
      database: process.env.DB_NAME,
      synchronize: false,
      entities,
      migrations: [path.resolve('dist', 'src', 'core', 'database', 'migrations', '**', '*.js')], // Look for JS files in dist
      migrationsRun: false, // Don't auto-run migrations
      logging: false,
    });

    await dataSource.initialize();
    logger.debug('✅ DataSource initialized for migration run');

    // Run pending migrations
    const migrations = await dataSource.runMigrations();

    if (migrations.length === 0) {
      logger.log('✅ No pending migrations to run');
    } else {
      logger.log(`✅ Successfully ran ${migrations.length} migration(s):`);
      migrations.forEach((migration) => {
        logger.log(`  - ${migration.name}`);
      });
    }

    await dataSource.destroy();

    if (fs.existsSync(migrationDir)) {
      fs.rmSync(migrationDir, { recursive: true, force: true });
      logger.log(`🧹 Cleaned up migration directory: ${migrationDir}`);
    }

    logger.debug('✅ Migration execution successful via DataSource API!');
  } catch (error) {
    logger.error('❌ Error in DataSource migration run:', error);
    throw error;
  }
}
