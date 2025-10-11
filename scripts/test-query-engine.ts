import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { Logger } from '@nestjs/common';
import { AppModule } from '../src/app.module';
import { QueryEngine } from '../src/infrastructure/query-engine/services/query-engine.service';

async function testQueryEngine() {
  const logger = new Logger('TestQueryEngine');
  
  try {
    logger.log('🚀 Creating application context...');
    const app = await NestFactory.createApplicationContext(AppModule);
    const queryEngine = app.get(QueryEngine);

    logger.log('✅ Application context created\n');

    // Test 1: Simple query - select all tables
    logger.log('📝 Test 1: Simple query - Get all tables');
    try {
      const result1 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['id', 'name', 'isSystem'],
        limit: 5,
      });
      logger.log(`✅ Found ${result1.data.length} tables:`);
      result1.data.forEach((table: any) => {
        logger.log(`   - ${table.name} (ID: ${table.id}, System: ${table.isSystem})`);
      });
    } catch (error) {
      logger.error(`❌ Test 1 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 2: Query with filter
    logger.log('📝 Test 2: Query with filter - Get system tables');
    try {
      const result2 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['id', 'name'],
        filter: {
          isSystem: { _eq: true },
        },
        limit: 3,
      });
      logger.log(`✅ Found ${result2.data.length} system tables:`);
      result2.data.forEach((table: any) => {
        logger.log(`   - ${table.name}`);
      });
    } catch (error) {
      logger.error(`❌ Test 2 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 3: Query with sorting
    logger.log('📝 Test 3: Query with sorting - Get tables sorted by name DESC');
    try {
      const result3 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['name'],
        sort: ['-name'], // DESC
        limit: 5,
      });
      logger.log(`✅ Found ${result3.data.length} tables (sorted):`);
      result3.data.forEach((table: any) => {
        logger.log(`   - ${table.name}`);
      });
    } catch (error) {
      logger.error(`❌ Test 3 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 4: Query with relation (many-to-one)
    logger.log('📝 Test 4: Query with many-to-one relation - Get columns with table info');
    try {
      const result4 = await queryEngine.find({
        tableName: 'column_definition',
        fields: ['id', 'name', 'type', 'table.name'],
        limit: 5,
      });
      logger.log(`✅ Found ${result4.data.length} columns with table info:`);
      result4.data.forEach((col: any) => {
        logger.log(`   - ${col.name} (${col.type}) from table: ${col.table?.name || 'N/A'}`);
      });
    } catch (error) {
      logger.error(`❌ Test 4 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 5: Query with many-to-many relation
    logger.log('📝 Test 5: Query with many-to-many relation - Get route with target tables');
    try {
      const result5 = await queryEngine.find({
        tableName: 'route_definition',
        fields: ['id', 'path', 'targetTables.name'],
        limit: 3,
      });
      logger.log(`✅ Found ${result5.data.length} routes with target tables:`);
      result5.data.forEach((route: any) => {
        logger.log(`   - ${route.path}:`);
        if (route.targetTables && route.targetTables.length > 0) {
          route.targetTables.forEach((table: any) => {
            logger.log(`     - ${table.name}`);
          });
        } else {
          logger.log(`     (no target tables)`);
        }
      });
    } catch (error) {
      logger.error(`❌ Test 5 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 6: Query with meta (totalCount, filterCount)
    logger.log('📝 Test 6: Query with meta - Get totalCount and filterCount');
    try {
      const result6 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['id', 'name'],
        filter: {
          isSystem: { _eq: true },
        },
        meta: '*',
        limit: 2,
      });
      logger.log(`✅ Meta info:`);
      logger.log(`   - Total count: ${result6.meta?.totalCount}`);
      logger.log(`   - Filter count: ${result6.meta?.filterCount}`);
      logger.log(`   - Data returned: ${result6.data.length}`);
    } catch (error) {
      logger.error(`❌ Test 6 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 7: Query with pagination
    logger.log('📝 Test 7: Query with pagination - Page 2 of tables');
    try {
      const result7 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['id', 'name'],
        page: 2,
        limit: 3,
      });
      logger.log(`✅ Found ${result7.data.length} tables (page 2):`);
      result7.data.forEach((table: any) => {
        logger.log(`   - ${table.name}`);
      });
    } catch (error) {
      logger.error(`❌ Test 7 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 8: Query with _contains operator
    logger.log('📝 Test 8: Query with _contains operator - Find tables with "definition" in name');
    try {
      const result8 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['name'],
        filter: {
          name: { _contains: 'definition' },
        },
        limit: 5,
      });
      logger.log(`✅ Found ${result8.data.length} tables:`);
      result8.data.forEach((table: any) => {
        logger.log(`   - ${table.name}`);
      });
    } catch (error) {
      logger.error(`❌ Test 8 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 9: Query with _in operator
    logger.log('📝 Test 9: Query with _in operator - Get specific tables by ID');
    try {
      const result9 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['id', 'name'],
        filter: {
          id: { _in: [1, 2, 3] },
        },
      });
      logger.log(`✅ Found ${result9.data.length} tables:`);
      result9.data.forEach((table: any) => {
        logger.log(`   - ID ${table.id}: ${table.name}`);
      });
    } catch (error) {
      logger.error(`❌ Test 9 failed: ${error.message}`);
    }

    console.log('\n');

    // Test 10: Complex query with _and, _or
    logger.log('📝 Test 10: Complex query with _and, _or - Get system tables OR tables with "user" in name');
    try {
      const result10 = await queryEngine.find({
        tableName: 'table_definition',
        fields: ['name', 'isSystem'],
        filter: {
          _or: [
            { isSystem: { _eq: true } },
            { name: { _contains: 'user' } },
          ],
        },
        limit: 5,
      });
      logger.log(`✅ Found ${result10.data.length} tables:`);
      result10.data.forEach((table: any) => {
        logger.log(`   - ${table.name} (System: ${table.isSystem})`);
      });
    } catch (error) {
      logger.error(`❌ Test 10 failed: ${error.message}`);
    }

    console.log('\n');
    logger.log('🎉 All tests completed!');

    await app.close();
    process.exit(0);
  } catch (error) {
    logger.error('❌ Test failed:', error);
    process.exit(1);
  }
}

testQueryEngine();

