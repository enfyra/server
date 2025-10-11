import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from '../src/app.module';
import { QueryEngine } from '../src/infrastructure/query-engine/services/query-engine.service';

// Test scenarios covering different relation patterns
const testCases = [
  // Basic queries
  { table: 'user_definition', fields: ['*'], desc: 'Simple wildcard' },
  { table: 'user_definition', fields: ['id', 'name'], desc: 'Specific fields' },
  
  // Many-to-one
  { table: 'user_definition', fields: ['*', 'role.*'], desc: 'M2O relation' },
  { table: 'session_definition', fields: ['*', 'user.*'], desc: 'M2O with UUID FK' },
  
  // One-to-many
  { table: 'table_definition', fields: ['*', 'columns.*'], desc: 'O2M relation' },
  { table: 'table_definition', fields: ['*', 'relations.*'], desc: 'O2M relation' },
  { table: 'route_definition', fields: ['*', 'handlers.*'], desc: 'O2M relation' },
  
  // Many-to-many
  { table: 'route_definition', fields: ['*', 'publishedMethods.*'], desc: 'M2M relation' },
  { table: 'route_definition', fields: ['*', 'targetTables.*'], desc: 'M2M relation' },
  
  // Nested M2O
  { table: 'route_definition', fields: ['*', 'mainTable.*'], desc: 'Nested M2O' },
  { table: 'column_definition', fields: ['*', 'table.*'], desc: 'Nested M2O' },
  
  // Nested O2M
  { table: 'route_definition', fields: ['*', 'mainTable.columns.*'], desc: 'Nested O2M' },
  { table: 'route_definition', fields: ['*', 'mainTable.relations.*'], desc: 'Nested O2M' },
  
  // Deep nesting with wildcards
  { table: 'route_definition', fields: ['*', 'mainTable.columns.table.*'], desc: 'Deep nested wildcard' },
  { table: 'route_definition', fields: ['*', 'mainTable.relations.targetTable.*'], desc: 'Deep nested wildcard' },
  
  // Complex multi-relation
  { table: 'route_definition', fields: ['*', 'mainTable.*', 'publishedMethods.*', 'targetTables.*'], desc: 'Multiple relations' },
  { table: 'route_definition', fields: ['*', 'mainTable.columns.*', 'mainTable.relations.*'], desc: 'Multiple nested O2M' },
  
  // Edge cases
  { table: 'table_definition', fields: ['id'], desc: 'Only ID' },
  { table: 'table_definition', fields: ['*'], desc: 'Wildcard with auto-joins' },
  { table: 'user_definition', fields: ['*', 'role.name'], desc: 'Specific nested field' },
  
  // With filters
  { table: 'table_definition', fields: ['*'], filter: { isSystem: { _eq: 1 } }, desc: 'Filter by boolean' },
  { table: 'route_definition', fields: ['*'], filter: { isEnabled: { _eq: 1 } }, desc: 'Filter enabled' },
  
  // With sorting
  { table: 'table_definition', fields: ['*'], sort: ['name'], desc: 'Sort by name' },
  { table: 'route_definition', fields: ['*'], sort: ['-id'], desc: 'Sort DESC' },
  
  // With pagination
  { table: 'table_definition', fields: ['*'], limit: 5, desc: 'Limit 5' },
  { table: 'route_definition', fields: ['*'], limit: 3, page: 2, desc: 'Page 2' },
  
  // With meta
  { table: 'table_definition', fields: ['*'], meta: ['totalCount', 'filterCount'], desc: 'With meta' },
];

async function runStressTest() {
  console.log('🚀 Creating app...\n');
  const app = await NestFactory.createApplicationContext(AppModule, { logger: false });
  const qe = app.get(QueryEngine);
  
  const results = {
    passed: 0,
    failed: 0,
    errors: [] as any[],
  };
  
  console.log(`📝 Running ${testCases.length} test cases...\n`);
  
  for (let i = 0; i < testCases.length; i++) {
    const testCase = testCases[i];
    
    try {
      const result = await qe.find({
        tableName: testCase.table,
        fields: testCase.fields,
        filter: testCase.filter,
        sort: testCase.sort,
        limit: testCase.limit,
        page: testCase.page,
        meta: testCase.meta as any,
      });
      
      // Basic validation
      if (!result || !result.data) {
        throw new Error('Result is missing data property');
      }
      
      if (!Array.isArray(result.data)) {
        throw new Error('Result.data is not an array');
      }
      
      results.passed++;
      console.log(`✅ Test ${i + 1}/${testCases.length}: ${testCase.desc}`);
      
    } catch (error: any) {
      results.failed++;
      console.log(`❌ Test ${i + 1}/${testCases.length}: ${testCase.desc}`);
      results.errors.push({
        test: i + 1,
        ...testCase,
        error: error.message,
      });
    }
  }
  
  console.log(`\n📊 Results:`);
  console.log(`✅ Passed: ${results.passed}/${testCases.length}`);
  console.log(`❌ Failed: ${results.failed}/${testCases.length}`);
  
  if (results.errors.length > 0) {
    console.log(`\n❌ Failed tests:`);
    results.errors.forEach((err) => {
      console.log(`\n  Test #${err.test}: ${err.desc}`);
      console.log(`  Table: ${err.table}`);
      console.log(`  Fields: ${err.fields.join(', ')}`);
      console.log(`  Error: ${err.error}`);
    });
  } else {
    console.log('\n🎉 All tests passed!');
  }
  
  await app.close();
  process.exit(results.failed > 0 ? 1 : 0);
}

runStressTest().catch((error) => {
  console.error('❌ Fatal error:', error.message);
  console.error(error.stack);
  process.exit(1);
});

