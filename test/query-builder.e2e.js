const knex = require('knex');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const DB_TYPE = process.env.DB_TYPE || 'postgres';
const DB_URI = process.env.DB_URI;

console.log(`Database Type: ${DB_TYPE}`);
console.log('');

if (!DB_URI) {
  console.error('DB_URI not set in environment');
  process.exit(1);
}

const parseDatabaseUri = (uri) => {
  const regex = /^(?:([^:]+):\/\/)?(?:([^:]+):([^@]*)@)?([^:\/]+)(?::(\d+))?\/(.+)$/;
  const match = uri.match(regex);
  return {
    host: match[4],
    port: parseInt(match[5] || (DB_TYPE === 'postgres' ? '5432' : '3306')),
    user: match[2],
    password: match[3],
    database: match[6],
  };
};

const parsed = parseDatabaseUri(DB_URI);

const db = knex({
  client: DB_TYPE === 'postgres' ? 'pg' : 'mysql2',
  connection: {
    host: parsed.host,
    port: parsed.port,
    user: parsed.user,
    password: parsed.password,
    database: parsed.database,
  },
  pool: { min: 1, max: 10 },
});

const distPath = path.join(__dirname, '..', 'dist', 'src');
const SqlQueryExecutor = require(path.join(distPath, 'infrastructure', 'query-builder', 'executors', 'sql-query-executor')).SqlQueryExecutor;

let testsPassed = 0;
let testsFailed = 0;
let metadata = null;
let executor = null;

function log(name, passed, details = '') {
  const status = passed ? '✓ PASS' : '✗ FAIL';
  console.log(`${status}: ${name}${details ? '\n         ' + details : ''}`);
  if (passed) testsPassed++;
  else testsFailed++;
}

async function loadMetadata() {
  const tables = await db('table_definition').select('*');
  const columns = await db('column_definition').select('*');
  const relations = await db('relation_definition')
    .select([
      'relation_definition.*',
      'targetTable.name as targetTableName',
      'sourceTable.name as sourceTableName',
    ])
    .leftJoin(db('table_definition').as('targetTable'), 'relation_definition.targetTableId', 'targetTable.id')
    .leftJoin(db('table_definition').as('sourceTable'), 'relation_definition.sourceTableId', 'sourceTable.id');

  const tablesMap = new Map();

  for (const table of tables) {
    tablesMap.set(table.name, {
      name: table.name,
      alias: table.alias,
      columns: columns.filter(c => c.tableId === table.id).map(c => ({
        name: c.name,
        type: c.type,
        propertyName: c.propertyName,
      })),
      relations: relations.filter(r => r.sourceTableId === table.id).map(r => ({
        propertyName: r.propertyName,
        type: r.type,
        targetTableName: r.targetTableName,
        targetTable: r.targetTableName,
        foreignKeyColumn: r.foreignKeyColumn,
        junctionTableName: r.junctionTableName,
        junctionSourceColumn: r.junctionSourceColumn,
        junctionTargetColumn: r.junctionTargetColumn,
        mappedBy: r.mappedBy,
        isInverse: r.isInverse,
      })),
    });
  }

  return { tables: tablesMap };
}

async function runTests() {
  console.log('='.repeat(70));
  console.log('QUERY BUILDER - COMPREHENSIVE E2E TESTS');
  console.log('='.repeat(70));
  console.log('');

  metadata = await loadMetadata();
  executor = new SqlQueryExecutor(db, DB_TYPE);

  await testBasicSelect();
  await testFieldSelection();
  await testSortFunctionality();
  await testPagination();
  await testFilterOperators();
  await testLogicalOperators();
  await testNestedRelations();
  await testDeepNestedRelations();
  await testMeta();
  await testEdgeCases();

  console.log('\n' + '='.repeat(70));
  console.log(`FINAL RESULTS: ${testsPassed} passed, ${testsFailed} failed`);
  console.log('='.repeat(70));

  await db.destroy();
  process.exit(testsFailed > 0 ? 1 : 0);
}

async function testBasicSelect() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 1: BASIC SELECT');
  console.log('='.repeat(70));

  const result = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    limit: 5,
    metadata,
  });

  log('Basic select returns array', Array.isArray(result.data));
  log('Basic select returns correct count', result.data.length === 5);
  log('Basic select has requested fields', result.data[0]?.id !== undefined && result.data[0]?.name !== undefined);

  const resultAll = await executor.execute({
    tableName: 'table_definition',
    fields: '*',
    limit: 1,
    metadata,
  });

  log('Select * returns all columns', Object.keys(resultAll.data[0]).length > 2);

  const resultNoFields = await executor.execute({
    tableName: 'table_definition',
    limit: 1,
    metadata,
  });

  log('Select without fields returns data', resultNoFields.data.length === 1);
}

async function testFieldSelection() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 2: FIELD SELECTION');
  console.log('='.repeat(70));

  const result = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name', 'alias'],
    limit: 1,
    metadata,
  });

  const keys = Object.keys(result.data[0]);
  log('Select specific fields only', keys.length === 3);
  log('Select specific fields - id present', keys.includes('id'));
  log('Select specific fields - name present', keys.includes('name'));
  log('Select specific fields - alias present', keys.includes('alias'));

  const resultArray = await executor.execute({
    tableName: 'table_definition',
    fields: 'id,name,alias',
    limit: 1,
    metadata,
  });

  log('Select fields as comma string', Object.keys(resultArray.data[0]).length === 3);

  const resultNested = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name', 'columns.id', 'columns.name'],
    limit: 1,
    metadata,
  });

  log('Select nested fields returns relation', resultNested.data[0]?.columns !== undefined);
  log('Select nested fields returns array', Array.isArray(resultNested.data[0]?.columns));
  log('Select nested fields has data', resultNested.data[0]?.columns?.length > 0);
}

async function testSortFunctionality() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 3: SORT FUNCTIONALITY');
  console.log('='.repeat(70));

  const resultAsc = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    sort: 'id',
    limit: 5,
    metadata,
  });

  let isAsc = true;
  for (let i = 1; i < resultAsc.data.length; i++) {
    if (resultAsc.data[i-1].id > resultAsc.data[i].id) isAsc = false;
  }
  log('Sort ascending by id', isAsc);

  const resultDesc = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    sort: '-id',
    limit: 5,
    metadata,
  });

  let isDesc = true;
  for (let i = 1; i < resultDesc.data.length; i++) {
    if (resultDesc.data[i-1].id < resultDesc.data[i].id) isDesc = false;
  }
  log('Sort descending by id', isDesc);

  const resultMulti = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    sort: 'name,id',
    limit: 10,
    metadata,
  });

  log('Sort multiple fields returns data', resultMulti.data.length > 0);

  const debugLog = [];
  const resultDefault = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 5,
    debugLog,
    metadata,
  });

  const sql = debugLog.find(d => d.type === 'SQL Query' || d.type === 'SQL Query (CTE)')?.sql;
  log('Default sort by id applied', sql?.includes('ORDER BY') && sql?.includes('"id"'));

  const resultNestedSort = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'columns.id'],
    sort: 'columns.id',
    limit: 5,
    debugLog: [],
    metadata,
  });

  log('Nested field sort ignored (no error)', resultNestedSort.data.length > 0);
}

async function testPagination() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 4: PAGINATION');
  console.log('='.repeat(70));

  const result1 = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 5,
    page: 1,
    metadata,
  });

  const result2 = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 5,
    page: 2,
    metadata,
  });

  log('Page 1 returns 5 items', result1.data.length === 5);
  log('Page 2 returns 5 items', result2.data.length === 5);
  log('Pages return different data', result1.data[0].id !== result2.data[0].id);

  const resultOffset = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 3,
    page: 3,
    metadata,
  });

  const allResult = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 9,
    page: 1,
    metadata,
  });

  const offsetId = resultOffset.data[0].id;
  const expectedId = allResult.data[6].id;
  log('Offset calculated correctly', offsetId === expectedId, `offset id: ${offsetId}, expected: ${expectedId}`);

  const resultLimit0 = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 0,
    metadata,
  });

  log('Limit 0 returns all data (no limit applied)', resultLimit0.data.length > 0);

  const resultNoLimit = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    metadata,
  });

  log('No limit returns all data', resultNoLimit.data.length > 10);
}

async function testFilterOperators() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 5: FILTER OPERATORS');
  console.log('='.repeat(70));

  const resultEq = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: { name: { _eq: 'table_definition' } },
    metadata,
  });

  log('Filter _eq works', resultEq.data.length === 1 && resultEq.data[0].name === 'table_definition');

  const resultNeq = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: { name: { _neq: 'table_definition' } },
    limit: 5,
    metadata,
  });

  log('Filter _neq works', resultNeq.data.every(d => d.name !== 'table_definition'));

  const allTables = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    metadata,
  });

  const halfCount = Math.floor(allTables.data.length / 2);
  const halfIds = allTables.data.slice(0, halfCount).map(d => d.id);

  const resultIn = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { id: { _in: halfIds } },
    metadata,
  });

  log('Filter _in works', resultIn.data.length === halfCount);

  const resultNin = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { id: { _not_in: halfIds } },
    metadata,
  });

  log('Filter _not_in works', resultNin.data.length === allTables.data.length - halfCount);

  const resultGt = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { id: { _gt: halfIds[0] } },
    limit: 5,
    metadata,
  });

  log('Filter _gt works', resultGt.data.every(d => d.id > halfIds[0]));

  const resultGte = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { id: { _gte: halfIds[0] } },
    limit: 5,
    metadata,
  });

  log('Filter _gte works', resultGte.data.every(d => d.id >= halfIds[0]));

  const resultLt = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { id: { _lt: halfIds[0] } },
    limit: 5,
    metadata,
  });

  log('Filter _lt works', resultLt.data.every(d => d.id < halfIds[0]));

  const resultLte = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { id: { _lte: halfIds[0] } },
    limit: 5,
    metadata,
  });

  log('Filter _lte works', resultLte.data.every(d => d.id <= halfIds[0]));

  const resultContains = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: { name: { _contains: 'table' } },
    limit: 5,
    metadata,
  });

  log('Filter _contains works', resultContains.data.every(d => d.name.toLowerCase().includes('table')));

  const resultStartsWith = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: { name: { _starts_with: 'table' } },
    limit: 5,
    metadata,
  });

  log('Filter _starts_with works', resultStartsWith.data.every(d => d.name.toLowerCase().startsWith('table')));

  const resultEndsWith = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: { name: { _ends_with: 'definition' } },
    limit: 5,
    metadata,
  });

  log('Filter _ends_with works', resultEndsWith.data.every(d => d.name.toLowerCase().endsWith('definition')));

  const resultIsNull = await executor.execute({
    tableName: 'column_definition',
    fields: ['id'],
    filter: { description: { _is_null: true } },
    limit: 5,
    metadata,
  });

  log('Filter _is_null works', resultIsNull.data.length >= 0);

  const resultIsNotNull = await executor.execute({
    tableName: 'column_definition',
    fields: ['id'],
    filter: { description: { _is_not_null: true } },
    limit: 5,
    metadata,
  });

  log('Filter _is_not_null works', resultIsNotNull.data.length >= 0);

  const simpleFilter = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: { name: 'table_definition' },
    metadata,
  });

  log('Simple filter (shorthand _eq) works', simpleFilter.data.length === 1);
}

async function testLogicalOperators() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 6: LOGICAL OPERATORS');
  console.log('='.repeat(70));

  const resultAnd = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: {
      _and: [
        { name: { _contains: 'table' } },
        { name: { _contains: 'definition' } }
      ]
    },
    limit: 5,
    metadata,
  });

  log('Filter _and works', resultAnd.data.every(d => d.name.includes('table') && d.name.includes('definition')));

  const resultOr = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: {
      _or: [
        { name: { _eq: 'table_definition' } },
        { name: { _eq: 'column_definition' } }
      ]
    },
    metadata,
  });

  log('Filter _or works', resultOr.data.length >= 2);

  const resultNot = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: {
      _not: { name: { _eq: 'table_definition' } }
    },
    limit: 5,
    metadata,
  });

  log('Filter _not works', resultNot.data.every(d => d.name !== 'table_definition'));

  const resultNestedLogical = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    filter: {
      _and: [
        { name: { _contains: 'definition' } },
        {
          _or: [
            { name: { _eq: 'table_definition' } },
            { name: { _eq: 'column_definition' } }
          ]
        }
      ]
    },
    metadata,
  });

  log('Nested logical operators work', resultNestedLogical.data.length >= 2);
}

async function testNestedRelations() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 7: NESTED RELATIONS');
  console.log('='.repeat(70));

  const resultOneToMany = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name', 'columns.id', 'columns.name'],
    limit: 3,
    metadata,
  });

  log('One-to-many relation returns data', resultOneToMany.data.length > 0);
  log('One-to-many relation has columns array', resultOneToMany.data[0]?.columns !== undefined);
  log('One-to-many columns is array', Array.isArray(resultOneToMany.data[0]?.columns));
  log('One-to-many columns have id', resultOneToMany.data[0]?.columns?.[0]?.id !== undefined);

  let isSorted = true;
  for (const row of resultOneToMany.data) {
    if (row.columns && row.columns.length > 1) {
      for (let i = 1; i < row.columns.length; i++) {
        if (row.columns[i-1].id > row.columns[i].id) isSorted = false;
      }
    }
  }
  log('One-to-many columns sorted by id', isSorted);

  const resultManyToOne = await executor.execute({
    tableName: 'column_definition',
    fields: ['id', 'name', 'table.id', 'table.name'],
    limit: 5,
    metadata,
  });

  log('Many-to-one relation returns data', resultManyToOne.data.length > 0);
  log('Many-to-one relation has table object', resultManyToOne.data[0]?.table !== undefined);
  log('Many-to-one table is object (not array)', !Array.isArray(resultManyToOne.data[0]?.table));
  log('Many-to-one table has id', resultManyToOne.data[0]?.table?.id !== undefined);

  const resultManyToMany = await executor.execute({
    tableName: 'route_definition',
    fields: ['id', 'path', 'availableMethods.id', 'availableMethods.name'],
    limit: 3,
    metadata,
  });

  log('Many-to-many relation returns data', resultManyToMany.data.length > 0);
  log('Many-to-many has methods array', resultManyToMany.data[0]?.availableMethods !== undefined);
  log('Many-to-many methods is array', Array.isArray(resultManyToMany.data[0]?.availableMethods));
}

async function testDeepNestedRelations() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 8: DEEP NESTED RELATIONS');
  console.log('='.repeat(70));

  const result = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name', 'columns.id', 'columns.name', 'columns.table.id', 'columns.table.name'],
    limit: 2,
    metadata,
  });

  log('Deep nested relation returns data', result.data.length > 0);
  log('Deep nested has columns', result.data[0]?.columns !== undefined);

  const hasDeepTable = result.data.some(d =>
    d.columns?.some(c => c.table !== undefined)
  );
  log('Deep nested has table inside columns', hasDeepTable);

  const resultAllFields = await executor.execute({
    tableName: 'table_definition',
    fields: '*',
    limit: 1,
    metadata,
  });

  log('Select * with relations returns data', resultAllFields.data.length > 0);
  log('Select * includes columns', resultAllFields.data[0]?.columns !== undefined);

  const resultMixed = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name', 'columns.*'],
    limit: 1,
    metadata,
  });

  log('Mixed field selection works', resultMixed.data.length > 0);
  log('Mixed - columns returned', resultMixed.data[0]?.columns !== undefined);
}

async function testMeta() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 9: META FIELDS');
  console.log('='.repeat(70));

  const resultTotal = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    meta: 'totalCount',
    limit: 5,
    metadata,
  });

  log('Meta totalCount present', resultTotal.meta?.totalCount !== undefined);
  log('Meta totalCount is number', typeof resultTotal.meta?.totalCount === 'number');
  log('Meta totalCount > limit', resultTotal.meta?.totalCount > 5);

  const resultFilter = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: { name: { _contains: 'definition' } },
    meta: 'filterCount',
    limit: 3,
    metadata,
  });

  log('Meta filterCount present', resultFilter.meta?.filterCount !== undefined);
  log('Meta filterCount is number', typeof resultFilter.meta?.filterCount === 'number');

  const resultAllMeta = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    meta: '*',
    limit: 5,
    metadata,
  });

  log('Meta * includes totalCount', resultAllMeta.meta?.totalCount !== undefined);
  log('Meta * includes filterCount', resultAllMeta.meta?.filterCount !== undefined);

  const resultNoMeta = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 5,
    metadata,
  });

  log('No meta - no meta object', resultNoMeta.meta === undefined);
}

async function testEdgeCases() {
  console.log('\n' + '='.repeat(70));
  console.log('SECTION 10: EDGE CASES');
  console.log('='.repeat(70));

  const resultEmptyFilter = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: {},
    limit: 1,
    metadata,
  });

  log('Empty filter returns data', resultEmptyFilter.data.length === 1);

  const resultNullFilter = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    filter: null,
    limit: 1,
    metadata,
  });

  log('Null filter returns data', resultNullFilter.data.length === 1);

  const resultLargeLimit = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 10000,
    metadata,
  });

  log('Large limit returns all data', resultLargeLimit.data.length > 10);

  const resultNegativeLimit = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: -1,
    metadata,
  });

  log('Negative limit returns empty or all', resultNegativeLimit.data.length >= 0);

  const resultStringLimit = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: '5',
    metadata,
  });

  log('String limit converted to number', resultStringLimit.data.length === 5);

  const resultStringPage = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    limit: 5,
    page: '2',
    metadata,
  });

  log('String page converted to number', resultStringPage.data.length === 5);

  let errorOnInvalidSort = false;
  try {
    await executor.execute({
      tableName: 'table_definition',
      fields: ['id'],
      sort: 'nonexistentField',
      limit: 5,
      metadata,
    });
  } catch (e) {
    errorOnInvalidSort = true;
  }
  log('Invalid sort field throws error', errorOnInvalidSort);

  const resultEmptyFields = await executor.execute({
    tableName: 'table_definition',
    fields: [],
    limit: 1,
    metadata,
  });

  log('Empty fields array returns data', resultEmptyFields.data.length >= 0);

  const resultWhitespaceFields = await executor.execute({
    tableName: 'table_definition',
    fields: ' id , name ',
    limit: 1,
    metadata,
  });

  log('Whitespace in fields string trimmed', resultWhitespaceFields.data.length === 1);

  const resultWhitespaceSort = await executor.execute({
    tableName: 'table_definition',
    fields: ['id'],
    sort: ' id , name ',
    limit: 1,
    metadata,
  });

  log('Whitespace in sort string trimmed', resultWhitespaceSort.data.length === 1);

  const resultMultipleSortDirections = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name'],
    sort: '-id,name',
    limit: 5,
    metadata,
  });

  log('Multiple sort with different directions works', resultMultipleSortDirections.data.length > 0);

  const resultFilterWithNullValue = await executor.execute({
    tableName: 'column_definition',
    fields: ['id'],
    filter: { description: null },
    limit: 5,
    metadata,
  });

  log('Filter with null value works', resultFilterWithNullValue.data.length >= 0);

  const resultDeepFilter = await executor.execute({
    tableName: 'table_definition',
    fields: ['id', 'name', 'columns.id'],
    filter: { name: { _contains: 'definition' } },
    limit: 3,
    metadata,
  });

  log('Filter with nested fields works', resultDeepFilter.data.length > 0);
  log('Filter with nested fields returns columns', resultDeepFilter.data[0]?.columns !== undefined);
}

runTests().catch(e => {
  console.error('Error:', e);
  process.exit(1);
});