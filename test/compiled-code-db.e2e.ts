import 'dotenv/config';
import assert from 'node:assert';
import knex, { type Knex } from 'knex';
import { RuntimeScriptRepairService } from '../src/engines/cache';
import { DatabaseConfigService } from '../src/shared/services';
import {
  SCRIPT_TABLE_LEGACY_FIELDS,
  SCRIPT_TABLE_NAMES,
} from '../src/shared/utils/script-table-contract.constants';
import {
  isExecutableJavaScript,
  normalizeScriptRecord,
} from '../src/shared/utils/script-code.util';

const FLOW_NAME = `e2e_compiled_code_repair_${Date.now()}`;
const STEP_KEY = 'repair_stale_compiled_code';
const SOURCE_CODE =
  'const value: string = @BODY.name || "missing"; return { value };';
const STALE_COMPILED_CODE =
  'const value: string = $ctx.$body.name || "missing"; return { value };';

function createKnex() {
  const uri = process.env.DB_URI;
  assert(uri, 'DB_URI is required');
  const client = uri.startsWith('postgres')
    ? 'pg'
    : uri.startsWith('mysql')
      ? 'mysql2'
      : uri.startsWith('sqlite')
        ? 'sqlite3'
        : null;
  assert(client, `Unsupported DB_URI protocol for E2E: ${uri.split(':')[0]}`);
  DatabaseConfigService.overrideForTesting(client === 'mysql2' ? 'mysql' : 'postgres');
  return knex({ client, connection: uri });
}

async function getColumns(db: Knex, table: string) {
  return new Set(Object.keys(await db(table).columnInfo()));
}

function pickColumns(data: Record<string, any>, columns: Set<string>) {
  return Object.fromEntries(
    Object.entries(data).filter(([key]) => columns.has(key)),
  );
}

async function insertReturning(db: Knex, table: string, data: Record<string, any>) {
  const columns = await getColumns(db, table);
  const payload = pickColumns(data, columns);
  if (db.client.config.client === 'pg') {
    const [row] = await db(table).insert(payload).returning('*');
    return row;
  }
  const [id] = await db(table).insert(payload);
  return await db(table).where({ id }).first();
}

async function scanDbScriptRecords(db: Knex) {
  let records = 0;
  let sourceRecords = 0;
  let compiledOnlyRecords = 0;

  for (const table of SCRIPT_TABLE_NAMES) {
    const columns = await getColumns(db, table);
    const legacyField = SCRIPT_TABLE_LEGACY_FIELDS[table];
    const fields = ['id', '_id', 'sourceCode', 'scriptLanguage', 'compiledCode'];
    if (legacyField) fields.push(legacyField);
    const existingFields = fields.filter((field) => columns.has(field));
    const rows = await db(table).select(existingFields).limit(1000);

    for (const row of rows) {
      records++;
      const normalized = normalizeScriptRecord(table, row);
      const sourceCode =
        typeof normalized.sourceCode === 'string' ? normalized.sourceCode : '';
      const compiledCode =
        typeof normalized.compiledCode === 'string'
          ? normalized.compiledCode
          : '';

      if (sourceCode) {
        sourceRecords++;
        assert(
          isExecutableJavaScript(compiledCode),
          `${table} source did not compile to executable JavaScript`,
        );
        continue;
      }

      if (compiledCode) {
        compiledOnlyRecords++;
        assert(
          isExecutableJavaScript(compiledCode),
          `${table} compiledCode is not executable JavaScript`,
        );
      }
    }
  }

  console.log(
    `Checked ${records} script records (${sourceRecords} source, ${compiledOnlyRecords} compiled-only)`,
  );
}

async function seedStaleFlowStep(db: Knex) {
  const now = new Date();
  const flow = await insertReturning(db, 'enfyra_flow', {
    name: FLOW_NAME,
    description: 'E2E compiledCode DB repair',
    triggerType: 'manual',
    triggerConfig: null,
    isEnabled: true,
    isSystem: false,
    timeout: 10000,
    maxExecutions: 10,
    createdAt: now,
    updatedAt: now,
  });
  const flowId = flow.id ?? flow._id;
  assert(flowId, 'flow id was not created');

  const step = await insertReturning(db, 'enfyra_flow_step', {
    flowId,
    key: STEP_KEY,
    stepOrder: 0,
    type: 'script',
    sourceCode: SOURCE_CODE,
    scriptLanguage: 'typescript',
    compiledCode: STALE_COMPILED_CODE,
    config: {},
    timeout: 5000,
    onError: 'stop',
    retryAttempts: 0,
    isEnabled: true,
    createdAt: now,
    updatedAt: now,
  });
  const stepId = step.id ?? step._id;
  assert(stepId, 'step id was not created');
  return { flowId, stepId };
}

async function cleanup(db: Knex, flowId?: string | number) {
  if (flowId == null) return;
  await db('enfyra_flow_step').where({ flowId }).delete();
  await db('enfyra_flow').where({ id: flowId }).delete();
}

async function main() {
  const db = createKnex();
  let flowId: string | number | undefined;

  try {
    await scanDbScriptRecords(db);
    const seeded = await seedStaleFlowStep(db);
    flowId = seeded.flowId;

    const staleStep = await db('enfyra_flow_step')
      .where({ id: seeded.stepId })
      .first();
    assert.strictEqual(staleStep.compiledCode, STALE_COMPILED_CODE);

    const repairService = new RuntimeScriptRepairService({
      queryBuilderService: {
        update: async (table: string, id: string | number, patch: any) => {
          await db(table).where({ id }).update(patch);
        },
      } as any,
    });
    await repairService.repairFlowStepScriptRecord(staleStep);

    const persisted = await db('enfyra_flow_step')
      .where({ id: seeded.stepId })
      .first();
    assert.notStrictEqual(persisted.compiledCode, STALE_COMPILED_CODE);
    assert(!persisted.compiledCode.includes(': string'));
    assert(persisted.compiledCode.includes('$ctx.$body.name'));

    console.log('CompiledCode DB repair E2E passed');
  } finally {
    await cleanup(db, flowId);
    await db.destroy();
  }
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
