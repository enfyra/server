import type { QueryBuilderService } from '@enfyra/kernel';
import { DatabaseConfigService } from '../../../shared/services';
import {
  compileScriptSource,
  normalizeScriptLanguage,
} from '../../../shared/utils/script-code.util';
import type { Logger } from '../../../shared/logger';

const LEGACY_STEP_TYPES = [
  'query',
  'create',
  'update',
  'delete',
  'http',
  'trigger_flow',
  'sleep',
  'log',
] as const;

const SCRIPT_STEP_TYPES = ['script', 'condition'];

const SAFE_TABLE_NAME = /^[a-z0-9_]+$/;

const STEP_TABLE = 'enfyra_flow_step';

interface LegacyStepRow {
  id?: number | string;
  _id?: unknown;
  key?: string;
  type?: string;
  config?: unknown;
  sourceCode?: string | null;
}

interface StepConversion {
  step: LegacyStepRow;
  patch: {
    sourceCode: string;
    scriptLanguage: string;
    compiledCode: string | null;
    type?: 'script';
  };
}

function parseConfig(value: unknown): Record<string, any> {
  if (value && typeof value === 'object') return value as Record<string, any>;
  if (typeof value === 'string' && value.trim() !== '') {
    try {
      const parsed = JSON.parse(value);
      return parsed && typeof parsed === 'object' ? parsed : {};
    } catch {
      return {};
    }
  }
  return {};
}

function requireTableName(
  config: Record<string, any>,
  step: LegacyStepRow,
): string {
  const table = config.table;
  if (typeof table !== 'string' || !SAFE_TABLE_NAME.test(table)) {
    throw new Error(
      `Legacy flow step ${step.key ?? step.type} (${step.type}) has no valid table name in config; convert it to a script step manually`,
    );
  }
  return table;
}

function toJson(value: unknown): string {
  return JSON.stringify(value === undefined ? null : value);
}

function buildLegacySource(step: LegacyStepRow): string {
  const config = parseConfig(step.config);

  switch (step.type) {
    case 'sleep': {
      const rawMs = Number(config.ms);
      const requested = Number.isFinite(rawMs) && rawMs > 0 ? rawMs : 1000;
      const ms = Math.min(requested, 60000);
      return [
        `const totalMs = ${ms}`,
        'for (let remaining = totalMs; remaining > 0; remaining -= 30000) {',
        '  await @HELPERS.$sleep(Math.min(remaining, 30000))',
        '}',
        'return { slept: totalMs }',
      ].join('\n');
    }
    case 'log': {
      const message =
        typeof config.message === 'string' && config.message !== ''
          ? toJson(config.message)
          : `@FLOW_LAST == null ? '' : JSON.stringify(@FLOW_LAST)`;
      return `const message = ${message}\n@LOGS(message)\nreturn { logged: true, message }`;
    }
    case 'query': {
      const table = requireTableName(config, step);
      return `return await #${table}.find({ filter: ${toJson(config.filter)}, fields: ${toJson(config.fields)}, limit: ${toJson(config.limit)}, sort: ${toJson(config.sort)} })`;
    }
    case 'create': {
      const table = requireTableName(config, step);
      return `return await #${table}.create({ data: ${toJson(config.data)} })`;
    }
    case 'update': {
      const table = requireTableName(config, step);
      return `return await #${table}.update({ id: ${toJson(config.id)}, data: ${toJson(config.data)} })`;
    }
    case 'delete': {
      const table = requireTableName(config, step);
      return `return await #${table}.delete({ id: ${toJson(config.id)} })`;
    }
    case 'trigger_flow': {
      const target = config.flowId ?? config.flowName;
      if (target === undefined || target === null || target === '') {
        throw new Error(
          `Legacy flow step ${step.key ?? step.type} (trigger_flow) has no target flow; convert it to a script step manually`,
        );
      }
      const payload =
        config.payload === undefined
          ? '@FLOW_LAST || {}'
          : toJson(config.payload);
      return `return await @TRIGGER(${toJson(target)}, ${payload})`;
    }
    case 'http':
      throw new Error(
        `Legacy flow step ${step.key ?? step.type} (http) cannot be converted automatically: the fixed HTTP step enforced host restrictions that a script does not. Convert it to a script step manually before upgrading`,
      );
    default:
      throw new Error(
        `Legacy flow step ${step.key ?? '<unknown>'} has unsupported type ${String(step.type)}`,
      );
  }
}

function compilePatch(
  sourceCode: string,
  scriptLanguage: unknown,
): StepConversion['patch'] {
  const language = normalizeScriptLanguage(scriptLanguage);
  return {
    sourceCode,
    scriptLanguage: language,
    compiledCode: compileScriptSource(sourceCode, language),
  };
}

function buildConversion(step: LegacyStepRow): StepConversion | null {
  if (!step.type) {
    throw new Error(
      `Flow step ${step.key ?? '<unknown>'} has no type; script-only flow steps accept script or condition`,
    );
  }

  if ((LEGACY_STEP_TYPES as readonly string[]).includes(step.type)) {
    return {
      step,
      patch: {
        ...compilePatch(buildLegacySource(step), 'javascript'),
        type: 'script',
      },
    };
  }

  if (SCRIPT_STEP_TYPES.includes(step.type)) {
    const hasSource =
      typeof step.sourceCode === 'string' && step.sourceCode.trim() !== '';
    if (hasSource) return null;
    const config = parseConfig(step.config);
    const code = config.sourceCode ?? config.code;
    if (typeof code !== 'string' || code.trim() === '') return null;
    return { step, patch: compilePatch(code, config.scriptLanguage) };
  }

  throw new Error(
    `Flow step ${step.key ?? '<unknown>'} has unsupported type ${String(step.type)}; script-only flow steps accept script or condition`,
  );
}

async function loadConfigRowsSql(
  queryBuilderService: QueryBuilderService,
): Promise<LegacyStepRow[] | null> {
  const knex = queryBuilderService.getKnex();
  if (!(await knex.schema.hasTable(STEP_TABLE))) return null;
  // A peer instance may have already run this migration; nothing is left to convert.
  if (!(await knex.schema.hasColumn(STEP_TABLE, 'config'))) return null;
  const hasSourceColumn = await knex.schema.hasColumn(STEP_TABLE, 'sourceCode');
  if (!hasSourceColumn) {
    const counted = await knex(STEP_TABLE)
      .whereNotNull('config')
      .count({ count: '*' });
    const count = Number((counted as any[])[0]?.count ?? 0);
    if (count > 0) {
      throw new Error(
        `${STEP_TABLE} has no sourceCode column but ${count} step(s) still store config; this upgrade path is unsupported`,
      );
    }
    return [];
  }
  const rows: LegacyStepRow[] = await knex(STEP_TABLE)
    .whereNotNull('config')
    .select(['id', 'key', 'type', 'config', 'sourceCode']);
  return rows;
}

async function loadConfigRowsMongo(
  queryBuilderService: QueryBuilderService,
): Promise<LegacyStepRow[] | null> {
  const db = queryBuilderService.getMongoDb();
  const collections = await db
    .listCollections({ name: STEP_TABLE }, { nameOnly: true })
    .toArray();
  if (collections.length === 0) return null;
  return db
    .collection(STEP_TABLE)
    .find({ config: { $exists: true } })
    .project({ key: 1, type: 1, config: 1, sourceCode: 1 })
    .toArray() as Promise<LegacyStepRow[]>;
}

/**
 * Carries pre-script-only flow steps onto the script-only contract before the
 * `enfyra_flow_step` enum is narrowed and `config` is dropped: retired fixed
 * steps become equivalent scripts, and script/condition steps whose code still
 * lives in `config` get `sourceCode` backfilled. Runs inside the bootstrap unit
 * of work before metadata activation, so it reads and writes physical storage
 * directly and a failed conversion rolls back with the migration.
 */
export async function convertLegacyFlowSteps(
  queryBuilderService: QueryBuilderService,
  logger?: Logger,
): Promise<number> {
  const isMongo = DatabaseConfigService.instanceIsMongoDb();
  const rows = isMongo
    ? await loadConfigRowsMongo(queryBuilderService)
    : await loadConfigRowsSql(queryBuilderService);
  if (!rows || rows.length === 0) return 0;

  const conversions = rows
    .map((step) => buildConversion(step))
    .filter((entry): entry is StepConversion => entry !== null);
  if (conversions.length === 0) return 0;

  const describe = (step: LegacyStepRow) =>
    `${String(DatabaseConfigService.getRecordId(step))} (${step.key ?? step.type})`;

  if (isMongo) {
    const collection = queryBuilderService.getMongoDb().collection(STEP_TABLE);
    for (const { step, patch } of conversions) {
      await collection.updateOne(
        { _id: DatabaseConfigService.getRecordId(step) },
        { $set: patch, $unset: { config: '' } },
      );
      logger?.log(
        `Converted legacy flow step ${describe(step)} to a script step`,
      );
    }
    return conversions.length;
  }

  const knex = queryBuilderService.getKnex();
  for (const { step, patch } of conversions) {
    await knex(STEP_TABLE)
      .where('id', DatabaseConfigService.getRecordId(step))
      .update({ ...patch, config: null });
    logger?.log(
      `Converted legacy flow step ${describe(step)} to a script step`,
    );
  }
  return conversions.length;
}
