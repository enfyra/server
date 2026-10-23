import type {
  BootstrapSchemaCommand,
  BootstrapSchemaOperation,
} from '../../src/engines/bootstrap/types/bootstrap-definition.types';
import type {
  ExtendedKnex,
  ExtendedQueryBuilder,
} from '../../src/engines/knex/types/knex-extended.types';
import type {
  RuntimeSchemaLogicalChange,
  RuntimeSchemaMutationCommand,
} from '../../src/modules/table-management/types/runtime-schema-mutation.types';
import type {
  PhysicalPlanMongo,
  PhysicalPlanSql,
  RuntimeSchemaPhysicalPlanPayload,
} from '../../src/modules/table-management/types/runtime-schema-physical-plan.types';

declare function accept<T>(value: T): void;

declare const dropTable: Extract<BootstrapSchemaOperation, { kind: 'drop-table' }>;
declare const renameTable: Extract<BootstrapSchemaOperation, { kind: 'rename-table' }>;
declare const renamePhysical: Extract<BootstrapSchemaOperation, { kind: 'rename-physical-table' }>;
declare const change: RuntimeSchemaLogicalChange;
declare const sqlPlan: PhysicalPlanSql;
declare const mongoPlan: PhysicalPlanMongo;

accept<BootstrapSchemaCommand>({ backend: 'postgresql', kind: 'apply-physical-change', operation: dropTable });
accept<BootstrapSchemaCommand>({ backend: 'mysql', kind: 'apply-metadata-change', operation: dropTable });
accept<BootstrapSchemaCommand>({ backend: 'mongodb', kind: 'cleanup-renamed-table', operation: renameTable });
accept<BootstrapSchemaCommand>({ backend: 'postgresql', kind: 'rename-physical-table', operation: renamePhysical });

// @ts-expect-error Table cleanup requires a rename operation, not a drop.
accept<BootstrapSchemaCommand>({ backend: 'mysql', kind: 'cleanup-renamed-table', operation: dropTable });
// @ts-expect-error A physical table rename cannot be dispatched as metadata mutation.
accept<BootstrapSchemaCommand>({ backend: 'mongodb', kind: 'apply-metadata-change', operation: renamePhysical });
// @ts-expect-error Direct rename commands must retain their matching logical operation.
accept<BootstrapSchemaCommand>({ backend: 'mysql', kind: 'rename-table', operation: dropTable });

function narrowBootstrapCommand(command: BootstrapSchemaCommand): void {
  if (command.kind === 'rename-table') {
    accept<string>(command.operation.rename.to);
    // @ts-expect-error Rename operations do not carry drop-table payloads.
    accept<string>(command.operation.tableName);
  }
}

accept<RuntimeSchemaMutationCommand>({ kind: 'apply-physical-change', change, physicalPlan: sqlPlan });
accept<RuntimeSchemaMutationCommand>({ kind: 'apply-physical-change', change, physicalPlan: mongoPlan });
accept<RuntimeSchemaMutationCommand>({ kind: 'apply-physical-change', change });
accept<RuntimeSchemaMutationCommand>({ kind: 'apply-metadata-change', change });
accept<RuntimeSchemaMutationCommand>({ kind: 'attest-source' });
// @ts-expect-error A per-change physical command must identify its logical change.
accept<RuntimeSchemaMutationCommand>({ kind: 'apply-physical-change', physicalPlan: sqlPlan });
// @ts-expect-error Metadata commands cannot carry a physical plan.
accept<RuntimeSchemaMutationCommand>({ kind: 'apply-metadata-change', change, physicalPlan: sqlPlan });
// @ts-expect-error Lifecycle commands cannot carry per-change payloads.
accept<RuntimeSchemaMutationCommand>({ kind: 'commit-database', change });
// @ts-expect-error Completion commands must identify the completed logical change.
accept<RuntimeSchemaMutationCommand>({ kind: 'complete-change' });

accept<RuntimeSchemaPhysicalPlanPayload>(sqlPlan);
accept<RuntimeSchemaPhysicalPlanPayload>(mongoPlan);
// @ts-expect-error SQL plans require complete up/down planning data.
accept<RuntimeSchemaPhysicalPlanPayload>({ backend: 'mysql' });
// @ts-expect-error Mongo plans require both forward and compensation diffs.
accept<RuntimeSchemaPhysicalPlanPayload>({ backend: 'mongodb', upDiff: {} });
// @ts-expect-error Mongo plans cannot carry SQL statement payloads.
accept<RuntimeSchemaPhysicalPlanPayload>({ backend: 'mongodb', upDiff: {}, downDiff: {}, upBatch: 'DROP TABLE item' });

function narrowPhysicalPlan(plan: RuntimeSchemaPhysicalPlanPayload): void {
  if (plan.backend === 'mongodb') {
    accept<unknown>(plan.upDiff);
    // @ts-expect-error SQL batches do not exist on Mongo plans.
    accept<string>(plan.upBatch);
  } else {
    accept<string>(plan.upBatch);
    // @ts-expect-error Mongo diffs do not exist on SQL plans.
    accept<unknown>(plan.upDiff);
  }
}

interface KnownRow {
  id: number;
  title: string;
}

declare const knex: ExtendedKnex;
declare const query: ExtendedQueryBuilder<KnownRow>;

async function typedKnexRows(): Promise<void> {
  const rows = await knex<KnownRow>('item');
  accept<KnownRow[]>(rows);
  for (const row of rows) {
    accept<string>(row.title);
    // @ts-expect-error Supplying a row contract must not default the query result to any.
    accept<number>(row.title);
    // @ts-expect-error Known rows do not expose arbitrary properties.
    accept<unknown>(row.missing);
  }
  const directRows = await query;
  for (const row of directRows) {
    // @ts-expect-error Direct extended builders preserve the supplied row contract.
    accept<boolean>(row.title);
  }
  const projectedRows = await knex<KnownRow, Array<{ title: string }>>('item');
  accept<Array<{ title: string }>>(projectedRows);
  const dynamicRows = await knex('dynamic_item');
  accept<unknown>(dynamicRows);
}

void narrowBootstrapCommand;
void narrowPhysicalPlan;
void typedKnexRows;
