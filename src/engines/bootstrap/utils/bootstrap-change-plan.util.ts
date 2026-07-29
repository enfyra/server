import type {
  BootstrapChangePlan,
  BootstrapDefinition,
  BootstrapPlannedChange,
  BootstrapSchemaExecutionPlan,
} from '../types';

export const BOOTSTRAP_PROGRESS_CHANGE_IDS = Object.freeze({
  metadataProvision: 'metadata:provision',
  healingPhysicalPreflight: 'healing:physical-preflight',
  healingMetadata: 'healing:metadata',
  healingDerivedContracts: 'healing:derived-contracts',
  healingExplicitRepairs: 'healing:explicit-repairs',
  cacheWarm: 'cache:warm',
  defaultsSeed: 'defaults:seed',
  handlersEnsure: 'handlers:ensure',
  dataMigrate: 'data:migrate',
  attestationTarget: 'attestation:target',
  finalizeSetting: 'finalize:setting',
});

type UnweightedBootstrapChange = Omit<BootstrapPlannedChange, 'weight'>;

const PROGRESS_GROUP_WEIGHTS = Object.freeze({
  schema: 40,
  metadata: 15,
  healing: 10,
  cache: 15,
  data: 10,
  handlers: 5,
  publication: 5,
});

export function buildBootstrapChangePlan(
  schemaPlan: BootstrapSchemaExecutionPlan,
  definition: BootstrapDefinition,
): BootstrapChangePlan {
  const changes: UnweightedBootstrapChange[] = [];
  const add = (
    stage: BootstrapPlannedChange['stage'],
    id: string,
    label: string,
  ) => changes.push({ stage, id, label });

  if (schemaPlan.mode === 'install') {
    for (const tableName of Object.keys(definition.snapshot)) {
      add('schema', `schema:table:${tableName}`, `install ${tableName}`);
    }
  } else {
    for (const operation of schemaPlan.operations) {
      add('schema', operation.id, operation.label);
    }
  }

  add(
    'healing',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.healingPhysicalPreflight,
    'repair physical schema preflight',
  );
  add(
    'metadata',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.metadataProvision,
    'provision metadata',
  );
  add(
    'healing',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.healingMetadata,
    'heal system metadata',
  );
  add(
    'healing',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.healingDerivedContracts,
    'repair derived schema contracts',
  );
  add(
    'healing',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.healingExplicitRepairs,
    'apply explicit schema repairs',
  );
  add(
    'cache',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.cacheWarm,
    'warm metadata cache',
  );
  add(
    'defaults',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.defaultsSeed,
    'seed default data',
  );
  add(
    'handlers',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.handlersEnsure,
    'ensure route handlers',
  );

  const hasDataMigrations = Object.values(definition.dataMigration).some(
    (records) =>
      Array.isArray(records) ? records.length > 0 : Boolean(records),
  );
  if (hasDataMigrations) {
    add(
      'data',
      BOOTSTRAP_PROGRESS_CHANGE_IDS.dataMigrate,
      'apply data migrations',
    );
  }
  add(
    'attestation',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.attestationTarget,
    'attest target state',
  );
  add(
    'finalize',
    BOOTSTRAP_PROGRESS_CHANGE_IDS.finalizeSetting,
    'publish initialized version',
  );

  const progressGroup = (change: UnweightedBootstrapChange) => {
    if (change.stage === 'defaults' || change.stage === 'data') return 'data';
    if (change.stage === 'attestation' || change.stage === 'finalize') {
      return 'publication';
    }
    return change.stage;
  };
  const groupCounts = new Map<string, number>();
  for (const change of changes) {
    const group = progressGroup(change);
    groupCounts.set(group, (groupCounts.get(group) ?? 0) + 1);
  }
  const weightedChanges = changes.map((change): BootstrapPlannedChange => {
    const group = progressGroup(change);
    return Object.freeze({
      ...change,
      weight:
        PROGRESS_GROUP_WEIGHTS[group] / (groupCounts.get(group) as number),
    });
  });

  return Object.freeze({ changes: Object.freeze(weightedChanges) });
}
