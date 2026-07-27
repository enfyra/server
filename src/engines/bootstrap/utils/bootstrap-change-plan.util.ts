import type {
  BootstrapChangePlan,
  BootstrapDefinition,
  BootstrapPlannedChange,
  BootstrapSchemaExecutionPlan,
} from '../types';

export function buildBootstrapChangePlan(
  schemaPlan: BootstrapSchemaExecutionPlan,
  definition: BootstrapDefinition,
): BootstrapChangePlan {
  const changes: BootstrapPlannedChange[] = [];
  const add = (
    stage: BootstrapPlannedChange['stage'],
    id: string,
    label: string,
  ) => changes.push({ stage, id: `${stage}:${id}`, label });

  if (schemaPlan.mode === 'install') {
    for (const tableName of Object.keys(definition.snapshot)) {
      add('schema', `table:${tableName}`, `install ${tableName}`);
    }
  } else {
    for (const operation of schemaPlan.operations) {
      changes.push({
        stage: 'schema',
        id: operation.id,
        label: operation.label,
      });
    }
  }
  if (schemaPlan.mode === 'install') {
    for (const [tableName, records] of Object.entries(definition.defaultData)) {
      const list = Array.isArray(records) ? records : [records];
      list.forEach((_, index) =>
        add('defaults', `${tableName}:${index}`, `seed ${tableName}`),
      );
    }
  }
  const routeRecords = definition.defaultData.enfyra_route;
  const routeCount = Array.isArray(routeRecords)
    ? routeRecords.length
    : routeRecords
      ? 1
      : 0;
  if (schemaPlan.mode === 'install') {
    for (let index = 0; index < routeCount; index++) {
      add('handlers', `route:${index}`, 'ensure route handlers');
    }
  }
  for (const [tableName, records] of Object.entries(definition.dataMigration)) {
    if (tableName.startsWith('_')) {
      const count = Array.isArray(records) ? records.length : records ? 1 : 0;
      for (let index = 0; index < count; index++) {
        add('data', `${tableName}:${index}`, `apply ${tableName}`);
      }
      continue;
    }
    const list = Array.isArray(records) ? records : [records];
    list.forEach((_, index) =>
      add('data', `${tableName}:${index}`, `migrate ${tableName}`),
    );
  }
  add('attestation', 'target', 'attest target state');
  add('finalize', 'setting', 'publish initialized version');

  for (const change of changes) Object.freeze(change);
  return Object.freeze({ changes: Object.freeze(changes) });
}
