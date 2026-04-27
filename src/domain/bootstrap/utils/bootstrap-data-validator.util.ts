import type {
  BootstrapDataFiles,
  BootstrapValidationIssue,
} from '../types/bootstrap-data-validator.types';

const ROUTE_METHOD_FIELDS = [
  'publishedMethods',
  'skipRoleGuardMethods',
  'availableMethods',
];
const PERMISSION_ACTIONS = new Set(['read', 'create', 'update', 'delete']);

function routePath(record: any) {
  return record.path ?? record._unique?.path?._eq;
}

function methodNames(defaultData: Record<string, any[]>) {
  return new Set(
    (defaultData.method_definition ?? [])
      .map((method: any) => method.method)
      .filter(Boolean),
  );
}

function tableNames(snapshot: Record<string, any>) {
  return new Set(Object.keys(snapshot).filter((name) => !name.startsWith('_')));
}

function recordName(record: any) {
  return record.name ?? record._unique?.name?._eq;
}

function nestedTableName(value: any) {
  if (typeof value === 'string') return value;
  return value?.name ?? value?._unique?.name?._eq ?? value?.name?._eq;
}

function collectPermissionRules(permission: any): any[] {
  if (!permission || typeof permission !== 'object') return [];
  const rules: any[] = [];
  if (permission.route || permission.actions) rules.push(permission);
  for (const key of ['and', 'or']) {
    if (Array.isArray(permission[key])) {
      for (const item of permission[key]) {
        rules.push(...collectPermissionRules(item));
      }
    }
  }
  return rules;
}

function validateRouteRefs(input: {
  file: BootstrapValidationIssue['file'];
  table: string;
  record: any;
  routes: Set<string>;
  methods: Set<string>;
  field?: string;
}) {
  const issues: BootstrapValidationIssue[] = [];
  const route = input.record.route ?? input.record.path;
  if (route && !input.routes.has(route)) {
    issues.push({
      file: input.file,
      table: input.table,
      path: route,
      field: input.field ?? 'route',
      message: `Route "${route}" is not defined in bootstrap route_definition.`,
    });
  }
  for (const field of ['methods', 'actions']) {
    if (input.record[field] === undefined) continue;
    if (!Array.isArray(input.record[field])) {
      issues.push({
        file: input.file,
        table: input.table,
        path: route,
        field,
        message: `${field} must be an array.`,
      });
      continue;
    }
    for (const method of input.record[field]) {
      const valid =
        field === 'actions'
          ? PERMISSION_ACTIONS.has(method)
          : input.methods.has(method);
      if (!valid) {
        issues.push({
          file: input.file,
          table: input.table,
          path: route,
          field,
          message:
            field === 'actions'
              ? `Permission action "${method}" is not supported.`
              : `Method "${method}" is not defined in method_definition.`,
        });
      }
    }
  }
  return issues;
}

function validateMenuPermission(input: {
  file: BootstrapValidationIssue['file'];
  record: any;
  routes: Set<string>;
  menuPaths: Set<string>;
  methods: Set<string>;
}) {
  const issues: BootstrapValidationIssue[] = [];
  for (const rule of collectPermissionRules(input.record.permission)) {
    issues.push(
      ...validateRouteRefs({
        file: input.file,
        table: 'menu_definition',
        record: rule,
        routes: new Set([...input.routes, ...input.menuPaths]),
        methods: input.methods,
        field: 'permission',
      }),
    );
  }
  return issues;
}

function validateGqlRecord(input: {
  file: BootstrapValidationIssue['file'];
  record: any;
  tables: Set<string>;
}) {
  const table = nestedTableName(input.record.table);
  if (!table || input.tables.has(table)) return [];
  return [
    {
      file: input.file,
      table: 'gql_definition',
      field: 'table',
      message: `GraphQL table "${table}" does not exist in snapshot.json.`,
    },
  ] satisfies BootstrapValidationIssue[];
}

function validateWebsocketEvent(input: {
  file: BootstrapValidationIssue['file'];
  record: any;
  gateways: Set<string>;
}) {
  const gateway = input.record.gateway ?? input.record.websocket ?? input.record.parent;
  const name = nestedTableName(gateway) ?? recordName(gateway) ?? gateway;
  if (!name || input.gateways.has(name)) return [];
  return [
    {
      file: input.file,
      table: 'websocket_event_definition',
      field: 'gateway',
      message: `WebSocket gateway "${name}" is not defined in websocket_definition.`,
    },
  ] satisfies BootstrapValidationIssue[];
}

function validateFlowStep(input: {
  file: BootstrapValidationIssue['file'];
  record: any;
  flows: Set<string>;
}) {
  const flow = input.record.flow ?? input.record.flowName;
  const name = recordName(flow) ?? flow;
  if (!name || input.flows.has(name)) return [];
  return [
    {
      file: input.file,
      table: 'flow_step_definition',
      field: 'flow',
      message: `Flow step references unknown flow "${name}".`,
    },
  ] satisfies BootstrapValidationIssue[];
}

function validateRouteRecord(input: {
  file: BootstrapValidationIssue['file'];
  record: any;
  methods: Set<string>;
  tables: Set<string>;
}) {
  const issues: BootstrapValidationIssue[] = [];
  const path = routePath(input.record);
  if (!path) {
    issues.push({
      file: input.file,
      table: 'route_definition',
      field: 'path',
      message: 'Route record must define path or _unique.path._eq.',
    });
  }

  if (input.record.mainTable && !input.tables.has(input.record.mainTable)) {
    issues.push({
      file: input.file,
      table: 'route_definition',
      path,
      field: 'mainTable',
      message: `Route mainTable "${input.record.mainTable}" does not exist in snapshot.json.`,
    });
  }

  for (const field of ROUTE_METHOD_FIELDS) {
    if (input.record[field] === undefined) continue;
    if (!Array.isArray(input.record[field])) {
      issues.push({
        file: input.file,
        table: 'route_definition',
        path,
        field,
        message: `${field} must be an array of method names.`,
      });
      continue;
    }

    for (const method of input.record[field]) {
      if (!input.methods.has(method)) {
        issues.push({
          file: input.file,
          table: 'route_definition',
          path,
          field,
          message: `Method "${method}" is not defined in default-data.json method_definition.`,
        });
      }
    }
  }

  return issues;
}

function validateUniqueRoutePaths(
  file: BootstrapValidationIssue['file'],
  routes: any[],
) {
  const issues: BootstrapValidationIssue[] = [];
  const seen = new Set<string>();
  for (const route of routes) {
    const path = routePath(route);
    if (!path) continue;
    if (seen.has(path)) {
      issues.push({
        file,
        table: 'route_definition',
        path,
        field: 'path',
        message: `Duplicate route path "${path}".`,
      });
    }
    seen.add(path);
  }
  return issues;
}

export function validateBootstrapDataFiles(input: BootstrapDataFiles) {
  const issues: BootstrapValidationIssue[] = [];
  const methods = methodNames(input.defaultData);
  const tables = tableNames(input.snapshot);
  const defaultRoutes = input.defaultData.route_definition ?? [];
  const migrationRoutes = input.dataMigration.route_definition ?? [];
  const routes = new Set([
    ...defaultRoutes.map(routePath).filter(Boolean),
    ...migrationRoutes.map(routePath).filter(Boolean),
  ]);
  const menuPaths = new Set([
    ...(input.defaultData.menu_definition ?? []).map(routePath).filter(Boolean),
    ...(input.dataMigration.menu_definition ?? []).map(routePath).filter(Boolean),
  ]);
  const gateways = new Set(
    (input.defaultData.websocket_definition ?? [])
      .map(recordName)
      .filter(Boolean),
  );
  const flows = new Set(
    (input.defaultData.flow_definition ?? [])
      .map(recordName)
      .filter(Boolean),
  );

  issues.push(...validateUniqueRoutePaths('default-data.json', defaultRoutes));
  issues.push(...validateUniqueRoutePaths('data-migration.json', migrationRoutes));

  for (const record of defaultRoutes) {
    issues.push(
      ...validateRouteRecord({
        file: 'default-data.json',
        record,
        methods,
        tables,
      }),
    );
  }

  for (const record of migrationRoutes) {
    issues.push(
      ...validateRouteRecord({
        file: 'data-migration.json',
        record,
        methods,
        tables,
      }),
    );
  }

  for (const file of ['default-data.json', 'data-migration.json'] as const) {
    const source =
      file === 'default-data.json' ? input.defaultData : input.dataMigration;

    for (const table of [
      'route_permission_definition',
      'route_handler_definition',
      'pre_hook_definition',
      'post_hook_definition',
    ]) {
      for (const record of source[table] ?? []) {
        issues.push(
          ...validateRouteRefs({
            file,
            table,
            record,
            routes,
            methods,
          }),
        );
      }
    }

    for (const record of source.menu_definition ?? []) {
      issues.push(
        ...validateMenuPermission({
          file,
          record,
          routes,
          menuPaths,
          methods,
        }),
      );
    }

    for (const record of source.gql_definition ?? []) {
      issues.push(...validateGqlRecord({ file, record, tables }));
    }

    for (const record of source.websocket_event_definition ?? []) {
      issues.push(...validateWebsocketEvent({ file, record, gateways }));
    }

    for (const record of source.flow_step_definition ?? []) {
      issues.push(...validateFlowStep({ file, record, flows }));
    }
  }

  return issues;
}
