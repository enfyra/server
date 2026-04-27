type BootstrapDataFiles = {
  snapshot: Record<string, any>;
  defaultData: Record<string, any[]>;
  dataMigration: Record<string, any>;
};

type BootstrapValidationIssue = {
  file: 'default-data.json' | 'data-migration.json';
  table: string;
  path?: string;
  field: string;
  message: string;
};

const ROUTE_METHOD_FIELDS = [
  'publishedMethods',
  'skipRoleGuardMethods',
  'availableMethods',
];

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

  return issues;
}

export type { BootstrapValidationIssue };
