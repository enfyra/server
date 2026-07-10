import type { PolicyService } from '../../domain/policy';
import {
  decideFieldPermission,
  type TFieldPermissionPolicyReader,
} from './field-permission.util';

type MetadataAccessDeps = {
  metadata: any;
  user: any;
  routeCacheService: { getRoutes: () => Promise<any[]> };
  policyService: PolicyService;
  fieldPermissionPolicyReader?: TFieldPermissionPolicyReader;
};

type ProjectionDeps = MetadataAccessDeps & {
  tableName?: string;
};

type MetadataFieldAction = 'read' | 'create' | 'update';

const TIMESTAMP_FIELDS = [
  { name: 'createdAt', type: 'timestamp' },
  { name: 'updatedAt', type: 'timestamp' },
];

function getRouteTableName(route: any): string | null {
  const mainTable = route?.mainTable;
  if (typeof mainTable === 'string') return mainTable;
  return (
    mainTable?.name ??
    route?.mainTableName ??
    route?.tableName ??
    route?.mainTable?.tableName ??
    null
  );
}

function getRouteMethods(route: any): string[] {
  const candidates =
    route?.availableMethods ?? route?.publicMethods ?? route?.methods ?? [];
  if (!Array.isArray(candidates)) return [];
  return candidates
    .map((item: any) => String(item?.name ?? item).toUpperCase())
    .filter(Boolean);
}

function canAccessRouteMethod(
  route: any,
  method: string,
  user: any,
  policyService: PolicyService,
) {
  return policyService.checkRequestAccess({
    method,
    routeData: route,
    user,
  }).allow;
}

function actionsForMethod(method: string): MetadataFieldAction[] {
  switch (method.toUpperCase()) {
    case 'GET':
      return ['read'];
    case 'POST':
      return ['create'];
    case 'PATCH':
    case 'PUT':
      return ['update'];
    case 'DELETE':
      return ['read'];
    default:
      return ['read'];
  }
}

export async function getAccessibleMetadataTableActions({
  metadata,
  user,
  routeCacheService,
  policyService,
}: MetadataAccessDeps): Promise<Map<string, Set<MetadataFieldAction>>> {
  const tableActions = new Map<string, Set<MetadataFieldAction>>();
  const add = (tableName: string, actions: MetadataFieldAction[]) => {
    const set = tableActions.get(tableName) ?? new Set<MetadataFieldAction>();
    for (const action of actions) set.add(action);
    tableActions.set(tableName, set);
  };

  if (user?.isRootAdmin) {
    for (const table of metadata?.tablesList ?? []) {
      if (table?.name) add(table.name, ['read', 'create', 'update']);
    }
    return tableActions;
  }

  const routes = await routeCacheService.getRoutes();
  for (const route of routes) {
    const tableName = getRouteTableName(route);
    if (!tableName) continue;

    const methods = getRouteMethods(route);
    for (const method of methods) {
      if (canAccessRouteMethod(route, method, user, policyService)) {
        add(tableName, actionsForMethod(method));
      }
    }
  }

  if (user && !user.isAnonymous) {
    add('enfyra_user', ['read', 'update']);
  }

  return tableActions;
}

export async function getAccessibleMetadataTableNames({
  metadata,
  user,
  routeCacheService,
  policyService,
}: MetadataAccessDeps): Promise<Set<string>> {
  return new Set(
    (
      await getAccessibleMetadataTableActions({
        metadata,
        user,
        routeCacheService,
        policyService,
      })
    ).keys(),
  );
}

async function getSubjectAccess(params: {
  user: any;
  tableName: string;
  subjectType: 'column' | 'relation';
  subjectName: string;
  subject: any;
  actions: Set<MetadataFieldAction>;
  fieldPermissionPolicyReader?: TFieldPermissionPolicyReader;
}): Promise<Record<MetadataFieldAction, boolean>> {
  const {
    user,
    tableName,
    subjectType,
    subjectName,
    subject,
    actions,
    fieldPermissionPolicyReader,
  } = params;

  const access = {
    read: false,
    create: false,
    update: false,
  };

  if (user?.isRootAdmin) {
    for (const action of Object.keys(access) as MetadataFieldAction[]) {
      access[action] = true;
    }
    return access;
  }

  if (subjectType === 'column' && subject?.isPrimary) {
    access.read = true;
    return access;
  }

  if (!fieldPermissionPolicyReader) {
    for (const action of actions) {
      access[action] = subject?.isPublished !== false;
    }
    return access;
  }

  for (const action of actions) {
    const decision = await decideFieldPermission(
      fieldPermissionPolicyReader,
      {
        user,
        tableName,
        action,
        subjectType,
        subjectName,
        record: null,
      },
      { defaultAllowed: subject?.isPublished !== false },
    );
    access[action] = decision.allowed;
  }

  return access;
}

function hasAnyAccess(access: Record<MetadataFieldAction, boolean>) {
  return access.read || access.create || access.update;
}

function buildTableDefinition(columns: any[], relations: any[]) {
  const foreignKeyColumns = new Set(
    relations
      .filter(
        (relation) =>
          relation?.isInverse !== true &&
          !relation?.mappedBy &&
          !relation?.mappedById,
      )
      .map((relation) => relation.foreignKeyColumn)
      .filter((name): name is string => typeof name === 'string' && !!name),
  );

  const definition = columns
    .filter(
      (column) =>
        column?.name && !foreignKeyColumns.has(String(column.name)),
    )
    .map((column) => ({ ...column, fieldType: 'column' }));

  for (const field of TIMESTAMP_FIELDS) {
    if (definition.some((item) => item.name === field.name)) continue;
    definition.push({
      ...field,
      isNullable: false,
      isSystem: true,
      isUpdatable: false,
      isHidden: false,
      fieldType: 'column',
      isVirtual: true,
    });
  }

  for (const relation of relations) {
    if (!relation?.propertyName) continue;
    definition.push({
      ...relation,
      name: relation.propertyName,
      fieldType: 'relation',
      relationType: relation.type,
    });
  }

  return definition;
}

async function projectTable(
  table: any,
  user: any,
  actions: Set<MetadataFieldAction>,
  fieldPermissionPolicyReader?: TFieldPermissionPolicyReader,
) {
  const columns = [];
  for (const column of table?.columns ?? []) {
    const name = column?.name;
    if (!name) continue;
    const metadataAccess = await getSubjectAccess({
      user,
      tableName: table.name,
      subjectType: 'column',
      subjectName: name,
      subject: column,
      actions,
      fieldPermissionPolicyReader,
    });
    if (hasAnyAccess(metadataAccess)) {
      columns.push({ ...column, metadataAccess });
    }
  }

  const relations = [];
  for (const relation of table?.relations ?? []) {
    const name = relation?.propertyName;
    if (!name) continue;
    const metadataAccess = await getSubjectAccess({
      user,
      tableName: table.name,
      subjectType: 'relation',
      subjectName: name,
      subject: relation,
      actions,
      fieldPermissionPolicyReader,
    });
    if (hasAnyAccess(metadataAccess)) {
      relations.push({ ...relation, metadataAccess });
    }
  }

  return {
    ...table,
    metadataAccess: {
      read: actions.has('read'),
      create: actions.has('create'),
      update: actions.has('update'),
    },
    columns,
    relations,
    definition: buildTableDefinition(columns, relations),
  };
}

export async function projectMetadataForUser({
  metadata,
  user,
  routeCacheService,
  policyService,
  fieldPermissionPolicyReader,
  tableName,
}: ProjectionDeps): Promise<any[] | any | null> {
  if (!metadata) return tableName ? null : [];
  if (user?.isRootAdmin) {
    if (tableName) {
      const table = metadata.tables?.get?.(tableName) ?? null;
      if (!table) return null;
      return projectTable(
        table,
        user,
        new Set<MetadataFieldAction>(['read', 'create', 'update']),
        fieldPermissionPolicyReader,
      );
    }
    return metadata.tablesList ?? [];
  }

  const accessibleActions = await getAccessibleMetadataTableActions({
    metadata,
    user,
    routeCacheService,
    policyService,
    fieldPermissionPolicyReader,
  });

  const canSeeTable = (table: any) =>
    table?.name && accessibleActions.has(table.name);

  if (tableName) {
    const table = metadata.tables?.get?.(tableName) ?? null;
    if (!table || !canSeeTable(table)) return null;
    return projectTable(
      table,
      user,
      accessibleActions.get(table.name)!,
      fieldPermissionPolicyReader,
    );
  }

  const projected = [];
  for (const table of metadata.tablesList ?? []) {
    if (!canSeeTable(table)) continue;
    projected.push(
      await projectTable(
        table,
        user,
        accessibleActions.get(table.name)!,
        fieldPermissionPolicyReader,
      ),
    );
  }
  return projected;
}
