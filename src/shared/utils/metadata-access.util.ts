import type { FieldPermissionCacheService } from '../../engines/cache';
import type { PolicyService } from '../../domain/policy';
import { decideFieldPermission } from './field-permission.util';

type MetadataAccessDeps = {
  metadata: any;
  user: any;
  routeCacheService: { getRoutes: () => Promise<any[]> };
  policyService: PolicyService;
  fieldPermissionCacheService?: FieldPermissionCacheService;
};

type ProjectionDeps = MetadataAccessDeps & {
  tableName?: string;
};

type MetadataFieldAction = 'read' | 'create' | 'update';

function getRecordId(value: any): string | null {
  if (value === undefined || value === null) return null;
  return String(value?._id ?? value?.id ?? value);
}

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
    route?.availableMethods ??
    route?.publishedMethods ??
    route?.methods ??
    [];
  if (!Array.isArray(candidates)) return [];
  return candidates
    .map((item: any) => String(item?.method ?? item).toUpperCase())
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
    add('user_definition', ['read', 'update']);
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
  fieldPermissionCacheService?: FieldPermissionCacheService;
}): Promise<Record<MetadataFieldAction, boolean>> {
  const {
    user,
    tableName,
    subjectType,
    subjectName,
    subject,
    actions,
    fieldPermissionCacheService,
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

  if (!fieldPermissionCacheService) {
    for (const action of actions) {
      access[action] = subject?.isPublished !== false;
    }
    return access;
  }

  for (const action of actions) {
    const decision = await decideFieldPermission(
      fieldPermissionCacheService,
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

async function projectTable(
  table: any,
  user: any,
  actions: Set<MetadataFieldAction>,
  fieldPermissionCacheService?: FieldPermissionCacheService,
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
      fieldPermissionCacheService,
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
      fieldPermissionCacheService,
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
  };
}

export async function projectMetadataForUser({
  metadata,
  user,
  routeCacheService,
  policyService,
  fieldPermissionCacheService,
  tableName,
}: ProjectionDeps): Promise<any[] | any | null> {
  if (!metadata) return tableName ? null : [];
  if (user?.isRootAdmin) {
    if (tableName) return metadata.tables?.get?.(tableName) ?? null;
    return metadata.tablesList ?? [];
  }

  const accessibleActions = await getAccessibleMetadataTableActions({
    metadata,
    user,
    routeCacheService,
    policyService,
    fieldPermissionCacheService,
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
      fieldPermissionCacheService,
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
        fieldPermissionCacheService,
      ),
    );
  }
  return projected;
}
