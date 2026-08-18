import { BadRequestException, ForbiddenException } from '../../../domain/exceptions';
import {
  rewriteFilterDenyingFields,
  rewriteSortDroppingDenied,
} from '@enfyra/kernel';
import type { EnfyraMetadata } from '../../../engines/cache/services/metadata-cache.service';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import {
  decideFieldPermission,
  fieldPermissionRuleAppliesToUser,
  fieldPermissionRuleMatchesSubject,
  formatFieldPermissionErrorMessage,
} from '../../../shared/utils/field-permission.util';

type ReadMetadataColumn = {
  name: string;
  isEncrypted?: boolean;
  isPrimary?: boolean;
  isPublished?: boolean;
};

type ReadMetadataRelation = {
  propertyName: string;
  targetTable?: string;
  targetTableName?: string;
  isPublished?: boolean;
};

type ReadMetadataTable = {
  columns?: ReadMetadataColumn[];
  relations?: ReadMetadataRelation[];
};

type ReadDeepEntry = {
  fields?: string | string[];
  filter?: unknown;
  sort?: string | string[];
  deep?: Record<string, ReadDeepEntry>;
};

type DynamicReadAuthorizationContext = {
  $user?: Record<string, unknown> | null;
  $query?: {
    filter?: unknown;
    sort?: unknown;
  };
};

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function getTableMetadata(
  metadata: EnfyraMetadata,
  tableName: string,
): ReadMetadataTable | null {
  const table = metadata.tables.get(tableName);
  return isRecord(table) ? (table as ReadMetadataTable) : null;
}

function getSort(value: unknown): string | string[] | undefined {
  if (typeof value === 'string') return value;
  return Array.isArray(value) && value.every((item) => typeof item === 'string')
    ? value
    : undefined;
}

export class DynamicReadAuthorizationService {
  constructor(
    private readonly deps: {
      runtimeRegistryService: RuntimeRegistryService;
    },
  ) {}

  async assertQueryAllowed({
    tableName,
    context,
    enforceFieldPermission,
  }: {
    tableName: string;
    context: DynamicReadAuthorizationContext;
    enforceFieldPermission: boolean;
  }): Promise<void> {
    if (
      !enforceFieldPermission ||
      context.$user?.isRootAdmin === true
    ) {
      return;
    }

    const metadata = this.deps.runtimeRegistryService.lookupTableByName(
      tableName,
    );
    const tableMetadata = isRecord(metadata)
      ? (metadata as ReadMetadataTable)
      : null;
    if (!tableMetadata) return;

    const policies =
      this.deps.runtimeRegistryService.getFieldPermissionPoliciesFor?.(
        context.$user,
        tableName,
        'read',
      ) ?? [];
    const queryColumns = new Set<string>();
    const queryRelations = new Set<string>();
    const checkColumn = (name: string) => {
      if (tableMetadata.columns?.some((column) => column.name === name)) {
        queryColumns.add(name);
      }
    };
    const checkRelation = (name: string) => {
      if (tableMetadata.relations?.some((relation) => relation.propertyName === name)) {
        queryRelations.add(name);
      }
    };
    const checkField = (name: string) => {
      checkColumn(name);
      checkRelation(name);
    };
    const walkFilter = (node: unknown): void => {
      if (Array.isArray(node)) {
        node.forEach(walkFilter);
        return;
      }
      if (!isRecord(node)) return;
      if (Array.isArray(node._and)) node._and.forEach(walkFilter);
      if (Array.isArray(node._or)) node._or.forEach(walkFilter);
      if (isRecord(node._not)) walkFilter(node._not);
      for (const key of Object.keys(node)) {
        if (key === '_and' || key === '_or' || key === '_not') continue;
        const [first] = key.split('.');
        if (key.includes('.') && first) checkRelation(first);
        else checkField(key);
      }
    };

    walkFilter(context.$query?.filter);
    const sort = context.$query?.sort;
    const sortTokens = Array.isArray(sort)
      ? sort.filter((item): item is string => typeof item === 'string')
      : typeof sort === 'string'
        ? sort.split(',').map((item) => item.trim()).filter(Boolean)
        : [];
    for (const token of sortTokens) {
      const field = token.startsWith('-') ? token.slice(1) : token;
      const [first] = field.split('.');
      if (!field) continue;
      if (field.includes('.') && first) checkRelation(first);
      else checkField(field);
    }

    const deniedFields: Array<{ type: 'column' | 'relation'; name: string }> = [];
    const checkQueryField = async (
      type: 'column' | 'relation',
      name: string,
    ): Promise<void> => {
      const subject =
        type === 'column'
          ? tableMetadata.columns?.find((column) => column.name === name)
          : tableMetadata.relations?.find(
              (relation) => relation.propertyName === name,
            );
      if (!subject) return;
      const rules = policies
        .flatMap((policy) => policy.rules ?? [])
        .filter((rule) => rule.isEnabled === true)
        .filter((rule) =>
          fieldPermissionRuleMatchesSubject(rule, {
            tableName,
            action: 'read',
            subjectType: type,
            subjectName: name,
          }),
        )
        .filter((rule) => fieldPermissionRuleAppliesToUser(rule, context.$user));
      if (rules.length === 0) {
        if (subject.isPublished === false) deniedFields.push({ type, name });
        return;
      }
      if (rules.some((rule) => rule.condition != null)) {
        deniedFields.push({ type, name });
        return;
      }
      const decision = await decideFieldPermission(
        this.deps.runtimeRegistryService,
        {
          user: context.$user,
          tableName,
          action: 'read',
          subjectType: type,
          subjectName: name,
          record: null,
        },
        { defaultAllowed: subject.isPublished !== false },
      );
      if (!decision.allowed) deniedFields.push({ type, name });
    };

    await Promise.all([
      ...[...queryColumns].map((name) => checkQueryField('column', name)),
      ...[...queryRelations].map((name) => checkQueryField('relation', name)),
    ]);

    if (deniedFields.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({
          action: 'filter',
          tableName,
          fields: deniedFields,
        }),
      );
    }
  }

  async stripDeniedFields({
    tableName,
    fields,
    deep,
    context,
    enforceFieldPermission,
  }: {
    tableName: string;
    fields: string | string[] | undefined;
    deep: Record<string, ReadDeepEntry> | undefined;
    context: DynamicReadAuthorizationContext;
    enforceFieldPermission: boolean;
  }): Promise<{
    fields: string | string[] | undefined;
    deep: Record<string, ReadDeepEntry> | undefined;
    needsPostSql: boolean;
  }> {
    if (!enforceFieldPermission) {
      return { fields, deep, needsPostSql: false };
    }

    const table = this.deps.runtimeRegistryService.lookupTableByName(tableName);
    const tableMetadata = isRecord(table) ? (table as ReadMetadataTable) : null;
    if (!tableMetadata) return { fields, deep, needsPostSql: false };

    let hasConditionalPending = false;
    const columns = new Set(
      (tableMetadata.columns ?? []).map((column) => column.name),
    );
    const relations = new Set(
      (tableMetadata.relations ?? []).map((relation) => relation.propertyName),
    );
    const isWildcard =
      !fields ||
      (typeof fields === 'string' && (fields === '' || fields === '*')) ||
      (Array.isArray(fields) && (fields.length === 0 || fields.includes('*')));
    const fieldList = isWildcard
      ? [...columns, ...relations]
      : typeof fields === 'string'
        ? fields
            .split(',')
            .map((item) => item.trim())
            .filter(Boolean)
        : [...fields];

    const columnsToCheck = new Set<string>();
    const relationsToCheck = new Set<string>();
    for (const field of fieldList) {
      const [first] = field.split('.');
      if (first && columns.has(first)) columnsToCheck.add(first);
      if (first && relations.has(first)) relationsToCheck.add(first);
    }
    for (const relationName of Object.keys(deep ?? {})) {
      if (relations.has(relationName)) relationsToCheck.add(relationName);
    }

    const deniedColumns = new Set<string>();
    for (const name of columnsToCheck) {
      const column = tableMetadata.columns?.find((item) => item.name === name);
      if (column?.isPrimary === true) continue;
      const defaultAllowed = column?.isPublished !== false;
      const decision = await this.decideReadField(
        tableName,
        'column',
        name,
        defaultAllowed,
        context,
      );
      if (!decision.allowed) {
        if (defaultAllowed) {
          deniedColumns.add(name);
        } else if (
          await this.hasConditionalRulesForField(
            tableName,
            'column',
            name,
            context,
          )
        ) {
          hasConditionalPending = true;
        } else {
          deniedColumns.add(name);
        }
      }
    }

    const deniedRelations = new Set<string>();
    for (const name of relationsToCheck) {
      const relation = tableMetadata.relations?.find(
        (item) => item.propertyName === name,
      );
      const defaultAllowed = relation?.isPublished !== false;
      const decision = await this.decideReadField(
        tableName,
        'relation',
        name,
        defaultAllowed,
        context,
      );
      if (!decision.allowed) {
        if (defaultAllowed) {
          deniedRelations.add(name);
        } else if (
          await this.hasConditionalRulesForField(
            tableName,
            'relation',
            name,
            context,
          )
        ) {
          hasConditionalPending = true;
        } else {
          deniedRelations.add(name);
        }
      }
    }

    const hasDenied = deniedColumns.size > 0 || deniedRelations.size > 0;
    const cleanFieldList = hasDenied
      ? fieldList.filter((field) => {
          const [first] = field.split('.');
          return !deniedColumns.has(first) && !deniedRelations.has(first);
        })
      : fieldList;
    const cleanFields =
      typeof fields === 'string' || isWildcard
        ? cleanFieldList.join(',')
        : cleanFieldList;
    const cleanDeep = deep ? { ...deep } : undefined;

    if (cleanDeep) {
      for (const relation of deniedRelations) delete cleanDeep[relation];
      for (const [relationName, entry] of Object.entries(cleanDeep)) {
        if (!isRecord(entry)) continue;
        const relation = tableMetadata.relations?.find(
          (item) => item.propertyName === relationName,
        );
        const targetTable = relation?.targetTableName ?? relation?.targetTable;
        if (!targetTable) continue;

        const nested = await this.stripDeniedFields({
          tableName: targetTable,
          fields: entry.fields,
          deep: entry.deep,
          context,
          enforceFieldPermission,
        });
        if (nested.needsPostSql) hasConditionalPending = true;

        let filter = entry.filter;
        let sort = entry.sort;
        if (context.$user?.isRootAdmin !== true) {
          const targetMetadata = this.deps.runtimeRegistryService.lookupTableByName(
            targetTable,
          );
          if (targetMetadata) {
            const metadata = this.deps.runtimeRegistryService.requireMetadata();
            if (filter) {
              filter = rewriteFilterDenyingFields(
                filter,
                targetTable,
                metadata,
                (currentTable, fieldName, type) =>
                  this.isPublishedReadField(currentTable, fieldName, type, metadata),
              );
            }
            if (sort) {
              sort = rewriteSortDroppingDenied(
                sort,
                targetTable,
                metadata,
                (currentTable, fieldName, type) =>
                  this.isPublishedReadField(currentTable, fieldName, type, metadata),
              );
            }
          }
        }

        cleanDeep[relationName] = {
          ...entry,
          ...(nested.fields !== entry.fields ? { fields: nested.fields } : {}),
          ...(nested.deep !== entry.deep ? { deep: nested.deep } : {}),
          ...(filter !== entry.filter ? { filter } : {}),
          ...(sort !== entry.sort ? { sort } : {}),
        };
      }
    }

    return {
      fields: cleanFields,
      deep: cleanDeep,
      needsPostSql: hasConditionalPending,
    };
  }

  private async decideReadField(
    tableName: string,
    subjectType: 'column' | 'relation',
    subjectName: string,
    defaultAllowed: boolean,
    context: DynamicReadAuthorizationContext,
  ) {
    return decideFieldPermission(
      this.deps.runtimeRegistryService,
      {
        user: context.$user,
        tableName,
        action: 'read',
        subjectType,
        subjectName,
        record: null,
      },
      { defaultAllowed },
    );
  }

  private async hasConditionalRulesForField(
    tableName: string,
    subjectType: 'column' | 'relation',
    subjectName: string,
    context: DynamicReadAuthorizationContext,
  ): Promise<boolean> {
    const policies =
      this.deps.runtimeRegistryService.getFieldPermissionPoliciesFor?.(
        context.$user,
        tableName,
        'read',
      ) ?? [];
    return policies.some((policy) =>
      (policy.rules ?? []).some(
        (rule) =>
          rule.condition != null &&
          rule.tableName === tableName &&
          rule.action === 'read' &&
          (subjectType === 'column'
            ? rule.columnName === subjectName
            : rule.relationPropertyName === subjectName),
      ),
    );
  }

  private isPublishedReadField(
    tableName: string,
    fieldName: string,
    subjectType: 'column' | 'relation',
    metadata: EnfyraMetadata,
  ): boolean {
    const table = getTableMetadata(metadata, tableName);
    if (!table) return true;
    const subject =
      subjectType === 'column'
        ? table.columns?.find((column) => column.name === fieldName)
        : table.relations?.find(
            (relation) => relation.propertyName === fieldName,
          );
    return subject?.isPublished !== false;
  }

  async assertAggregateFieldsAllowed(
    tableName: string,
    aggregate: unknown,
    context: DynamicReadAuthorizationContext,
    enforceFieldPermission: boolean,
  ): Promise<void> {
    if (!enforceFieldPermission || context.$user?.isRootAdmin === true) return;
    if (!isRecord(aggregate)) return;
    const metadata = this.deps.runtimeRegistryService.requireMetadata();
    const table = getTableMetadata(metadata, tableName);
    if (!table) return;
    const fields = new Set<string>();
    const walkFilter = (value: unknown): void => {
      if (Array.isArray(value)) {
        value.forEach(walkFilter);
        return;
      }
      if (!isRecord(value)) return;
      for (const [key, nested] of Object.entries(value)) {
        if (key === '_and' || key === '_or' || key === '_not') {
          walkFilter(nested);
        } else if (!key.startsWith('_')) {
          fields.add(key.split('.')[0]);
          walkFilter(nested);
        }
      }
    };
    walkFilter(aggregate.filter);
    const dimensions = Array.isArray(aggregate.dimensions) ? aggregate.dimensions : [];
    for (const dimension of dimensions) {
      if (isRecord(dimension) && typeof dimension.field === 'string') fields.add(dimension.field);
    }
    if (isRecord(aggregate.measures)) {
      for (const measure of Object.values(aggregate.measures)) {
        if (!isRecord(measure)) continue;
        for (const field of Object.values(measure)) {
          if (typeof field === 'string') fields.add(field);
        }
      }
    }
    if (Array.isArray(aggregate.sort)) {
      for (const sort of aggregate.sort) {
        if (isRecord(sort) && typeof sort.field === 'string') fields.add(sort.field);
      }
    }
    const denied: Array<{ type: 'column'; name: string }> = [];
    for (const field of fields) {
      const column = table.columns?.find((item) => item.name === field);
      if (!column) continue;
      const decision = await this.decideReadField(
        tableName,
        'column',
        field,
        column.isPublished !== false,
        context,
      );
      if (!decision.allowed) denied.push({ type: 'column', name: field });
    }
    if (denied.length > 0) {
      throw new ForbiddenException(
        formatFieldPermissionErrorMessage({
          action: 'filter',
          tableName,
          fields: denied,
        }),
      );
    }
  }

  async assertEncryptedQueryFieldsAllowed(
    tableName: string,
    filter: unknown,
    sort: string | string[] | undefined,
    deep: Record<string, ReadDeepEntry>,
  ): Promise<void> {
    const metadata = this.deps.runtimeRegistryService.requireMetadata();
    this.assertEncryptedFilterFields(tableName, filter, metadata);
    this.assertEncryptedSortFields(tableName, sort, metadata);

    const tableMetadata = getTableMetadata(metadata, tableName);
    for (const [relationName, entry] of Object.entries(deep)) {
      const relation = tableMetadata?.relations?.find(
        (item) => item.propertyName === relationName,
      );
      const targetTable = relation?.targetTableName ?? relation?.targetTable;
      if (!targetTable || !entry) continue;
      await this.assertEncryptedQueryFieldsAllowed(
        targetTable,
        entry.filter,
        getSort(entry.sort),
        entry.deep ?? {},
      );
    }
  }

  private assertEncryptedFilterFields(
    tableName: string,
    filter: unknown,
    metadata: EnfyraMetadata,
  ): void {
    if (!isRecord(filter)) {
      if (Array.isArray(filter)) {
        for (const item of filter) {
          this.assertEncryptedFilterFields(tableName, item, metadata);
        }
      }
      return;
    }

    const tableMetadata = getTableMetadata(metadata, tableName);
    for (const [key, value] of Object.entries(filter)) {
      if (key === '_and' || key === '_or' || key === '_not') {
        this.assertEncryptedFilterFields(tableName, value, metadata);
        continue;
      }
      if (key.startsWith('_')) continue;

      const relation = tableMetadata?.relations?.find(
        (item) => item.propertyName === key,
      );
      if (relation) {
        const targetTable = relation.targetTableName ?? relation.targetTable;
        if (targetTable) {
          this.assertEncryptedFilterFields(targetTable, value, metadata);
        }
        continue;
      }

      const column = tableMetadata?.columns?.find((item) => item.name === key);
      if (column?.isEncrypted === true) {
        throw new BadRequestException(
          `Encrypted field '${key}' on '${tableName}' cannot be used for filter.`,
        );
      }
    }
  }

  private assertEncryptedSortFields(
    tableName: string,
    sort: string | string[] | undefined,
    metadata: EnfyraMetadata,
  ): void {
    if (!sort) return;
    const tokens = Array.isArray(sort)
      ? sort
      : sort
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean);

    for (const token of tokens) {
      const path = token.startsWith('-') ? token.slice(1) : token;
      if (!path || path.startsWith('_count(')) continue;

      const parts = path.split('.');
      let currentTable = tableName;
      let field = parts[0];
      for (let index = 0; index < parts.length; index++) {
        field = parts[index];
        if (index === parts.length - 1) break;
        const relation = getTableMetadata(metadata, currentTable)?.relations?.find(
          (item) => item.propertyName === field,
        );
        if (!relation) break;
        currentTable = relation.targetTableName ?? relation.targetTable ?? currentTable;
      }

      const column = getTableMetadata(metadata, currentTable)?.columns?.find(
        (item) => item.name === field,
      );
      if (column?.isEncrypted === true) {
        throw new BadRequestException(
          `Encrypted field '${field}' on '${currentTable}' cannot be used for sort.`,
        );
      }
    }
  }
}
