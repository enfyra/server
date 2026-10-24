import {
  BadRequestException,
  ForbiddenException,
} from '../../../domain/exceptions';
import type { EnfyraMetadata } from '../../../engines/cache/services/metadata-cache.service';
import type { RuntimeRegistryService } from '../../../engines/cache/services/runtime-registry.service';
import { getFieldPermissionConditionFields } from '../../../shared/utils/field-permission-condition.util';
import { normalizeDynamicReadProjection } from '../utils/field-selection.util';
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
    if (!enforceFieldPermission) {
      return;
    }
    const isRoot = context.$user?.isRootAdmin === true;

    const checked = new Set<string>();
    const checkField = async (
      currentTable: string,
      name: string,
    ): Promise<string | undefined> => {
      const table = this.deps.runtimeRegistryService.lookupTableByName(
        currentTable,
      ) as ReadMetadataTable | null;
      const column = table?.columns?.find((item) => item.name === name);
      const relation = table?.relations?.find(
        (item) => item.propertyName === name,
      );
      if (!column && !relation) return;
      const type = relation ? 'relation' : 'column';
      const key = JSON.stringify([currentTable, type, name]);
      if (!checked.has(key)) {
        if (isRoot && (relation ?? column)?.isPublished !== false) {
          checked.add(key);
          return relation?.targetTableName ?? relation?.targetTable;
        }
        const conditional = await this.getConditionalFieldsForField(
          currentTable,
          type,
          name,
          context,
        );
        const decision = await this.decideReadField(
          currentTable,
          type,
          name,
          (relation ?? column)?.isPublished !== false,
          context,
        );
        if (conditional !== null || !decision.allowed) {
          throw new ForbiddenException(
            formatFieldPermissionErrorMessage({
              action: 'filter',
              tableName: currentTable,
              fields: [{ type, name }],
            }),
          );
        }
        checked.add(key);
      }
      return relation?.targetTableName ?? relation?.targetTable;
    };
    const checkPath = async (
      currentTable: string,
      path: string,
    ): Promise<void> => {
      const parts = path
        .split('.')
        .map((part) => part.trim())
        .filter(Boolean);
      for (const [index, part] of parts.entries()) {
        const target = await checkField(currentTable, part);
        if (index < parts.length - 1) {
          if (!target) return;
          currentTable = target;
        }
      }
    };
    const ancestors = new Set<object>();
    const walkFilter = async (
      node: unknown,
      currentTable: string,
      depth = 0,
    ): Promise<void> => {
      if (!node || typeof node !== 'object') return;
      if (depth > 64 || ancestors.has(node))
        throw new BadRequestException('Invalid filter structure');
      ancestors.add(node);
      try {
        if (Array.isArray(node)) {
          for (const entry of node)
            await walkFilter(entry, currentTable, depth + 1);
          return;
        }
        for (const [name, value] of Object.entries(node)) {
          if (name === '_and' || name === '_or' || name === '_not') {
            await walkFilter(value, currentTable, depth + 1);
            continue;
          }
          await checkPath(currentTable, name);
          const table = this.deps.runtimeRegistryService.lookupTableByName(
            currentTable,
          ) as ReadMetadataTable | null;
          const relation = table?.relations?.find(
            (item) => item.propertyName === name,
          );
          const target = relation?.targetTableName ?? relation?.targetTable;
          if (target) await walkFilter(value, target, depth + 1);
        }
      } finally {
        ancestors.delete(node);
      }
    };
    await walkFilter(context.$query?.filter, tableName);
    const sort = getSort(context.$query?.sort);
    const tokens = Array.isArray(sort) ? sort : (sort?.split(',') ?? []);
    for (const token of tokens) {
      const field = token.trim().replace(/^-/, '');
      const aggregate = field.match(/^_(count|max|min)\(([^)]+)\)$/);
      await checkPath(tableName, aggregate?.[2] ?? field);
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
    const isRoot = context.$user?.isRootAdmin === true;

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

    const dependencies = new Set<string>();
    const deniedColumns = new Set<string>();
    for (const name of columnsToCheck) {
      const column = tableMetadata.columns?.find((item) => item.name === name);
      if (column?.isPrimary === true) continue;
      if (isRoot && column?.isPublished !== false) continue;
      const conditionalFields = await this.getConditionalFieldsForField(
        tableName,
        'column',
        name,
        context,
      );
      if (conditionalFields !== null) {
        hasConditionalPending = true;
        for (const field of conditionalFields) dependencies.add(field);
        continue;
      }
      const defaultAllowed = column?.isPublished !== false;
      const decision = await this.decideReadField(
        tableName,
        'column',
        name,
        defaultAllowed,
        context,
      );
      if (!decision.allowed) deniedColumns.add(name);
    }

    const deniedRelations = new Set<string>();
    for (const name of relationsToCheck) {
      const relation = tableMetadata.relations?.find(
        (item) => item.propertyName === name,
      );
      if (isRoot && relation?.isPublished !== false) continue;
      const conditionalFields = await this.getConditionalFieldsForField(
        tableName,
        'relation',
        name,
        context,
      );
      if (conditionalFields !== null) {
        hasConditionalPending = true;
        for (const field of conditionalFields) dependencies.add(field);
        continue;
      }
      const decision = await this.decideReadField(
        tableName,
        'relation',
        name,
        relation?.isPublished !== false,
        context,
      );
      if (!decision.allowed) deniedRelations.add(name);
    }

    const cleanFieldList = fieldList.filter((field) => {
      const first = field.split('.').at(0) ?? '';
      return !deniedColumns.has(first) && !deniedRelations.has(first);
    });
    let cleanFields: string | string[] =
      cleanFieldList.length === 0
        ? []
        : typeof fields === 'string' || isWildcard
          ? cleanFieldList.join(',')
          : cleanFieldList;
    let cleanDeep = deep ? { ...deep } : undefined;
    if (cleanFieldList.length === 0) hasConditionalPending = true;

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

        await this.assertQueryAllowed({
          tableName: targetTable,
          context: {
            ...context,
            $query: { filter: entry.filter, sort: entry.sort },
          },
          enforceFieldPermission,
        });

        const cleanEntry = { ...entry };
        if (nested.fields === undefined) delete cleanEntry.fields;
        else cleanEntry.fields = nested.fields;
        if (nested.deep === undefined) delete cleanEntry.deep;
        else cleanEntry.deep = nested.deep;
        cleanDeep[relationName] = cleanEntry;
      }
    }

    if (dependencies.size > 0) {
      const internal = normalizeDynamicReadProjection({
        tableName,
        fields: [...new Set([...cleanFieldList, ...dependencies])],
        deep: cleanDeep,
        metadata: this.deps.runtimeRegistryService.requireMetadata(),
      });
      cleanFields = internal.fields ?? [];
      cleanDeep = internal.deep;
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

  private async getConditionalFieldsForField(
    tableName: string,
    subjectType: 'column' | 'relation',
    subjectName: string,
    context: DynamicReadAuthorizationContext,
  ): Promise<string[] | null> {
    const policies =
      (await this.deps.runtimeRegistryService.getFieldPermissionPoliciesFor?.(
        context.$user,
        tableName,
        'read',
      )) ?? [];
    const rules = policies
      .flatMap((policy) => policy.rules ?? [])
      .filter(
        (rule) =>
          rule.condition != null &&
          fieldPermissionRuleMatchesSubject(rule, {
            tableName,
            action: 'read',
            subjectType,
            subjectName,
          }) &&
          fieldPermissionRuleAppliesToUser(rule, context.$user),
      );
    return rules.length === 0
      ? null
      : rules.flatMap((rule) =>
          getFieldPermissionConditionFields(rule.condition),
        );
  }

  async assertAggregateFieldsAllowed(
    tableName: string,
    aggregate: unknown,
    context: DynamicReadAuthorizationContext,
    enforceFieldPermission: boolean,
  ): Promise<void> {
    if (!enforceFieldPermission) return;
    if (!isRecord(aggregate)) return;
    const isRoot = context.$user?.isRootAdmin === true;
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
          fields.add(key.split('.').at(0) ?? '');
          walkFilter(nested);
        }
      }
    };
    walkFilter(aggregate.filter);
    const dimensions = Array.isArray(aggregate.dimensions)
      ? aggregate.dimensions
      : [];
    for (const dimension of dimensions) {
      if (isRecord(dimension) && typeof dimension.field === 'string')
        fields.add(dimension.field);
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
        if (isRecord(sort) && typeof sort.field === 'string')
          fields.add(sort.field);
      }
    }
    const denied: Array<{ type: 'column'; name: string }> = [];
    for (const field of fields) {
      const column = table.columns?.find((item) => item.name === field);
      if (!column) continue;
      if (isRoot && column.isPublished !== false) continue;
      const conditional = await this.getConditionalFieldsForField(
        tableName,
        'column',
        field,
        context,
      );
      if (conditional !== null) {
        throw new ForbiddenException(
          formatFieldPermissionErrorMessage({
            action: 'filter',
            tableName,
            fields: [{ type: 'column', name: field }],
          }),
        );
      }
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
        const relation = getTableMetadata(
          metadata,
          currentTable,
        )?.relations?.find((item) => item.propertyName === field);
        if (!relation) break;
        currentTable =
          relation.targetTableName ?? relation.targetTable ?? currentTable;
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
