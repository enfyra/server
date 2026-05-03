import { Logger } from '../../../shared/logger';
import { ObjectId } from 'mongodb';
import { QueryBuilderService } from '@enfyra/kernel';
import {
  type MongoPhysicalMigrationService,
  MongoSchemaMigrationService,
  MongoService,
  MongoSchemaMigrationLockService,
} from '../../../engines/mongo';
import { MetadataCacheService } from '../../../engines/cache';
import {
  LoggingService,
  DatabaseException,
  DuplicateResourceException,
  ResourceNotFoundException,
  ValidationException,
} from '../../../domain/exceptions';
import {
  PolicyService,
  isPolicyDeny,
  isPolicyPreview,
} from '../../../domain/policy';
import { TDynamicContext } from '../../../shared/types';
import { validateUniquePropertyNames } from '../utils/duplicate-field-check';
import { DatabaseConfigService } from '../../../shared/services';
import { getDeletedIds } from '../utils/get-deleted-ids';
import { TCreateTableBody } from '../types/table-handler.types';
import { TableManagementValidationService } from './table-validation.service';
import { MongoMetadataSnapshotService } from './mongo-metadata-snapshot.service';
import {
  MONGO_PRIMARY_KEY_TYPE,
  isMongoPrimaryKeyType,
  normalizeMongoPrimaryKeyColumn,
} from '../utils/mongo-primary-key.util';
import { getRelationMappedByProperty } from '../utils/relation-target-id.util';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { ensureMongoTableRouteArtifacts } from './table-route-artifacts.service';
import {
  ensureMongoSingleRecord,
  syncMongoGqlDefinition,
} from './table-post-migration.service';

export class MongoTableHandlerService {
  protected logger = new Logger(MongoTableHandlerService.name);
  protected queryBuilderService: QueryBuilderService;
  protected mongoSchemaMigrationService: MongoSchemaMigrationService;
  protected mongoPhysicalMigrationService: MongoPhysicalMigrationService;
  protected mongoService: MongoService;
  protected mongoSchemaMigrationLockService: MongoSchemaMigrationLockService;
  protected metadataCacheService: MetadataCacheService;
  protected loggingService: LoggingService;
  protected policyService: PolicyService;
  protected tableValidationService: TableManagementValidationService;
  protected mongoMetadataSnapshotService: MongoMetadataSnapshotService;
  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    mongoSchemaMigrationService: MongoSchemaMigrationService;
    mongoPhysicalMigrationService: MongoPhysicalMigrationService;
    mongoService: MongoService;
    mongoSchemaMigrationLockService: MongoSchemaMigrationLockService;
    metadataCacheService: MetadataCacheService;
    loggingService: LoggingService;
    policyService: PolicyService;
    tableManagementValidationService: TableManagementValidationService;
    mongoMetadataSnapshotService: MongoMetadataSnapshotService;
  }) {
    this.queryBuilderService = deps.queryBuilderService;
    this.mongoSchemaMigrationService = deps.mongoSchemaMigrationService;
    this.mongoPhysicalMigrationService = deps.mongoPhysicalMigrationService;
    this.mongoService = deps.mongoService;
    this.mongoSchemaMigrationLockService = deps.mongoSchemaMigrationLockService;
    this.metadataCacheService = deps.metadataCacheService;
    this.loggingService = deps.loggingService;
    this.policyService = deps.policyService;
    this.tableValidationService = deps.tableManagementValidationService;
    this.mongoMetadataSnapshotService = deps.mongoMetadataSnapshotService;
  }
  protected async writeNestedRulesMongo(opts: {
    rules: any[] | undefined;
    subjectFk: 'column' | 'relation';
    subjectFkValue: any;
  }): Promise<void> {
    if (!Array.isArray(opts.rules)) return;
    const { data: existing } = await this.queryBuilderService.find({
      table: 'column_rule_definition',
      where: { [opts.subjectFk]: opts.subjectFkValue },
    });
    const deletedIds = getDeletedIds(existing, opts.rules);
    for (const rid of deletedIds) {
      await this.queryBuilderService.delete('column_rule_definition', rid);
    }
    for (const rule of opts.rules) {
      const ruleData: any = {
        ruleType: rule.ruleType,
        value: rule.value ?? null,
        message: rule.message ?? null,
        isEnabled: rule.isEnabled !== false,
        [opts.subjectFk]: opts.subjectFkValue,
      };
      const ruleId = rule._id || rule.id;
      if (ruleId) {
        await this.queryBuilderService.update(
          'column_rule_definition',
          ruleId,
          ruleData,
        );
      } else {
        await this.queryBuilderService.insert(
          'column_rule_definition',
          ruleData,
        );
      }
    }
  }

  protected async writeNestedFieldPermissionsMongo(opts: {
    permissions: any[] | undefined;
    subjectFk: 'column' | 'relation';
    subjectFkValue: any;
  }): Promise<void> {
    if (!Array.isArray(opts.permissions)) return;
    const { data: existing } = await this.queryBuilderService.find({
      table: 'field_permission_definition',
      where: { [opts.subjectFk]: opts.subjectFkValue },
    });
    const deletedIds = getDeletedIds(existing, opts.permissions);
    for (const pid of deletedIds) {
      await this.queryBuilderService.delete('field_permission_definition', pid);
    }
    for (const perm of opts.permissions) {
      const roleRef =
        perm.role && typeof perm.role === 'object'
          ? perm.role._id || perm.role.id
          : perm.role;
      const allowedUserIds = Array.isArray(perm.allowedUsers)
        ? perm.allowedUsers
            .map((u: any) => (typeof u === 'object' ? u._id || u.id : u))
            .filter((v: any) => v != null)
        : undefined;
      const permData: any = {
        action: perm.action,
        effect: perm.effect ?? 'allow',
        condition: perm.condition ?? null,
        isEnabled: perm.isEnabled !== false,
        description: perm.description ?? null,
        role: roleRef ?? null,
        column: opts.subjectFk === 'column' ? opts.subjectFkValue : null,
        relation: opts.subjectFk === 'relation' ? opts.subjectFkValue : null,
        ...(allowedUserIds !== undefined && { allowedUsers: allowedUserIds }),
      };
      const permId = perm._id || perm.id;
      if (permId) {
        await this.queryBuilderService.update(
          'field_permission_definition',
          permId,
          permData,
        );
      } else {
        await this.queryBuilderService.insert(
          'field_permission_definition',
          permData,
        );
      }
    }
  }

  protected async getFullTableMetadata(tableId: any): Promise<any> {
    const queryId =
      typeof tableId === 'string' ? new ObjectId(tableId) : tableId;

    // Use direct MongoDB queries to avoid filter DSL routing issues
    // where FK column names (e.g. 'table', 'sourceTable') collide with relation names
    const db = this.mongoService.getDb();
    const normalize = (doc: any) => {
      if (!doc) return doc;
      const normalized: any = {};
      for (const [key, value] of Object.entries(doc)) {
        if (value instanceof ObjectId) {
          normalized[key] = value.toString();
        } else if (value instanceof Date) {
          normalized[key] = value.toISOString();
        } else {
          normalized[key] = value;
        }
      }
      return normalized;
    };

    const rawTable = await db
      .collection('table_definition')
      .findOne({ _id: queryId });
    if (!rawTable) return null;
    const table = normalize(rawTable);

    if (table.uniques && typeof table.uniques === 'string') {
      try {
        table.uniques = JSON.parse(table.uniques);
      } catch (e: any) {
        table.uniques = [];
      }
    }
    if (table.indexes && typeof table.indexes === 'string') {
      try {
        table.indexes = JSON.parse(table.indexes);
      } catch (e: any) {
        table.indexes = [];
      }
    }
    const rawColumns = await db
      .collection('column_definition')
      .find({ table: queryId })
      .toArray();
    const columns = rawColumns.map(normalize);
    table.columns = columns;
    for (const col of table.columns) {
      if (col.defaultValue && typeof col.defaultValue === 'string') {
        try {
          col.defaultValue = JSON.parse(col.defaultValue);
        } catch (e: any) {}
      }
      if (col.options && typeof col.options === 'string') {
        try {
          col.options = JSON.parse(col.options);
        } catch (e: any) {}
      }
    }
    const rawRelations = await db
      .collection('relation_definition')
      .find({ sourceTable: queryId })
      .toArray();
    const relations = rawRelations.map(normalize);
    table.relations = relations;
    return table;
  }

  protected getAllowedConstraintFields(body: TCreateTableBody): Set<string> | null {
    if (!body.columns && !body.relations) return null;
    const fields = new Set<string>(['_id', 'id', 'createdAt', 'updatedAt']);
    for (const col of body.columns || []) {
      if (col?.name) fields.add(col.name);
    }
    for (const rel of body.relations || []) {
      if (rel?.propertyName) fields.add(rel.propertyName);
    }
    return fields;
  }

  protected filterConstraintGroups(
    groups: any[],
    allowedFields: Set<string>,
  ): any[] {
    return (groups || []).filter((group) =>
      (Array.isArray(group) ? group : group?.value || []).every((field: string) =>
        allowedFields.has(field),
      ),
    );
  }

  protected normalizeConstraintGroups(
    groups: any[],
    oldMetadata: any,
    body: TCreateTableBody,
    allowedFields: Set<string>,
  ): any[] {
    const renames = this.getConstraintFieldRenames(oldMetadata, body);
    return (groups || [])
      .map((group) => {
        const values = (Array.isArray(group) ? group : group?.value || []).map(
          (field: string) => renames.get(field) || field,
        );
        return Array.isArray(group) ? values : { ...group, value: values };
      })
      .filter((group) =>
        (Array.isArray(group) ? group : group?.value || []).every((field: string) =>
          allowedFields.has(field),
        ),
      );
  }

  protected getConstraintFieldRenames(
    oldMetadata: any,
    body: TCreateTableBody,
  ): Map<string, string> {
    const renames = new Map<string, string>();
    const oldColumnsById = new Map<string, any>(
      (oldMetadata?.columns || []).map((col: any) => [
        String(col.id ?? col._id),
        col,
      ]),
    );
    for (const col of body.columns || []) {
      const oldCol = oldColumnsById.get(String((col as any).id ?? (col as any)._id));
      if (oldCol?.name && col.name && oldCol.name !== col.name) {
        renames.set(oldCol.name, col.name);
      }
    }

    const oldRelationsById = new Map<string, any>(
      (oldMetadata?.relations || []).map((rel: any) => [
        String(rel.id ?? rel._id),
        rel,
      ]),
    );
    for (const rel of body.relations || []) {
      const oldRel = oldRelationsById.get(String((rel as any).id ?? (rel as any)._id));
      if (
        oldRel?.propertyName &&
        rel.propertyName &&
        oldRel.propertyName !== rel.propertyName
      ) {
        renames.set(oldRel.propertyName, rel.propertyName);
      }
    }
    return renames;
  }

  protected async runWithSchemaLock<T>(
    context: string,
    handler: () => Promise<T>,
  ): Promise<T> {
    const lock = await this.mongoSchemaMigrationLockService.acquire(context);
    try {
      return await handler();
    } finally {
      await this.mongoSchemaMigrationLockService.release(lock);
    }
}

}
