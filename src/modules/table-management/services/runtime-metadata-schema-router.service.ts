import type { QueryBuilderService } from '@enfyra/kernel';
import { ObjectId } from 'mongodb';
import {
  ResourceNotFoundException,
  ValidationException,
} from '../../../domain/exceptions';
import type { DatabaseConfigService } from '../../../shared/services';
import type { TCreateTableBody } from '../types/table-handler.types';
import type {
  RuntimeMetadataSchemaMutationInput,
  RuntimeMetadataSchemaMutationResult,
  RuntimeSchemaMetadataTable,
  RuntimeTableMutationInput,
} from '../types/runtime-metadata-schema-router.types';
import type { TableHandlerService } from './table-handler.service';
import type { RuntimeSchemaContractCompilerService } from './runtime-schema-contract-compiler.service';
import type { RuntimeSchemaExecutorService } from './runtime-schema-executor.service';
import { getSqlJunctionPhysicalNames } from '../utils/sql-junction-naming.util';
import { normalizeTableConstraints } from '../utils/table-constraints.util';
import type { TableManagementValidationService } from './table-validation.service';

export class RuntimeMetadataSchemaRouterService {
  constructor(
    private readonly deps: {
      queryBuilderService: QueryBuilderService;
      tableHandlerService: TableHandlerService;
      databaseConfigService: DatabaseConfigService;
      tableManagementValidationService: TableManagementValidationService;
      runtimeSchemaContractCompilerService: RuntimeSchemaContractCompilerService;
      runtimeSchemaExecutorService: RuntimeSchemaExecutorService;
    },
  ) {}

  handles(tableName: string): tableName is RuntimeSchemaMetadataTable {
    return tableName === 'enfyra_column' || tableName === 'enfyra_relation';
  }

  async create(
    input: RuntimeMetadataSchemaMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    this.validateProvidedColumns(
      input.tableName === 'enfyra_column' ? [input.data ?? {}] : [],
    );
    const ownerTableId = this.getOwnerTableId(input.tableName, input.data);
    const table = await this.loadOwnerTable(ownerTableId);
    let body = this.buildTableBody(table);
    const list = this.getBodyList(body, input.tableName);
    const child = this.normalizeChild(input.tableName, input.data ?? {});
    list.push(child);
    body = this.normalizeCompleteTableConstraints(body);
    this.validateSchemaBody(body);
    body = await this.resolveRelationTargetNames(body);
    const { contract, requiredConfirmHash } =
      await this.deps.runtimeSchemaContractCompilerService.compile({
        operation: 'update',
        tableName: String(table.name),
        tableId: ownerTableId,
        currentUser: input.context?.$user,
        beforeMetadata: table,
        afterMetadata: body,
        data: input.data,
        requestContext: input.context,
      });
    const confirmHash = this.extractConfirmHash(input.context);
    if (
      contract.context.diff.isDestructive &&
      confirmHash !== requiredConfirmHash
    ) {
      return {
        preview: this.buildPreview(contract, requiredConfirmHash),
        ownerTableId,
      };
    }
    const execResult = await this.deps.runtimeSchemaExecutorService.execute({
      contract,
      ownerTableId,
      body,
      context: input.context,
    });
    if (execResult.preview) {
      return { preview: execResult.preview, ownerTableId };
    }
    if (
      execResult.affectedTables.length === 0 &&
      contract.context.diff.schemaChanged === false
    ) {
      return {
        ownerTableId,
        affectedTables: [],
        mutationId: execResult.mutationId,
      };
    }
    const created = await this.findCreatedChild(
      input.tableName,
      ownerTableId,
      child,
    );
    return {
      recordId: this.getRecordId(created),
      ownerTableId,
      affectedTables: execResult.affectedTables as string[],
      tableRenames: execResult.tableRenames,
      mutationId: execResult.mutationId,
    };
  }

  async update(
    input: RuntimeMetadataSchemaMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    if (input.recordId == null) {
      throw new ValidationException('Schema metadata record id is required');
    }
    this.validateProvidedColumns(
      input.tableName === 'enfyra_column' ? [input.data ?? {}] : [],
    );
    const existing = input.existing ?? (await this.loadChild(input));
    const ownerTableId = this.getOwnerTableId(input.tableName, existing);
    const table = await this.loadOwnerTable(ownerTableId);
    let body = this.buildTableBody(table);
    const list = this.getBodyList(body, input.tableName);
    const index = list.findIndex(
      (entry: any) =>
        String(this.getRecordId(entry)) === String(input.recordId),
    );
    if (index < 0) {
      throw new ResourceNotFoundException(
        input.tableName,
        String(input.recordId),
      );
    }
    const previousChild = list[index];
    list[index] = this.normalizeChild(
      input.tableName,
      this.mergeNestedSchemaSubject(list[index], {
        ...input.data,
        [this.getPkField()]: input.recordId,
      }),
    );
    const fieldRenames = new Map<string, string>();
    if (
      input.tableName === 'enfyra_column' &&
      previousChild.name &&
      list[index].name &&
      previousChild.name !== list[index].name
    ) {
      fieldRenames.set(previousChild.name, list[index].name);
    }
    body = this.normalizeCompleteTableConstraints(body, fieldRenames);
    this.validateSchemaBody(body);
    body = await this.resolveRelationTargetNames(body);
    const { contract, requiredConfirmHash } =
      await this.deps.runtimeSchemaContractCompilerService.compile({
        operation: 'update',
        tableName: String(table.name),
        tableId: ownerTableId,
        currentUser: input.context?.$user,
        beforeMetadata: table,
        afterMetadata: body,
        data: input.data,
        requestContext: input.context,
      });
    const confirmHash = this.extractConfirmHash(input.context);
    if (
      contract.context.diff.isDestructive &&
      confirmHash !== requiredConfirmHash
    ) {
      return {
        preview: this.buildPreview(contract, requiredConfirmHash),
        ownerTableId,
        recordId: input.recordId,
      };
    }
    const execResult = await this.deps.runtimeSchemaExecutorService.execute({
      contract,
      ownerTableId,
      body,
      context: input.context,
    });
    if (execResult.preview) {
      return {
        preview: execResult.preview,
        ownerTableId,
        recordId: input.recordId,
      };
    }
    return {
      recordId: input.recordId,
      ownerTableId,
      affectedTables: execResult.affectedTables as string[],
      tableRenames: execResult.tableRenames,
      mutationId: execResult.mutationId,
    };
  }

  async delete(
    input: RuntimeMetadataSchemaMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    if (input.recordId == null) {
      throw new ValidationException('Schema metadata record id is required');
    }
    const existing = input.existing ?? (await this.loadChild(input));
    const ownerTableId = this.getOwnerTableId(input.tableName, existing);
    const table = await this.loadOwnerTable(ownerTableId);
    let body = this.buildTableBody(table);
    const list = this.getBodyList(body, input.tableName);
    const next = list.filter(
      (entry: any) =>
        String(this.getRecordId(entry)) !== String(input.recordId),
    );
    if (next.length === list.length) {
      throw new ResourceNotFoundException(
        input.tableName,
        String(input.recordId),
      );
    }
    if (input.tableName === 'enfyra_column') body.columns = next;
    else body.relations = next;
    body = await this.resolveRelationTargetNames(body);
    const { contract, requiredConfirmHash } =
      await this.deps.runtimeSchemaContractCompilerService.compile({
        operation: 'update',
        tableName: String(table.name),
        tableId: ownerTableId,
        currentUser: input.context?.$user,
        beforeMetadata: table,
        afterMetadata: body,
        data: input.data,
        requestContext: input.context,
      });
    const confirmHash = this.extractConfirmHash(input.context);
    if (confirmHash !== requiredConfirmHash) {
      return {
        preview: this.buildPreview(contract, requiredConfirmHash),
        ownerTableId,
        recordId: input.recordId,
      };
    }
    const execResult = await this.deps.runtimeSchemaExecutorService.execute({
      contract,
      ownerTableId,
      body,
      context: input.context,
    });
    if (execResult.preview) {
      return {
        preview: execResult.preview,
        ownerTableId,
        recordId: input.recordId,
      };
    }
    return {
      recordId: input.recordId,
      ownerTableId,
      affectedTables: execResult.affectedTables as string[],
      tableRenames: execResult.tableRenames,
      mutationId: execResult.mutationId,
    };
  }

  async markActivated(mutationId: string): Promise<void> {
    await this.deps.runtimeSchemaExecutorService.markActivated(mutationId);
  }

  async createTable(
    input: RuntimeTableMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    const body = this.normalizeCompleteTableConstraints(input.body!);
    this.validateSchemaBody(body);
    const resolvedBody = await this.resolveRelationTargetNames(body);
    this.assertNotReservedTableName(String(resolvedBody.name));
    const { contract, requiredConfirmHash } =
      await this.deps.runtimeSchemaContractCompilerService.compile({
        operation: 'create',
        tableName: String(resolvedBody.name),
        tableId: null,
        currentUser: input.context?.$user,
        beforeMetadata: null,
        afterMetadata: resolvedBody,
        data: resolvedBody,
        requestContext: input.context,
      });
    const confirmHash = this.extractConfirmHash(input.context);
    if (
      contract.context.diff.isDestructive &&
      confirmHash !== requiredConfirmHash
    ) {
      return {
        preview: this.buildPreview(contract, requiredConfirmHash),
      };
    }
    const execResult = await this.deps.runtimeSchemaExecutorService.execute({
      contract,
      body: resolvedBody,
      context: input.context,
    });
    if (execResult.preview) {
      return { preview: execResult.preview };
    }
    return {
      recordId: execResult.recordId,
      affectedTables: execResult.affectedTables as string[],
      tableRenames: execResult.tableRenames,
      mutationId: execResult.mutationId,
    };
  }

  async updateTable(
    input: RuntimeTableMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    this.validateProvidedColumns(input.body?.columns ?? []);
    const tableId = input.tableId!;
    const existing = await this.deps.queryBuilderService.findOne({
      table: 'enfyra_table',
      where: { [this.getPkField()]: tableId },
      fields: [
        '*',
        'columns.*',
        'columns.rules.*',
        'columns.fieldPermissions.*',
        'columns.fieldPermissions.role.id',
        'columns.fieldPermissions.allowedUsers.id',
        'relations.fieldPermissions.*',
        'relations.fieldPermissions.role.id',
        'relations.fieldPermissions.allowedUsers.id',
        'relations.*',
        'relations.targetTable.name',
        'relations.mappedBy.id',
        'relations.mappedBy._id',
        'relations.mappedBy.propertyName',
      ],
    });
    if (!existing) {
      throw new ResourceNotFoundException('enfyra_table', String(tableId));
    }
    const body = input.body!;
    const target = await this.resolveRelationTargetNames(
      this.buildCompleteTarget(existing, body),
    );
    this.validateSchemaBody(target);
    if (target.name !== existing.name) {
      this.assertNotReservedTableName(String(target.name));
    }
    const { contract, requiredConfirmHash } =
      await this.deps.runtimeSchemaContractCompilerService.compile({
        operation: 'update',
        tableName: String(existing.name),
        tableId,
        currentUser: input.context?.$user,
        beforeMetadata: existing,
        afterMetadata: target,
        data: target,
        requestContext: input.context,
      });
    const confirmHash = this.extractConfirmHash(input.context);
    if (
      contract.context.diff.schemaChanged &&
      confirmHash !== requiredConfirmHash
    ) {
      return {
        preview: this.buildPreview(contract, requiredConfirmHash),
      };
    }
    const execResult = await this.deps.runtimeSchemaExecutorService.execute({
      contract,
      ownerTableId: tableId,
      body: target,
      context: input.context,
    });
    if (execResult.preview) {
      return { preview: execResult.preview };
    }
    return {
      recordId: execResult.recordId ?? tableId,
      ownerTableId: tableId,
      affectedTables: execResult.affectedTables as string[],
      tableRenames: execResult.tableRenames,
      mutationId: execResult.mutationId,
    };
  }

  async deleteTable(
    input: RuntimeTableMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    const tableId = input.tableId!;
    const existing = await this.deps.queryBuilderService.findOne({
      table: 'enfyra_table',
      where: { [this.getPkField()]: tableId },
      fields: [
        '*',
        'columns.*',
        'columns.rules.*',
        'columns.fieldPermissions.*',
        'columns.fieldPermissions.role.id',
        'columns.fieldPermissions.allowedUsers.id',
        'relations.fieldPermissions.*',
        'relations.fieldPermissions.role.id',
        'relations.fieldPermissions.allowedUsers.id',
        'relations.*',
        'relations.targetTable.name',
        'relations.mappedBy.id',
        'relations.mappedBy._id',
        'relations.mappedBy.propertyName',
      ],
    });
    if (!existing) {
      throw new ResourceNotFoundException('enfyra_table', String(tableId));
    }
    const { contract, requiredConfirmHash } =
      await this.deps.runtimeSchemaContractCompilerService.compile({
        operation: 'delete',
        tableName: String(existing.name),
        tableId,
        currentUser: input.context?.$user,
        beforeMetadata: existing,
        afterMetadata: null,
        data: null,
        requestContext: input.context,
      });
    const confirmHash = this.extractConfirmHash(input.context);
    if (confirmHash !== requiredConfirmHash) {
      return {
        preview: this.buildPreview(contract, requiredConfirmHash),
      };
    }
    const execResult = await this.deps.runtimeSchemaExecutorService.execute({
      contract,
      tableId,
      context: input.context,
    });
    if (execResult.preview) {
      return { preview: execResult.preview };
    }
    return {
      recordId: tableId,
      ownerTableId: tableId,
      affectedTables: execResult.affectedTables as string[],
      tableRenames: execResult.tableRenames,
      mutationId: execResult.mutationId,
    };
  }

  private buildCompleteTarget(existing: any, body: any): any {
    const pk = this.getPkField();
    const target: any = { ...existing };
    const fieldRenames = new Map<string, string>();
    const scalarFields = [
      'name',
      'description',
      'alias',
      'isSingleRecord',
      'graphqlEnabled',
      'validateBody',
    ];
    for (const field of scalarFields) {
      if (body[field] !== undefined) target[field] = body[field];
    }
    if (body.indexes !== undefined) {
      target.indexes = body.indexes;
    } else if (typeof target.indexes === 'string') {
      try {
        target.indexes = JSON.parse(target.indexes);
      } catch {}
    }
    if (body.uniques !== undefined) {
      target.uniques = body.uniques;
    } else if (typeof target.uniques === 'string') {
      try {
        target.uniques = JSON.parse(target.uniques);
      } catch {}
    }
    if (Array.isArray(body.columns)) {
      target.columns = body.columns.map((col: any) => {
        const childId = col[pk] ?? col.id ?? col._id;
        const matchById =
          childId != null
            ? (existing.columns || []).find(
                (c: any) => String(c[pk] ?? c.id ?? c._id) === String(childId),
              )
            : null;
        const matchByName =
          !matchById && col.name
            ? (existing.columns || []).find((c: any) => c.name === col.name)
            : null;
        const match = matchById || matchByName;
        if (match) {
          if (match.name && col.name && match.name !== col.name) {
            fieldRenames.set(match.name, col.name);
          }
          return this.mergeNestedSchemaSubject(match, col);
        }
        return col;
      });
    }
    if (Array.isArray(body.relations)) {
      target.relations = body.relations.map((rel: any) => {
        const childId = rel[pk] ?? rel.id ?? rel._id;
        const matchById =
          childId != null
            ? (existing.relations || []).find(
                (r: any) => String(r[pk] ?? r.id ?? r._id) === String(childId),
              )
            : null;
        const matchByProp =
          !matchById && rel.propertyName
            ? (existing.relations || []).find(
                (r: any) => r.propertyName === rel.propertyName,
              )
            : null;
        const match = matchById || matchByProp;
        if (match) {
          if (
            match.propertyName &&
            rel.propertyName &&
            match.propertyName !== rel.propertyName
          ) {
            fieldRenames.set(match.propertyName, rel.propertyName);
          }
          return this.mergeNestedSchemaSubject(match, rel);
        }
        return rel;
      });
    }
    return this.normalizeCompleteTableConstraints(target, fieldRenames);
  }

  private mergeNestedSchemaSubject(existing: any, incoming: any): any {
    return {
      ...existing,
      ...incoming,
      ...(Array.isArray(incoming.fieldPermissions) && {
        fieldPermissions: this.mergeNestedSchemaMetadata(
          existing.fieldPermissions,
          incoming.fieldPermissions,
        ),
      }),
      ...(Array.isArray(incoming.rules) && {
        rules: this.mergeNestedSchemaMetadata(existing.rules, incoming.rules),
      }),
    };
  }

  private mergeNestedSchemaMetadata(
    existingEntries: unknown,
    incomingEntries: unknown[],
  ): unknown[] {
    if (!Array.isArray(existingEntries)) return incomingEntries;
    const existingById = new Map(
      existingEntries
        .filter((entry: any) => entry && typeof entry === 'object')
        .map((entry: any) => [String(this.getRecordId(entry)), entry]),
    );
    return incomingEntries.map((entry: any) => {
      if (!entry || typeof entry !== 'object') return entry;
      const entryId = this.getRecordId(entry);
      if (entryId == null) return entry;
      const existing = existingById.get(String(entryId));
      return existing ? { ...existing, ...entry } : entry;
    });
  }

  private normalizeCompleteTableConstraints(
    body: TCreateTableBody,
    fieldRenames: ReadonlyMap<string, string> = new Map(),
  ): TCreateTableBody {
    const allowedConstraintFields = new Set<string>([
      'id',
      '_id',
      'createdAt',
      'updatedAt',
      ...(body.columns || []).map((column: any) => column.name).filter(Boolean),
      ...(body.relations || [])
        .map((relation: any) => relation.propertyName)
        .filter(Boolean),
    ]);
    const constraints = normalizeTableConstraints({
      uniques: body.uniques,
      indexes: body.indexes,
      columns: body.columns,
      renames: fieldRenames,
      allowedFields: allowedConstraintFields,
    });
    return {
      ...body,
      uniques: constraints.uniques as any,
      indexes: constraints.indexes as any,
    };
  }

  private async loadChild(
    input: RuntimeMetadataSchemaMutationInput,
  ): Promise<Record<string, any>> {
    const child = await this.deps.queryBuilderService.findOne({
      table: input.tableName,
      where: { [this.getPkField()]: input.recordId },
      fields: ['*'],
    });
    if (!child) {
      throw new ResourceNotFoundException(
        input.tableName,
        String(input.recordId),
      );
    }
    return child;
  }

  private async loadOwnerTable(ownerTableId: string | number): Promise<any> {
    const table = await this.deps.queryBuilderService.findOne({
      table: 'enfyra_table',
      where: { [this.getPkField()]: ownerTableId },
      fields: [
        '*',
        'columns.*',
        'columns.rules.*',
        'columns.fieldPermissions.*',
        'columns.fieldPermissions.role.id',
        'columns.fieldPermissions.allowedUsers.id',
        'relations.fieldPermissions.*',
        'relations.fieldPermissions.role.id',
        'relations.fieldPermissions.allowedUsers.id',
        'relations.*',
        'relations.targetTable.id',
        'relations.targetTable._id',
        'relations.targetTable.name',
        'relations.mappedBy.id',
        'relations.mappedBy._id',
        'relations.mappedBy.propertyName',
      ],
    });
    if (!table) {
      throw new ResourceNotFoundException('enfyra_table', String(ownerTableId));
    }
    return table;
  }

  private buildTableBody(table: any): TCreateTableBody {
    return {
      name: table.name,
      alias: table.alias,
      description: table.description,
      isSystem: table.isSystem,
      isSingleRecord: table.isSingleRecord,
      graphqlEnabled: table.graphqlEnabled,
      validateBody: table.validateBody,
      indexes: table.indexes ?? [],
      uniques: table.uniques ?? [],
      columns: (table.columns ?? []).map((column: any) =>
        this.normalizeChild('enfyra_column', column),
      ),
      relations: (table.relations ?? []).map((relation: any) =>
        this.normalizeChild('enfyra_relation', relation),
      ),
    };
  }

  private getBodyList(
    body: TCreateTableBody,
    tableName: RuntimeSchemaMetadataTable,
  ): any[] {
    return tableName === 'enfyra_column'
      ? (body.columns as any[])
      : ((body.relations ??= []) as any[]);
  }

  private normalizeChild(
    tableName: RuntimeSchemaMetadataTable,
    value: Record<string, any>,
  ): any {
    const normalized = { ...value };
    delete normalized.table;
    delete normalized.sourceTable;
    delete normalized.sourceTableId;
    delete normalized.sourceTableName;
    if (tableName === 'enfyra_relation') {
      const target = value.targetTable;
      const targetId =
        target && typeof target === 'object'
          ? this.getReferenceId(target)
          : (target ?? value.targetTableId);
      const targetName =
        target && typeof target === 'object'
          ? target.name
          : value.targetTableName;
      if (this.deps.databaseConfigService.isMongoDb() && targetId != null) {
        normalized.targetTable = {
          _id:
            typeof targetId === 'string' && ObjectId.isValid(targetId)
              ? new ObjectId(targetId)
              : targetId,
        };
      } else {
        normalized.targetTable = targetId;
      }
      if (targetName) {
        normalized.targetTableName = targetName;
      }
      delete normalized.targetTableId;
    }
    return normalized;
  }

  private async findCreatedChild(
    tableName: RuntimeSchemaMetadataTable,
    ownerTableId: string | number,
    child: Record<string, any>,
  ): Promise<any> {
    const ownerField = tableName === 'enfyra_column' ? 'table' : 'sourceTable';
    const identityField =
      tableName === 'enfyra_column' ? 'name' : 'propertyName';
    const created = await this.deps.queryBuilderService.findOne({
      table: tableName,
      where: {
        [ownerField]: ownerTableId,
        [identityField]: child[identityField],
      },
      fields: ['*'],
    });
    if (!created) {
      throw new ResourceNotFoundException(
        tableName,
        `${String(ownerTableId)}.${String(child[identityField])}`,
      );
    }
    return created;
  }

  private getOwnerTableId(
    tableName: RuntimeSchemaMetadataTable,
    value: Record<string, any> | undefined,
  ): string | number {
    const owner =
      tableName === 'enfyra_column'
        ? (value?.table ?? value?.tableId)
        : (value?.sourceTable ?? value?.sourceTableId);
    const ownerId =
      owner && typeof owner === 'object' ? this.getReferenceId(owner) : owner;
    if (ownerId == null || ownerId === '') {
      throw new ValidationException(
        `${tableName} requires its owning table reference`,
      );
    }
    return ownerId;
  }

  private getPkField(): 'id' | '_id' {
    return this.deps.databaseConfigService.isMongoDb() ? '_id' : 'id';
  }

  private getRecordId(record: any): any {
    return record?.[this.getPkField()] ?? record?.id ?? record?._id;
  }

  private getReferenceId(record: any): any {
    return this.deps.databaseConfigService.isMongoDb()
      ? (record?._id ?? record?.id)
      : (record?.id ?? record?._id);
  }

  private extractConfirmHash(context: any): string | undefined {
    const value =
      context?.$query?.schemaConfirmHash ??
      context?.$query?.schema_confirm_hash ??
      context?.$query?.confirmHash ??
      context?.$query?.confirm_hash;
    return typeof value === 'string' ? value.trim().toLowerCase() : undefined;
  }

  private buildPreview(contract: any, requiredConfirmHash: string): Record<string, any> {
    const diff = contract.context?.diff ?? {};
    return {
      _preview: true,
      tableName: diff.tableName,
      operation: diff.operation,
      schemaChanged: diff.schemaChanged,
      policyMetadataChanged: diff.policyMetadataChanged,
      isDestructive: diff.isDestructive,
      removedColumns: diff.removedColumns ?? [],
      addedColumns: diff.addedColumns ?? [],
      renamedColumns: diff.renamedColumns ?? [],
      changedColumns: diff.changedColumns ?? [],
      removedRelationsCount: Array.isArray(diff.removedRelations)
        ? diff.removedRelations.length
        : 0,
      addedRelationsCount: Array.isArray(diff.addedRelations)
        ? diff.addedRelations.length
        : 0,
      removedUniques: diff.removedUniques ?? [],
      addedUniques: diff.addedUniques ?? [],
      removedIndexes: diff.removedIndexes ?? [],
      addedIndexes: diff.addedIndexes ?? [],
      owningSideInverseCascadeWarnings:
        diff.owningSideInverseCascadeWarnings ?? [],
      requiredConfirmHash,
      schemaMutationContract: contract,
    };
  }

  private validateColumns(body: TCreateTableBody): void {
    this.deps.tableManagementValidationService.validateColumns(
      body.columns,
      this.deps.databaseConfigService.getDbType(),
    );
  }

  private validateSchemaBody(body: TCreateTableBody): void {
    this.validateColumns(body);
    this.deps.tableManagementValidationService.validateRelations(
      body.relations ?? [],
    );
  }

  private validateProvidedColumns(
    columns: readonly { name?: unknown }[],
  ): void {
    const namedColumns = columns.filter((column) => column?.name !== undefined);
    if (namedColumns.length === 0) return;
    this.deps.tableManagementValidationService.validateColumns(
      namedColumns,
      this.deps.databaseConfigService.getDbType(),
    );
  }

  private assertNotReservedTableName(name: string): void {
    if (name.startsWith('enfyra_')) {
      throw new ValidationException(
        `Table name '${name}' uses the reserved 'enfyra_' prefix and cannot be created or renamed at runtime`,
      );
    }
  }

  private async resolveRelationTargetNames(
    body: TCreateTableBody,
  ): Promise<TCreateTableBody> {
    if (!body.relations?.length) return body;
    const targetLookups = new Map<string, Promise<any>>();
    const mappedByLookups = new Map<string, Promise<any>>();
    const relations = await Promise.all(
      body.relations.map(async (relation: any) => {
        const rel = { ...relation };
        const raw = rel.targetTable;
        if (raw != null) {
          const targetId =
            raw && typeof raw === 'object' ? this.getReferenceId(raw) : raw;
          if (targetId == null || targetId === '') {
            throw new ValidationException(
              `Relation '${String(rel.propertyName)}' requires a target table id`,
            );
          }
          const canonicalId = this.normalizeReferenceId(targetId);
          const lookupKey = String(canonicalId);
          let lookup = targetLookups.get(lookupKey);
          if (!lookup) {
            lookup = this.deps.queryBuilderService.findOne({
              table: 'enfyra_table',
              where: { [this.getPkField()]: canonicalId },
              fields: ['name'],
            });
            targetLookups.set(lookupKey, lookup);
          }
          const target = await lookup;
          if (!target?.name) {
            throw new ResourceNotFoundException(
              'enfyra_table',
              String(canonicalId),
            );
          }
          rel.targetTableName = target.name;
        }

        const rawMappedBy =
          rel.mappedBy ?? rel.mappedById ?? rel.mappedByRelationId;
        if (rawMappedBy != null && rawMappedBy !== '') {
          const propertyName =
            typeof rawMappedBy === 'object'
              ? rawMappedBy.propertyName
              : undefined;
          if (propertyName) {
            rel.mappedBy = String(propertyName);
          } else if (
            typeof rawMappedBy === 'object' ||
            this.isRelationReferenceId(rawMappedBy)
          ) {
            const mappedById =
              typeof rawMappedBy === 'object'
                ? this.getReferenceId(rawMappedBy)
                : rawMappedBy;
            if (mappedById == null || mappedById === '') {
              throw new ValidationException(
                `Relation '${String(rel.propertyName)}' has an invalid mappedBy reference`,
              );
            }
            const canonicalId = this.normalizeReferenceId(mappedById);
            const lookupKey = String(canonicalId);
            let lookup = mappedByLookups.get(lookupKey);
            if (!lookup) {
              lookup = this.deps.queryBuilderService.findOne({
                table: 'enfyra_relation',
                where: { [this.getPkField()]: canonicalId },
                fields: ['propertyName'],
              });
              mappedByLookups.set(lookupKey, lookup);
            }
            const mappedBy = await lookup;
            if (!mappedBy?.propertyName) {
              throw new ResourceNotFoundException(
                'enfyra_relation',
                String(canonicalId),
              );
            }
            rel.mappedBy = mappedBy.propertyName;
          }
        }
        delete rel.mappedById;
        delete rel.mappedByRelationId;
        if (
          rel.type === 'many-to-many' &&
          !rel.mappedBy &&
          body.name &&
          rel.propertyName &&
          rel.targetTableName
        ) {
          const junction = getSqlJunctionPhysicalNames({
            sourceTable: body.name,
            propertyName: rel.propertyName,
            targetTable: rel.targetTableName,
          });
          rel.junctionTableName ||= junction.junctionTableName;
          rel.junctionSourceColumn ||= junction.junctionSourceColumn;
          rel.junctionTargetColumn ||= junction.junctionTargetColumn;
        }
        return rel;
      }),
    );
    return { ...body, relations };
  }

  private normalizeReferenceId(value: unknown): unknown {
    if (this.deps.databaseConfigService.isMongoDb()) {
      return typeof value === 'string' && ObjectId.isValid(value)
        ? new ObjectId(value)
        : value;
    }
    return typeof value === 'string' && /^\d+$/.test(value)
      ? Number(value)
      : value;
  }

  private isRelationReferenceId(value: unknown): boolean {
    if (typeof value === 'number') return true;
    if (typeof value !== 'string') return false;
    return this.deps.databaseConfigService.isMongoDb()
      ? ObjectId.isValid(value)
      : /^\d+$/.test(value);
  }
}
