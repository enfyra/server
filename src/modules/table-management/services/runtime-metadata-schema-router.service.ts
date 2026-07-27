import type { QueryBuilderService } from '@enfyra/kernel';
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
} from '../types/runtime-metadata-schema-router.types';
import type { TableHandlerService } from './table-handler.service';
import type { RuntimeSchemaContractCompilerService } from './runtime-schema-contract-compiler.service';
import type { RuntimeSchemaExecutorService } from './runtime-schema-executor.service';

export class RuntimeMetadataSchemaRouterService {
  constructor(
    private readonly deps: {
      queryBuilderService: QueryBuilderService;
      tableHandlerService: TableHandlerService;
      databaseConfigService: DatabaseConfigService;
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
    const ownerTableId = this.getOwnerTableId(input.tableName, input.data);
    const table = await this.loadOwnerTable(ownerTableId);
    const body = this.buildTableBody(table);
    const list = this.getBodyList(body, input.tableName);
    const child = this.normalizeChild(input.tableName, input.data ?? {});
    list.push(child);
    await this.resolveRelationTargetNames(body);
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
    if (contract.context.diff.isDestructive && confirmHash !== requiredConfirmHash) {
      return {
        preview: { _preview: true, requiredConfirmHash, schemaMutationContract: contract },
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
    if (execResult.affectedTables.length === 0 && contract.context.diff.schemaChanged === false) {
      return { ownerTableId, affectedTables: [] };
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
    };
  }

  async update(
    input: RuntimeMetadataSchemaMutationInput,
  ): Promise<RuntimeMetadataSchemaMutationResult> {
    if (input.recordId == null) {
      throw new ValidationException('Schema metadata record id is required');
    }
    const existing = input.existing ?? (await this.loadChild(input));
    const ownerTableId = this.getOwnerTableId(input.tableName, existing);
    const table = await this.loadOwnerTable(ownerTableId);
    const body = this.buildTableBody(table);
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
    list[index] = this.normalizeChild(input.tableName, {
      ...list[index],
      ...input.data,
      [this.getPkField()]: input.recordId,
    });
    await this.resolveRelationTargetNames(body);
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
    if (contract.context.diff.isDestructive && confirmHash !== requiredConfirmHash) {
      return {
        preview: { _preview: true, requiredConfirmHash, schemaMutationContract: contract },
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
      return { preview: execResult.preview, ownerTableId, recordId: input.recordId };
    }
    return {
      recordId: input.recordId,
      ownerTableId,
      affectedTables: execResult.affectedTables as string[],
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
    const body = this.buildTableBody(table);
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
    await this.resolveRelationTargetNames(body);
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
        preview: { _preview: true, requiredConfirmHash, schemaMutationContract: contract },
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
    return {
      recordId: input.recordId,
      ownerTableId,
      affectedTables: execResult.affectedTables as string[],
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
        'relations.*',
        'relations.targetTable.id',
        'relations.targetTable._id',
        'relations.targetTable.name',
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
      normalized.targetTable = targetId;
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
    return (
      context?.$query?.schemaConfirmHash ??
      context?.$query?.confirmHash ??
      undefined
    );
  }

  private async resolveRelationTargetNames(body: TCreateTableBody): Promise<void> {
    if (!body.relations?.length) return;
    const unresolved = body.relations.filter(
      (rel: any) => rel.targetTable != null && !rel.targetTableName,
    );
    if (!unresolved.length) return;
    await Promise.all(
      unresolved.map(async (rel: any) => {
        const targetId = rel.targetTable;
        const target = await this.deps.queryBuilderService.findOne({
          table: 'enfyra_table',
          where: { [this.getPkField()]: targetId },
          fields: ['name'],
        });
        if (target?.name) {
          rel.targetTableName = target.name;
        }
      }),
    );
  }
}
