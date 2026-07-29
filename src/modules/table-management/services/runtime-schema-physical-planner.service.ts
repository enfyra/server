import type { KnexService } from '../../../engines/knex';
import type { QueryBuilderService } from '@enfyra/kernel';
import type { MetadataCacheService } from '../../../engines/cache';
import type { SqlSchemaDiffService } from '../../../engines/knex/services/sql-schema-diff.service';
import type { MongoSchemaDiffService } from '../../../engines/mongo/services/mongo-schema-diff.service';
import type { DatabaseConfigService } from '../../../shared/services';
import {
  generateSQLFromDiff,
  generateBatchSQL,
} from '../../../engines/knex/utils/migration/sql-diff-generator';
import type { SchemaMutationBackend } from '../../../shared/types/schema-mutation-contract.types';

export interface PhysicalPlanSql {
  backend: 'postgresql' | 'mysql';
  upStatements: readonly string[];
  upBatch: string;
  downStatements: readonly string[];
  downBatch: string;
  metadataUpdate: unknown;
  activeTableName: string;
}

export interface PhysicalPlanMongo {
  backend: 'mongodb';
  upDiff: unknown;
  downDiff: unknown;
}

export type PhysicalPlan = PhysicalPlanSql | PhysicalPlanMongo | null;

export class RuntimeSchemaPhysicalPlannerService {
  constructor(
    private readonly deps: {
      databaseConfigService: DatabaseConfigService;
      knexService: KnexService;
      queryBuilderService: QueryBuilderService;
      metadataCacheService: MetadataCacheService;
      sqlSchemaDiffService: SqlSchemaDiffService;
      mongoSchemaDiffService: MongoSchemaDiffService;
    },
  ) {}

  async plan(input: {
    backend: SchemaMutationBackend;
    tableName: string;
    beforeMetadata: unknown;
    afterMetadata: unknown;
    schemaChanged: boolean;
  }): Promise<PhysicalPlan> {
    if (!input.schemaChanged) return null;
    if (!input.beforeMetadata || !input.afterMetadata) return null;

    if (input.backend === 'mongodb') {
      return this.planMongo(input);
    }
    return this.planSql(input);
  }

  private async planSql(input: {
    tableName: string;
    beforeMetadata: unknown;
    afterMetadata: unknown;
  }): Promise<PhysicalPlanSql> {
    const knex = this.deps.knexService.getKnex();
    const dbType = this.deps.queryBuilderService.getDatabaseType() as
      | 'mysql'
      | 'postgres';
    const backend = dbType === 'postgres' ? 'postgresql' : 'mysql';

    const upDiff = await this.deps.sqlSchemaDiffService.generateSchemaDiff(
      input.beforeMetadata,
      input.afterMetadata,
    );
    const upStatements = await generateSQLFromDiff(
      knex,
      input.tableName,
      upDiff,
      dbType,
      this.deps.metadataCacheService,
    );

    const downDiff = await this.deps.sqlSchemaDiffService.generateSchemaDiff(
      input.afterMetadata,
      input.beforeMetadata,
    );
    const downStatements = await generateSQLFromDiff(
      knex,
      input.tableName,
      downDiff,
      dbType,
      this.deps.metadataCacheService,
    );

    return {
      backend,
      upStatements,
      upBatch: generateBatchSQL(upStatements),
      downStatements,
      downBatch: generateBatchSQL(downStatements),
      metadataUpdate: upDiff.metadataUpdate ?? null,
      activeTableName: upDiff.table?.update?.newName || input.tableName,
    };
  }

  private planMongo(input: {
    beforeMetadata: unknown;
    afterMetadata: unknown;
  }): PhysicalPlanMongo {
    const upDiff = this.deps.mongoSchemaDiffService.generateMongoSchemaDiff(
      input.beforeMetadata,
      input.afterMetadata,
    );
    const downDiff = this.deps.mongoSchemaDiffService.generateMongoSchemaDiff(
      input.afterMetadata,
      input.beforeMetadata,
    );
    return { backend: 'mongodb', upDiff, downDiff };
  }
}
