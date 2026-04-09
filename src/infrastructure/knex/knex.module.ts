import { Global, Module, forwardRef } from '@nestjs/common';
import { KnexService } from './knex.service';
import { SqlSchemaMigrationService } from './services/sql-schema-migration.service';
import { DatabaseSchemaService } from './services/database-schema.service';
import { CacheModule } from '../cache/cache.module';
import { SchemaMigrationLockService } from './services/schema-migration-lock.service';
import { ReplicationManager } from './services/replication-manager.service';
import { SqlPoolClusterCoordinatorService } from './services/sql-pool-cluster-coordinator.service';
import { CommonModule } from '../../shared/common/common.module';

@Global()
@Module({
  imports: [forwardRef(() => CacheModule), CommonModule],
  providers: [
    ReplicationManager,
    KnexService,
    SqlPoolClusterCoordinatorService,
    SqlSchemaMigrationService,
    DatabaseSchemaService,
    SchemaMigrationLockService,
  ],
  exports: [
    KnexService,
    SqlSchemaMigrationService,
    SchemaMigrationLockService,
    ReplicationManager,
  ],
})
export class KnexModule {}
