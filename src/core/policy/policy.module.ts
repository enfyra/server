import { Global, Module } from '@nestjs/common';
import { PolicyService } from './policy.service';
import { SchemaMigrationValidatorService } from './services/schema-migration-validator.service';
import { SystemSafetyAuditorService } from './services/system-safety-auditor.service';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';
import { CommonModule } from '../../shared/common/common.module';

@Global()
@Module({
  imports: [CommonModule],
  providers: [
    PolicyService,
    SchemaMigrationValidatorService,
    SystemSafetyAuditorService,
    MetadataCacheService,
  ],
  exports: [
    PolicyService,
    SchemaMigrationValidatorService,
    SystemSafetyAuditorService,
  ],
})
export class PolicyModule {}
