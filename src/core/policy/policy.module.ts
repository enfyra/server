import { Global, Module } from '@nestjs/common';
import { PolicyService } from './policy.service';

@Global()
@Module({
  providers: [PolicyService],
  exports: [PolicyService],
})
export class PolicyModule {}
