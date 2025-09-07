import { Global, Module } from '@nestjs/common';
import { AutoService } from './services/auto.service';

@Global()
@Module({
  providers: [AutoService],
  exports: [AutoService],
})
export class AutoModule {}
