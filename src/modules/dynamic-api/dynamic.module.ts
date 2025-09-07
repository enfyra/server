import { Module } from '@nestjs/common';
import { DynamicService } from './services/dynamic.service';
import { DynamicController } from './controllers/dynamic.controller';

@Module({
  imports: [],
  controllers: [DynamicController],
  providers: [DynamicService],
})
export class DynamicModule {}
