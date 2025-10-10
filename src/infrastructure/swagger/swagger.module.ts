import { Module } from '@nestjs/common';
import { SwaggerService } from './services/swagger.service';
import { SwaggerController } from './controllers/swagger.controller';

@Module({
  controllers: [SwaggerController],
  providers: [SwaggerService],
  exports: [SwaggerService],
})
export class SwaggerModule {}

