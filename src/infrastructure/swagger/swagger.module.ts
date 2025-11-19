import { Global, Module } from '@nestjs/common';
import { SwaggerService } from './services/swagger.service';
import { SwaggerController } from './controllers/swagger.controller';

@Global()
@Module({
  controllers: [SwaggerController],
  providers: [SwaggerService],
  exports: [SwaggerService],
})
export class SwaggerModule {}

