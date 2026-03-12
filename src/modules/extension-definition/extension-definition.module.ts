import { Module } from '@nestjs/common';
import { ExtensionDefinitionController } from './extension-definition.controller';

@Module({
  controllers: [ExtensionDefinitionController],
})
export class ExtensionDefinitionModule {}
