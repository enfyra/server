import { Controller, Get, Param, NotFoundException } from '@nestjs/common';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';

@Controller('metadata')
export class MetadataController {
  constructor(private readonly metadataCacheService: MetadataCacheService) {}

  @Get()
  async getAllMetadata() {
    await this.metadataCacheService.waitForLoad(30000);

    const metadata = this.metadataCacheService.getDirectMetadata();
    if (!metadata) {
      throw new NotFoundException('Metadata not available');
    }

    return { data: metadata.tablesList };
  }

  @Get(':name')
  async getTableMetadata(@Param('name') name: string) {
    await this.metadataCacheService.waitForLoad(30000);

    const table = await this.metadataCacheService.lookupTableByName(name);
    if (!table) {
      throw new NotFoundException(`Table '${name}' not found`);
    }

    return { data: table };
  }
}