import { Controller, Get, Param, NotFoundException } from '@nestjs/common';
import { MetadataCacheService } from '../../infrastructure/cache/services/metadata-cache.service';

@Controller('metadata')
export class MetadataController {
  constructor(private readonly metadataCacheService: MetadataCacheService) {}

  @Get()
  async getAllMetadata() {
    const metadata = await this.metadataCacheService.getMetadata();
    if (!metadata) {
      throw new NotFoundException('Metadata not available');
    }

    return { data: metadata.tablesList };
  }

  @Get(':name')
  async getTableMetadata(@Param('name') name: string) {
    const table = await this.metadataCacheService.getTableMetadata(name);
    if (!table) {
      throw new NotFoundException(`Table '${name}' not found`);
    }

    return { data: table };
  }
}