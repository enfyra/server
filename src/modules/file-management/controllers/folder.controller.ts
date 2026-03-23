import { Controller, Get, Query } from '@nestjs/common';
import { FolderTreeCacheService } from '../../../infrastructure/cache/services/folder-tree-cache.service';

@Controller('folder_definition')
export class FolderController {
  constructor(private readonly folderTreeCache: FolderTreeCacheService) {}

  @Get('tree')
  async getFolderTree(@Query('flat') flat?: string) {
    if (flat === 'true') {
      const folders = await this.folderTreeCache.getFolders();
      return { data: Array.from(folders.values()) };
    }

    const tree = await this.folderTreeCache.getTree();
    return { data: tree };
  }
}
