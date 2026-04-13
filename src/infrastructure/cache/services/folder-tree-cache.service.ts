import { Injectable } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { QueryBuilderService } from '../../query-builder/query-builder.service';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import {
  CACHE_IDENTIFIERS,
} from '../../../shared/utils/cache-events.constants';

const FOLDER_TREE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.FOLDER_TREE,
  colorCode: '\x1b[36m',
  cacheName: 'FolderTreeCache',
};

interface FolderNode {
  id: string;
  parentId: string | null;
  name: string;
  slug: string;
  order: number;
  icon: string;
  description: string | null;
  children?: FolderNode[];
}

interface FolderTreeCache {
  folders: Map<string, FolderNode>;
  tree: FolderNode[];
}

@Injectable()
export class FolderTreeCacheService extends BaseCacheService<FolderTreeCache> {
  constructor(
    private readonly queryBuilder: QueryBuilderService,
    eventEmitter: EventEmitter2,
  ) {
    super(FOLDER_TREE_CONFIG, eventEmitter);
  }

  protected async loadFromDb(): Promise<FolderNode[]> {
    const result = await this.queryBuilder.find({
      table: 'folder_definition',
      fields: ['id', 'name', 'slug', 'order', 'icon', 'description', 'parent'],
      sort: ['order'],
    });

    return result.data.map((f: any) => ({
      id: f.id,
      name: f.name,
      slug: f.slug,
      order: f.order,
      icon: f.icon,
      description: f.description,
      parentId: f.parent?.id ?? null,
    }));
  }

  protected transformData(rawFolders: FolderNode[]): FolderTreeCache {
    const folders = new Map<string, FolderNode>();
    rawFolders.forEach((f) => folders.set(f.id, f));

    return {
      folders,
      tree: this.buildTree(rawFolders),
    };
  }

  private buildTree(folders: FolderNode[]): FolderNode[] {
    const folderMap = new Map<
      string,
      FolderNode & { children?: FolderNode[] }
    >();

    folders.forEach((f) => {
      folderMap.set(f.id, { ...f, children: [] });
    });

    const roots: (FolderNode & { children?: FolderNode[] })[] = [];

    folderMap.forEach((folder) => {
      if (!folder.parentId) {
        roots.push(folder);
      } else {
        const parent = folderMap.get(folder.parentId);
        if (parent) {
          parent.children?.push(folder);
        }
      }
    });

    const sortNodes = (nodes: (FolderNode & { children?: FolderNode[] })[]) => {
      nodes.sort((a, b) => (a.order ?? 0) - (b.order ?? 0));
      nodes.forEach((node) => {
        if (node.children?.length) {
          sortNodes(node.children);
        }
      });
      return nodes;
    };

    return sortNodes(roots);
  }

  protected getLogCount(): string {
    return `${this.cache.folders.size} folders`;
  }

  // --- Public methods ---

  async getTree(): Promise<FolderNode[]> {
    await this.ensureLoaded();
    return this.cache.tree;
  }

  async getFolders(): Promise<Map<string, FolderNode>> {
    await this.ensureLoaded();
    return this.cache.folders;
  }

  async isCircular(
    folderId: string | null,
    newParentId: string | null,
  ): Promise<boolean> {
    await this.ensureLoaded();

    if (!folderId) return false;
    if (!newParentId) return false;

    const visited = new Set<string>();
    let currentId: string | null = newParentId;

    while (currentId) {
      if (currentId === folderId) {
        return true;
      }

      if (visited.has(currentId)) {
        break;
      }

      visited.add(currentId);
      const folder = this.cache.folders.get(currentId);
      currentId = folder?.parentId ?? null;
    }

    return false;
  }
}
