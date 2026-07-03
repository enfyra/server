import { EventEmitter2 } from 'eventemitter2';
import { QueryBuilderService } from '@enfyra/kernel';
import { BaseCacheService, CacheConfig } from './base-cache.service';
import { RedisRuntimeCacheStore } from './redis-runtime-cache-store.service';
import { CACHE_IDENTIFIERS } from '../../../shared/utils/cache-events.constants';
import type { FolderNode, FolderTreeCache } from '../types/cache-data.types';

const FOLDER_TREE_CONFIG: CacheConfig = {
  cacheIdentifier: CACHE_IDENTIFIERS.FOLDER_TREE,
  colorCode: '\x1b[36m',
  cacheName: 'FolderTreeCache',
};

export class FolderTreeCacheService extends BaseCacheService<FolderTreeCache> {
  private readonly queryBuilderService: QueryBuilderService;

  constructor(deps: {
    queryBuilderService: QueryBuilderService;
    eventEmitter?: EventEmitter2;
    redisRuntimeCacheStore?: RedisRuntimeCacheStore;
  }) {
    super(FOLDER_TREE_CONFIG, deps.eventEmitter, deps.redisRuntimeCacheStore);
    this.queryBuilderService = deps.queryBuilderService;
  }

  protected async loadFromDb(): Promise<FolderNode[]> {
    const result = await this.queryBuilderService.find({
      table: 'enfyra_folder',
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
}
