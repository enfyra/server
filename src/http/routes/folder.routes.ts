import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerFolderRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/folder_definition/tree', async (req: any, res: Response) => {
    const folderTreeCache =
      req.scope?.cradle?.folderTreeCacheService ??
      container.cradle.folderTreeCacheService;
    const flat = req.query.flat === 'true';

    if (flat) {
      const folders = await folderTreeCache.getFolders();
      return res.json({ data: Array.from(folders.values()) });
    }

    const tree = await folderTreeCache.getTree();
    return res.json({ data: tree });
  });
}
