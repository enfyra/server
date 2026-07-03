import type { Express, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerFolderRoutes(
  app: Express,
  container: AwilixContainer<Cradle>,
) {
  app.get('/enfyra_folder/tree', async (req: any, res: Response) => {
    const runtimeRegistryService =
      req.scope?.cradle?.runtimeRegistryService ??
      container.cradle.runtimeRegistryService;
    const flat = req.query.flat === 'true';

    if (flat) {
      const folders = runtimeRegistryService.getFolders();
      return res.json({ data: Array.from(folders.values()) });
    }

    const tree = runtimeRegistryService.getFolderTree();
    return res.json({ data: tree });
  });
}
