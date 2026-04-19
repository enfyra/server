import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';

export function registerGraphqlSchemaRoutes(app: Express, container: AwilixContainer<Cradle>) {
  app.get('/graphql-schema', async (req: any, res: Response) => {
    const graphqlService = req.scope?.cradle?.graphqlService ?? container.cradle.graphqlService;
    try {
      const sdl = graphqlService.getSchemaSdl();
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.send(sdl);
    } catch (error: any) {
      res.status(503).send(error?.message ?? 'GraphQL schema not available');
    }
  });
}
