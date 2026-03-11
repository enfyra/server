import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';
import { Public } from '../../../shared/decorators/public-route.decorator';
import { GraphqlService } from '../services/graphql.service';

@Controller('graphql-schema')
export class GraphqlSchemaController {
  constructor(private readonly graphqlService: GraphqlService) {}

  @Public()
  @Get()
  getSchema(@Res() res: Response) {
    try {
      const sdl = this.graphqlService.getSchemaSdl();
      res.setHeader('Content-Type', 'text/plain; charset=utf-8');
      res.send(sdl);
    } catch (error: any) {
      res.status(503).send(error?.message ?? 'GraphQL schema not available');
    }
  }
}
