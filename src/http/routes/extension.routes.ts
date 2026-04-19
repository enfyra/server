import type { Express, Request, Response } from 'express';
import type { AwilixContainer } from 'awilix';
import type { Cradle } from '../../container';
import { BadRequestException } from '../../core/exceptions/custom-exceptions';

export function registerExtensionRoutes(app: Express, container: AwilixContainer<Cradle>) {
  app.post('/extension_definition/preview', async (req: any, res: Response) => {
    const body = req.body;
    if (!body?.code || typeof body.code !== 'string') {
      throw new BadRequestException('Code is required');
    }

    const { processExtensionDefinition } = await import('../../modules/extension-definition/utils/processor.util');
    const { buildExtensionWithVite } = await import('../../modules/extension-definition/utils/compiler.util');
    const {
      isProbablyVueSFC,
      assertValidVueSFC,
      assertValidJsBundleSyntax,
    } = await import('../../modules/extension-definition/utils/validation.util');

    const code = body.code;
    const extensionId = body.id || body.name || `preview_${Date.now()}`;

    if (isProbablyVueSFC(code)) {
      assertValidVueSFC(code);
      const compiledCode = await buildExtensionWithVite(code, extensionId);
      return res.json({ success: true, compiledCode, extensionId });
    }

    assertValidJsBundleSyntax(code);
    res.json({ success: true, compiledCode: code, extensionId });
  });

  app.post('/extension_definition', async (req: any, res: Response) => {
    const { processExtensionDefinition } = await import('../../modules/extension-definition/utils/processor.util');
    const { processedBody } = await processExtensionDefinition(req.body, 'POST');

    if (req.routeData?.context) {
      req.routeData.context.$body = processedBody;
    } else {
      req.body = processedBody;
    }

    const dynamicService = req.scope?.cradle?.dynamicService ?? container.cradle.dynamicService;
    const result = await dynamicService.runHandler(req);
    res.json(result);
  });

  app.patch('/extension_definition/:id', async (req: any, res: Response) => {
    const { processExtensionDefinition } = await import('../../modules/extension-definition/utils/processor.util');
    const { processedBody } = await processExtensionDefinition(req.body, 'PATCH');

    if (req.routeData?.context) {
      req.routeData.context.$body = processedBody;
    } else {
      req.body = processedBody;
    }

    const dynamicService = req.scope?.cradle?.dynamicService ?? container.cradle.dynamicService;
    const result = await dynamicService.runHandler(req);
    res.json(result);
  });
}
