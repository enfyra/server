import {
  BadRequestException,
  Body,
  Controller,
  Patch,
  Post,
  Req,
} from '@nestjs/common';
import { Request } from 'express';
import { DynamicService } from '../dynamic-api/services/dynamic.service';
import { processExtensionDefinition } from './utils/processor.util';
import { buildExtensionWithVite } from './utils/compiler.util';
import {
  isProbablyVueSFC,
  assertValidVueSFC,
  assertValidJsBundleSyntax,
} from './utils/validation.util';

@Controller('extension_definition')
export class ExtensionDefinitionController {
  constructor(private readonly dynamicService: DynamicService) {}

  @Post('preview')
  async preview(@Body() body: { code?: string; id?: string; name?: string }) {
    if (!body?.code || typeof body.code !== 'string') {
      throw new BadRequestException('Code is required');
    }
    const code = body.code;
    const extensionId = body.id || body.name || `preview_${Date.now()}`;
    if (isProbablyVueSFC(code)) {
      assertValidVueSFC(code);
      const compiledCode = await buildExtensionWithVite(code, extensionId);
      return { success: true, compiledCode, extensionId };
    }
    assertValidJsBundleSyntax(code);
    return { success: true, compiledCode: code, extensionId };
  }

  @Post()
  async create(@Req() req: Request & { routeData?: any }, @Body() body: any) {
    const { processedBody } = await processExtensionDefinition(body, 'POST');
    if (req.routeData?.context) {
      req.routeData.context.$body = processedBody;
    } else {
      (req as any).body = processedBody;
    }
    return this.dynamicService.runHandler(req);
  }

  @Patch(':id')
  async update(@Req() req: Request & { routeData?: any }, @Body() body: any) {
    const { processedBody } = await processExtensionDefinition(body, 'PATCH');
    if (req.routeData?.context) {
      req.routeData.context.$body = processedBody;
    } else {
      (req as any).body = processedBody;
    }
    return this.dynamicService.runHandler(req);
  }
}
