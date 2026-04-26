import { BadRequestException } from '../../../domain/exceptions';
import { autoAssignExtensionName } from './naming.util';
import {
  isProbablyVueSFC,
  assertValidVueSFC,
  assertValidJsBundleSyntax,
} from './validation.util';
import { buildExtensionWithVite } from './compiler.util';

export async function processExtensionDefinition(
  body: any,
  method: string,
): Promise<{ processedBody: any }> {
  if (method !== 'POST' && method !== 'PATCH') {
    return { processedBody: body };
  }

  if (!body || typeof body.code !== 'string') {
    return { processedBody: body };
  }

  body = autoAssignExtensionName(body);

  const code: string = body.code;
  const extensionId = body.id || body.name || 'extension_' + Date.now();

  if (isProbablyVueSFC(code)) {
    assertValidVueSFC(code);

    try {
      const compiledCode = await buildExtensionWithVite(code, body.extensionId);
      body.compiledCode = compiledCode;
      return { processedBody: body };
    } catch (compileError: any) {
      const message =
        compileError?.message ||
        `Failed to build Vue SFC for ${extensionId}: ${compileError?.message || 'Unknown error'}`;
      throw new BadRequestException(message);
    }
  } else {
    assertValidJsBundleSyntax(code);
    return { processedBody: body };
  }
}
