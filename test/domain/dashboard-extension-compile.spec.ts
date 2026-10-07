import { describe, expect, it } from 'vitest';
import { dashboardExtension } from '../../src/data/dashboard-extension';
import { processExtensionDefinition } from '../../src/modules/extension-definition/utils/processor.util';

describe('Fresh dashboard Vue source', () => {
  it('compiles through the production extension compiler without a prebuilt bundle', async () => {
    expect(dashboardExtension).not.toHaveProperty('compiledCode');
    const { processedBody } = await processExtensionDefinition(
      { ...dashboardExtension },
      'POST',
    );
    expect(processedBody.compiledCode).toBeTruthy();
    expect(processedBody.compiledCode).not.toBe(dashboardExtension.code);
    expect(processedBody.code).toBe(dashboardExtension.code);
  }, 30_000);
});
