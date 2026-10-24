import { afterEach, describe, expect, it } from 'vitest';
import { PreHookDefinitionProcessor } from '../../src/domain/bootstrap/processors/pre-hook-definition.processor';
import { PostHookDefinitionProcessor } from '../../src/domain/bootstrap/processors/post-hook-definition.processor';
import { DatabaseConfigService } from '../../src/shared/services';

afterEach(() => DatabaseConfigService.resetForTesting());

describe('hook bootstrap script persistence', () => {
  it.each([
    ['pre-hook', PreHookDefinitionProcessor],
    ['post-hook', PostHookDefinitionProcessor],
  ] as const)(
    'persists canonical script fields in %s',
    async (_name, Processor) => {
      DatabaseConfigService.overrideForTesting('postgres');
      const processor = new Processor({ queryBuilderService: {} as never });
      const [record] = await processor.transformRecords([
        { name: 'fixture', sourceCode: 'return 1;' },
      ]);
      expect(record).not.toHaveProperty('code');
      expect(record).toMatchObject({
        sourceCode: 'return 1;',
        scriptLanguage: 'typescript',
        compiledCode: expect.stringContaining('return 1;'),
      });
    },
  );
});
