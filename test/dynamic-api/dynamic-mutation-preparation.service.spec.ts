import { describe, expect, it, vi } from 'vitest';
import { DynamicMutationPreparationService } from '../../src/modules/dynamic-api/services/dynamic-mutation-preparation.service';

describe('DynamicMutationPreparationService', () => {
  it('removes only non-updatable fields and preserves explicit encrypted updates', () => {
    const service = new DynamicMutationPreparationService();

    expect(
      service.prepareUpdateBody(
        {
          immutableCode: 'preserve',
          encryptedSecret: '',
          encryptedNullable: null,
          title: 'published',
        },
        {
          columns: [
            { name: 'immutableCode', isUpdatable: false },
            { name: 'encryptedSecret', isEncrypted: true, type: 'text' },
            {
              name: 'encryptedNullable',
              isEncrypted: true,
              isNullable: true,
              type: 'text',
            },
          ],
        },
      ),
    ).toEqual({
      encryptedSecret: '',
      encryptedNullable: null,
      title: 'published',
    });
  });

  it('preserves an explicit clear for an unpublished unencrypted column', () => {
    const service = new DynamicMutationPreparationService();

    expect(
      service.prepareUpdateBody(
        {
          pinnedSeller: null,
          privateNote: '',
        },
        {
          columns: [
            {
              name: 'pinnedSeller',
              isPublished: false,
              isEncrypted: false,
              isNullable: true,
              type: 'varchar',
            },
            {
              name: 'privateNote',
              isPublished: false,
              isEncrypted: false,
              isNullable: true,
              type: 'text',
            },
          ],
        },
      ),
    ).toEqual({ pinnedSeller: null, privateNote: '' });
  });

  it('rejects legacy throw APIs only when new script source is written', () => {
    const service = new DynamicMutationPreparationService();

    expect(() =>
      service.normalizeCreate('enfyra_route_handler', {
        sourceCode: '$ctx.$throw.notFound("Project");',
        scriptLanguage: 'javascript',
      }),
    ).toThrow('$throw exposes only .http(statusCode, message?)');

    expect(() =>
      service.normalizeUpdate(
        'enfyra_route_handler',
        { sourceCode: '@THROW404("missing");' },
        { sourceCode: '$ctx.$throw.notFound("Project");' },
      ),
    ).not.toThrow();

    expect(() =>
      service.normalizeUpdate(
        'enfyra_route_handler',
        { description: 'unchanged source' },
        { sourceCode: '$ctx.$throw.notFound("Project");' },
      ),
    ).not.toThrow();

    expect(() =>
      service.normalizeFlowStep({
        type: 'script',
        sourceCode: '$ctx.$throw.notFound("Project");',
      }),
    ).toThrow('$throw exposes only .http(statusCode, message?)');
    expect(() =>
      service.normalizeFlowStep({
        type: 'script',
        sourceCode: '@THROW404("missing");',
      }),
    ).not.toThrow();
  });

  it('rejects retired flow step types and any config key on the write body', () => {
    const service = new DynamicMutationPreparationService();

    expect(() => service.normalizeFlowStep({ type: 'sleep' })).toThrow(
      'Flow steps are script-only',
    );
    expect(() => service.normalizeFlowStep({ type: 'query' })).toThrow(
      'Flow steps are script-only',
    );
    expect(() =>
      service.normalizeFlowStep({
        type: 'script',
        sourceCode: 'return 1;',
        config: { table: 'enfyra_user', limit: 5 },
      }),
    ).toThrow('Flow steps have no config column');
    expect(() =>
      service.normalizeFlowStep({
        type: 'script',
        sourceCode: 'return 1;',
        config: { code: 'return 2;' },
      }),
    ).toThrow('Flow steps have no config column');

    const normalized = service.normalizeFlowStep({
      type: 'script',
      sourceCode: 'return 1;',
    });
    expect(normalized.sourceCode).toBe('return 1;');
    expect(normalized.compiledCode).toBe('return 1;');
  });

  it('preserves supplied private and encrypted values during create', async () => {
    const service = new DynamicMutationPreparationService();
    const mutationAuthorizationService = {
      stripUnauthorizedDirectFields: vi.fn(async (_action, body) => body),
      assertMutationSafety: vi.fn().mockResolvedValue(undefined),
    };
    const tableValidationService = {
      assertTableValid: vi.fn().mockResolvedValue(undefined),
    };

    const body = await service.prepareCreateBody(
      {
        privateValue: null,
        encryptedSecret: '',
      },
      'articles',
      {
        columns: [
          { name: 'privateValue', isPublished: false },
          { name: 'encryptedSecret', isEncrypted: true },
        ],
      },
      mutationAuthorizationService,
      tableValidationService,
    );

    expect(body).toEqual({ privateValue: null, encryptedSecret: '' });
    expect(
      mutationAuthorizationService.stripUnauthorizedDirectFields,
    ).toHaveBeenCalledWith('create', {
      privateValue: null,
      encryptedSecret: '',
    });
  });
});
