import { describe, expect, it } from 'vitest';
import { FieldStripper } from '../../src/engines/knex/utils/field-stripper';
import { isGeneratedScriptPersistenceField } from '../../src/shared/utils/script-persistence-contract.util';

function metadataFor(tableName: string) {
  return {
    tables: new Map([
      [
        tableName,
        {
          name: tableName,
          columns: [
            { name: 'id', isPrimary: true, isUpdatable: false },
            { name: 'sourceCode', isUpdatable: true },
            { name: 'scriptLanguage', isUpdatable: true },
            { name: 'compiledCode', isUpdatable: false },
            { name: 'createdAt', isUpdatable: false },
          ],
        },
      ],
    ]),
  };
}

describe('script persistence contract', () => {
  it('marks compiledCode as an internal generated field only on script tables', () => {
    expect(
      isGeneratedScriptPersistenceField('enfyra_route_handler', 'compiledCode'),
    ).toBe(true);
    expect(
      isGeneratedScriptPersistenceField('article_definition', 'compiledCode'),
    ).toBe(false);
    expect(
      isGeneratedScriptPersistenceField('enfyra_route_handler', 'createdAt'),
    ).toBe(false);
  });

  it('keeps generated compiledCode while stripping other non-updatable fields', async () => {
    const stripper = new FieldStripper({
      requireMetadata: () => metadataFor('enfyra_route_handler'),
    } as any);

    const stripped = await stripper.stripNonUpdatableFields(
      'enfyra_route_handler',
      {
        id: 10,
        sourceCode: 'return @BODY.name;',
        scriptLanguage: 'typescript',
        compiledCode: 'return $ctx.$body.name;',
        createdAt: '2026-05-20T00:00:00.000Z',
      },
    );

    expect(stripped).toEqual({
      sourceCode: 'return @BODY.name;',
      scriptLanguage: 'typescript',
      compiledCode: 'return $ctx.$body.name;',
    });
  });

  it('does not keep compiledCode for non-script tables', async () => {
    const stripper = new FieldStripper({
      requireMetadata: () => metadataFor('article_definition'),
    } as any);

    const stripped = await stripper.stripNonUpdatableFields(
      'article_definition',
      {
        sourceCode: 'text',
        compiledCode: 'not-script',
      },
    );

    expect(stripped).toEqual({
      sourceCode: 'text',
    });
  });
});
