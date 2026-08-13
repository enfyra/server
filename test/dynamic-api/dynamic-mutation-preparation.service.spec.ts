import { describe, expect, it } from 'vitest';
import { DynamicMutationPreparationService } from '../../src/modules/dynamic-api/services/dynamic-mutation-preparation.service';

describe('DynamicMutationPreparationService', () => {
  it('removes non-updatable fields and empty unpublished fields', () => {
    const service = new DynamicMutationPreparationService();

    expect(
      service.prepareUpdateBody(
        {
          immutableCode: 'preserve',
          unpublishedNote: '',
          title: 'published',
        },
        {
          columns: [
            { name: 'immutableCode', isUpdatable: false },
            { name: 'unpublishedNote', isPublished: false, type: 'text' },
          ],
        },
      ),
    ).toEqual({ title: 'published' });
  });
});
