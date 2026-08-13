import { describe, expect, it } from 'vitest';
import { buildContainer } from '../../src/container';
import {
  MongoTableHandlerService,
  SqlTableHandlerService,
  TableHandlerService,
} from '../../src/modules/table-management';

describe('table-management registrations', () => {
  it('resolves the table handlers through the typed Awilix cradle', async () => {
    const container = buildContainer();

    try {
      expect(container.resolve('tableHandlerService')).toBeInstanceOf(
        TableHandlerService,
      );
      expect(container.resolve('sqlTableHandlerService')).toBeInstanceOf(
        SqlTableHandlerService,
      );
      expect(container.resolve('mongoTableHandlerService')).toBeInstanceOf(
        MongoTableHandlerService,
      );
    } finally {
      await container.dispose();
    }
  });
});
