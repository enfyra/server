import {
  validateFilterShape,
  assertFieldOperatorValueIsClean,
} from '../../src/infrastructure/query-builder/utils/shared/filter-sanitizer.util';
import { DatabaseConfigService } from '../../src/shared/services/database-config.service';

describe('Filter Sanitizer — MongoDB _id in relation filter', () => {
  const metadata = {
    tables: new Map([
      [
        'route_handler_definition',
        {
          columns: [
            { name: '_id', type: 'ObjectId' },
            { name: 'logic', type: 'string' },
          ],
          relations: [
            {
              propertyName: 'route',
              type: 'many-to-one',
              targetTableName: 'route_definition',
            },
            {
              propertyName: 'method',
              type: 'many-to-one',
              targetTableName: 'method_definition',
            },
          ],
        },
      ],
      [
        'post_hook_definition',
        {
          columns: [
            { name: '_id', type: 'ObjectId' },
            { name: 'name', type: 'string' },
          ],
          relations: [
            {
              propertyName: 'route',
              type: 'many-to-one',
              targetTableName: 'route_definition',
            },
          ],
        },
      ],
      [
        'route_definition',
        {
          columns: [
            { name: '_id', type: 'ObjectId' },
            { name: 'path', type: 'string' },
          ],
          relations: [],
        },
      ],
    ]),
  };

  describe('SQL mode', () => {
    beforeAll(() => DatabaseConfigService.overrideForTesting('mysql'));
    afterAll(() => DatabaseConfigService.resetForTesting());
    it('rejects _id as operator inside relation filter', () => {
      const filter = { route: { _id: '507f1f77bcf86cd799439011' } };
      expect(() => {
        validateFilterShape(filter, 'route_handler_definition', metadata);
      }).toThrow('Unsupported filter operator "_id"');
    });

    it('assertFieldOperatorValueIsClean rejects _id', () => {
      expect(() => {
        assertFieldOperatorValueIsClean(
          'route',
          { _id: 'abc123' },
          'route_handler_definition',
        );
      }).toThrow('Unsupported filter operator "_id"');
    });
  });

  describe('MongoDB mode', () => {
    beforeAll(() => DatabaseConfigService.overrideForTesting('mongodb'));
    afterAll(() => DatabaseConfigService.resetForTesting());

    it('allows _id inside relation filter value (the fix)', () => {
      const filter = { route: { _id: { _eq: '507f1f77bcf86cd799439011' } } };
      expect(() => {
        validateFilterShape(filter, 'route_handler_definition', metadata);
      }).not.toThrow();
    });

    it('allows _id inside post_hook relation filter', () => {
      const filter = { route: { _id: { _eq: '507f1f77bcf86cd799439011' } } };
      expect(() => {
        validateFilterShape(filter, 'post_hook_definition', metadata);
      }).not.toThrow();
    });

    it('assertFieldOperatorValueIsClean allows _id on MongoDB', () => {
      expect(() => {
        assertFieldOperatorValueIsClean(
          'route',
          { _id: { _eq: 'abc123' } },
          'route_handler_definition',
        );
      }).not.toThrow();
    });

    it('still rejects unknown _ operators on MongoDB', () => {
      expect(() => {
        assertFieldOperatorValueIsClean(
          'route',
          { _invalid: 'abc' },
          'route_handler_definition',
        );
      }).toThrow('Unsupported filter operator "_invalid"');
    });

    it('accepts { route: { _eq: ObjectId } } (correct form)', () => {
      const filter = { route: { _eq: '507f1f77bcf86cd799439011' } };
      expect(() => {
        validateFilterShape(filter, 'route_handler_definition', metadata);
      }).not.toThrow();
    });
  });
});
