import { describe, expect, it } from 'vitest';
import { ValidationException } from '../../src/domain/exceptions';
import { TableManagementValidationService } from '../../src/modules/table-management/services/table-validation.service';
import {
  MYSQL_RESERVED_COLUMN_IDENTIFIERS,
  POSTGRES_RESERVED_COLUMN_IDENTIFIERS,
} from '../../src/modules/table-management/utils/sql-identifier-validation.util';

const service = new TableManagementValidationService();

describe('TableManagementValidationService column identifiers', () => {
  it.each([
    ['postgres', POSTGRES_RESERVED_COLUMN_IDENTIFIERS],
    ['mysql', MYSQL_RESERVED_COLUMN_IDENTIFIERS],
  ] as const)(
    'rejects every reserved %s identifier before mutation',
    (dbType, keywords) => {
      for (const keyword of keywords) {
        expect(() =>
          service.validateColumns([{ name: keyword, type: 'varchar' }], dbType),
        ).toThrow(ValidationException);
        expect(() =>
          service.validateColumns([{ name: keyword, type: 'varchar' }], dbType),
        ).toThrow(/reserved keyword/i);
      }
    },
  );

  it.each(['postgres', 'mysql', 'mongodb'] as const)(
    'accepts ordinary identifiers for %s',
    (dbType) => {
      expect(() =>
        service.validateColumns(
          [
            { name: 'modelName', type: 'varchar' },
            { name: 'upstream_model', type: 'varchar' },
            { name: '_private', type: 'varchar' },
          ],
          dbType,
        ),
      ).not.toThrow();
    },
  );

  it.each(['postgres', 'mysql', 'mongodb'] as const)(
    'rejects structurally invalid identifiers for %s',
    (dbType) => {
      for (const name of ['1model', 'model-name', 'model name', '']) {
        expect(() =>
          service.validateColumns([{ name, type: 'varchar' }], dbType),
        ).toThrow(ValidationException);
      }
    },
  );

  it('does not apply SQL keyword restrictions to MongoDB', () => {
    expect(() =>
      service.validateColumns(
        [
          { name: 'as', type: 'varchar' },
          { name: 'select', type: 'varchar' },
        ],
        'mongodb',
      ),
    ).not.toThrow();
  });

  it.each(['postgres', 'mysql', 'mongodb'] as const)(
    'requires non-empty unique string options for %s enum columns',
    (dbType) => {
      for (const options of [
        undefined,
        [],
        ['active', 1],
        ['active', 'active'],
      ]) {
        expect(() =>
          service.validateColumns(
            [{ name: 'status', type: 'enum', options }],
            dbType,
          ),
        ).toThrow(/enum options/i);
      }

      expect(() =>
        service.validateColumns(
          [
            {
              name: 'status',
              type: 'enum',
              options: ['active', 'inactive'],
            },
          ],
          dbType,
        ),
      ).not.toThrow();
    },
  );

  it.each(['postgres', 'mysql', 'mongodb'] as const)(
    'requires the %s enum default to belong to its options',
    (dbType) => {
      expect(() =>
        service.validateColumns(
          [
            {
              name: 'status',
              type: 'enum',
              options: ['active', 'inactive'],
              defaultValue: 'pending',
            },
          ],
          dbType,
        ),
      ).toThrow(/enum default/i);
    },
  );
});
