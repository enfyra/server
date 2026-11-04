import { describe, expect, it } from 'vitest';
import {
  isSupportedMongoColumnType,
  isSupportedMySqlColumnType,
  isSupportedPostgresColumnType,
  toMongoTypeForSqlType,
} from '../../src/shared/types/column-type.types';
import { generateColumnDefinition } from '../../src/engines/knex/utils/migration/sql-generator';
import { sqlTypeToBsonType } from '../../src/engines/mongo/utils/mongo-validation-schema.util';
import { coerceTemporalContractValue, isCalendarDate, isUtcDateTime } from '../../src/shared/utils/temporal-write.util';
import { BootstrapDefinitionService } from '../../src/engines/bootstrap/services/bootstrap-definition.service';
import { validateSnapshotMigrationCoverage } from '../../src/engines/bootstrap/utils/metadata-migration.util';

describe('calendar date and UTC datetime schema contract', () => {
  it.each([isSupportedPostgresColumnType, isSupportedMySqlColumnType, isSupportedMongoColumnType])(
    'accepts date and datetime and rejects timestamp',
    (isSupported) => {
      expect(isSupported('date')).toBe(true);
      expect(isSupported('datetime')).toBe(true);
      expect(isSupported('timestamp')).toBe(false);
    },
  );

  it.each(['postgres', 'mysql'] as const)(
    'maps calendar dates to a native SQL DATE on %s',
    (backend) => {
      expect(generateColumnDefinition({ name: 'day', type: 'date' }, backend)).toBe('DATE');
    },
  );

  it('keeps Mongo calendar dates distinct from BSON datetime instants', () => {
    expect(toMongoTypeForSqlType('date')).toBe('date');
    expect(toMongoTypeForSqlType('datetime')).toBe('datetime');
    expect(sqlTypeToBsonType('date')).toBe('string');
    expect(sqlTypeToBsonType('datetime')).toBe('date');
  });

  it('validates calendar days independently of instants', () => {
    expect(isCalendarDate('2024-02-29')).toBe(true);
    expect(isCalendarDate('0000-01-01')).toBe(false);
    expect(isCalendarDate('2023-02-29')).toBe(false);
    expect(isCalendarDate('2026-10-27T00:00:00.000Z')).toBe(false);
    expect(coerceTemporalContractValue('2024-02-29', 'date')).toBe('2024-02-29');
    expect(() => coerceTemporalContractValue('2024-02-30', 'date')).toThrow();
  });

  it('requires UTC instants with at most millisecond precision', () => {
    expect(isUtcDateTime('2026-10-27T00:00:00.123Z')).toBe(true);
    for (const value of ['0000-01-01T00:00:00Z', new Date('0000-01-01T00:00:00Z'), '2026-10-27', '2026-10-27T00:00:00+07:00', '2026-02-30T00:00:00Z', '2026-10-27T00:00:00.123456Z']) {
      expect(isUtcDateTime(value)).toBe(false);
      expect(() => coerceTemporalContractValue(value, 'datetime')).toThrow();
    }
  });

  it.each(['postgres', 'mysql', 'mongodb'])('covers a genuine 2.3.3 temporal metadata source on %s', (dbType) => {
    const service = new BootstrapDefinitionService({ databaseConfigService: { getDbType: () => dbType } as any });
    const definition = service.resolveForVersion('2.3.3');
    const target = definition.snapshot.enfyra_session.columns.find((column: any) => column.name === 'expiredAt');
    expect(target.type).toBe('datetime');
    const state = { tables: [], columns: [{ ...target, tableName: 'enfyra_session', type: 'date' }], relations: [] };
    expect(() => validateSnapshotMigrationCoverage(definition.snapshot, definition.migration, state, definition.dataTargetSnapshot)).not.toThrow();
    expect(() => validateSnapshotMigrationCoverage(definition.snapshot, null, state, definition.dataTargetSnapshot)).toThrow(/updates type without migration/);
  });

  it('declares the 2.4.0 legacy instant migration for both backends', () => {
    for (const dbType of ['postgres', 'mongodb']) {
      const service = new BootstrapDefinitionService({ databaseConfigService: { getDbType: () => dbType } as any });
      service.resolveForVersion('2.3.3');
      const step = service.getApplicableSchemaSteps().find((entry) => entry.toVersion === '2.4.0');
      const mappings = dbType === 'mongodb' ? step?.schema.mongoColumnTypesToModify : step?.schema.sqlColumnTypesToModify;
      expect(mappings).toEqual(expect.arrayContaining([{ from: 'date', to: 'datetime' }, { from: 'timestamp', to: 'datetime' }]));
    }
  });
});
