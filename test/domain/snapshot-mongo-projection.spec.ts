import { describe, expect, it } from 'vitest';
import snapshot from '../../src/data/snapshot';
import {
  MONGO_PRIMARY_KEY_TYPE,
  toMongoTypeForSqlType,
} from '../../src/shared/types/column-type.types';
import {
  isSupportedMongoColumnType,
  toMongoTargetSnapshot,
  toSqlTargetSnapshot,
} from '../../src/shared/utils/column-type.util';

describe('snapshot mongo projection', () => {
  it('declares a Mongo type for every column that matches the contract', () => {
    const mismatches: string[] = [];
    for (const [table, definition] of Object.entries(snapshot)) {
      for (const column of definition.columns) {
        const declared = column.mongoType;
        if (!declared) {
          mismatches.push(`${table}.${column.name}:missing-mongoType`);
          continue;
        }
        const expected = column.isPrimary
          ? MONGO_PRIMARY_KEY_TYPE
          : toMongoTypeForSqlType(column.sqlType.type);
        if (declared.type !== expected) {
          mismatches.push(
            `${table}.${column.name}:${column.sqlType.type}->${declared.type}, expected ${expected}`,
          );
        }
      }
    }
    expect(mismatches).toEqual([]);
  });

  it('projects every column to a supported type on both backends', () => {
    const offenders: string[] = [];
    const check = (
      projected: Record<string, any>,
      validate: (type: string) => boolean,
      label: string,
    ) => {
      for (const [table, definition] of Object.entries(projected)) {
        for (const column of (definition as any).columns ?? []) {
          if (!validate(column.type)) {
            offenders.push(`${label} ${table}.${column.name}:${column.type}`);
          }
          if ('mongoType' in column || 'sqlType' in column) {
            offenders.push(`${label} ${table}.${column.name}:leaked-declaration`);
          }
        }
      }
    };
    check(toMongoTargetSnapshot(snapshot as any), isSupportedMongoColumnType, 'mongo');
    check(toSqlTargetSnapshot(snapshot as any), () => true, 'sql');
    expect(offenders).toEqual([]);
  });
});
