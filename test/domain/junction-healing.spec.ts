import { ObjectId } from 'mongodb';
import { describe, expect, it } from 'vitest';
import {
  resolveJunctionColumns,
  resolveJunctionIdentity,
} from '../../src/engines/bootstrap/utils/junction-healing.util';

const input = {
  sourceTable: 'users',
  targetTable: 'roles',
  propertyName: 'roles',
  junctionTableName: 'j_target',
  junctionSourceColumn: 'sourceId',
  junctionTargetColumn: 'targetId',
  previousSourceColumn: 'userId',
  previousTargetColumn: 'roleId',
};

describe('junction convergence preflight', () => {
  it('maps persisted and table-derived inputs onto one canonical contract', () => {
    expect(resolveJunctionColumns(input, ['usersId', 'rolesId'])).toMatchObject(
      { sourceColumn: 'usersId', targetColumn: 'rolesId' },
    );
    expect(resolveJunctionColumns(input, ['userId', 'roleId'])).toMatchObject({
      sourceColumn: 'userId',
      targetColumn: 'roleId',
    });
  });
  it('rejects ambiguous self-referencing mappings instead of choosing a direction', () => {
    expect(() =>
      resolveJunctionColumns(
        {
          ...input,
          sourceTable: 'users',
          targetTable: 'users',
          previousSourceColumn: null,
          previousTargetColumn: null,
        },
        ['usersId'],
      ),
    ).toThrow(/ambiguous or missing/);
  });
  it('keeps equal BSON identities and rejects conflicting aliases', () => {
    const id = new ObjectId();
    expect(
      resolveJunctionIdentity(
        { sourceId: id, userId: new ObjectId(id.toHexString()) },
        ['sourceId', 'userId'],
        'sourceId',
      ),
    ).toBe(id);
    expect(() =>
      resolveJunctionIdentity(
        { sourceId: 1, userId: 2 },
        ['sourceId', 'userId'],
        'sourceId',
      ),
    ).toThrow(/conflicting/);
    expect(() => resolveJunctionIdentity({}, ['sourceId'], 'sourceId')).toThrow(
      /missing/,
    );
  });
});
