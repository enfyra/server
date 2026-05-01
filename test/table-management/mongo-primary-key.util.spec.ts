import { describe, expect, it } from 'vitest';
import {
  MONGO_PRIMARY_KEY_NAME,
  MONGO_PRIMARY_KEY_TYPE,
  isMongoPrimaryKeyType,
  normalizeMongoPrimaryKeyColumn,
} from '../../src/modules/table-management/utils/mongo-primary-key.util';

describe('Mongo primary key metadata', () => {
  it('normalizes legacy primary id columns to Mongo _id ObjectId metadata', () => {
    expect(
      normalizeMongoPrimaryKeyColumn({
        name: 'id',
        type: 'uuid',
        isPrimary: true,
        isGenerated: true,
      }),
    ).toEqual({
      name: MONGO_PRIMARY_KEY_NAME,
      type: MONGO_PRIMARY_KEY_TYPE,
      isPrimary: true,
      isGenerated: true,
    });
  });

  it('requires the canonical ObjectId primary key type for Mongo metadata', () => {
    expect(isMongoPrimaryKeyType('ObjectId')).toBe(true);
    expect(isMongoPrimaryKeyType('objectId')).toBe(false);
    expect(isMongoPrimaryKeyType('uuid')).toBe(false);
    expect(isMongoPrimaryKeyType('int')).toBe(false);
  });
});
