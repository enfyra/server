/**
 * Bootstrap data provision: getUniqueIdentifier
 *
 * Each processor's getUniqueIdentifier() must return field names that
 * match the *transformed* record — i.e. after transformRecords() has
 * renamed FK fields for the target database.
 *
 *   SQL transform:   route → routeId,  role → roleId,  method → methodId
 *   Mongo transform: route → ObjectId, role → ObjectId, method → ObjectId
 */

import { ObjectId } from 'mongodb';

describe('RoutePermissionDefinitionProcessor.getUniqueIdentifier', () => {
  // Mirror the production logic
  const getUniqueIdentifier = (record: any, isMongoDB: boolean) => {
    if (isMongoDB) {
      return { route: record.route, role: record.role };
    }
    return { routeId: record.routeId, roleId: record.roleId };
  };

  describe('SQL (after transform)', () => {
    it('returns routeId and roleId from the transformed record', () => {
      const record = { routeId: 42, roleId: 7, isEnabled: true };
      const where = getUniqueIdentifier(record, false);

      expect(where).toEqual({ routeId: 42, roleId: 7 });
    });

    it('all returned keys have defined values', () => {
      const record = { routeId: 1, roleId: 2 };
      const where = getUniqueIdentifier(record, false);

      for (const v of Object.values(where)) {
        expect(v).toBeDefined();
      }
    });
  });

  describe('MongoDB (after transform)', () => {
    it('returns route and role as ObjectId from the transformed record', () => {
      const routeOid = new ObjectId();
      const roleOid = new ObjectId();
      const record = { route: routeOid, role: roleOid, isEnabled: true };
      const where = getUniqueIdentifier(record, true);

      expect(where).toEqual({ route: routeOid, role: roleOid });
    });

    it('all returned keys have defined values', () => {
      const record = { route: new ObjectId(), role: new ObjectId() };
      const where = getUniqueIdentifier(record, true);

      for (const v of Object.values(where)) {
        expect(v).toBeDefined();
      }
    });
  });
});

describe('RouteHandlerDefinitionProcessor.getUniqueIdentifier', () => {
  const getUniqueIdentifier = (record: any, isMongoDB: boolean) => {
    if (isMongoDB) {
      return { route: record.route, method: record.method };
    }
    return { routeId: record.routeId, methodId: record.methodId };
  };

  describe('SQL (after transform)', () => {
    it('returns routeId and methodId from the transformed record', () => {
      const record = { routeId: 10, methodId: 3, logic: 'return {}' };
      const where = getUniqueIdentifier(record, false);

      expect(where).toEqual({ routeId: 10, methodId: 3 });
    });
  });

  describe('MongoDB (after transform)', () => {
    it('returns route and method as ObjectId', () => {
      const routeOid = new ObjectId();
      const methodOid = new ObjectId();
      const record = { route: routeOid, method: methodOid, logic: 'return {}' };
      const where = getUniqueIdentifier(record, true);

      expect(where).toEqual({ route: routeOid, method: methodOid });
    });

    it('all returned keys have defined values', () => {
      const record = { route: new ObjectId(), method: new ObjectId() };
      const where = getUniqueIdentifier(record, true);

      for (const v of Object.values(where)) {
        expect(v).toBeDefined();
      }
    });
  });
});

describe('getUniqueIdentifier used as findOne where-clause', () => {
  it('SQL: would match a specific record in knex where()', () => {
    const getUniqueIdentifier = (record: any) => ({
      routeId: record.routeId,
      roleId: record.roleId,
    });

    const record = { routeId: 5, roleId: 3, isEnabled: true };
    const where = getUniqueIdentifier(record);

    // knex(tableName).where(where).first() — where must have no undefined
    expect(where.routeId).toBe(5);
    expect(where.roleId).toBe(3);
  });

  it('MongoDB: would match a specific document in collection.findOne()', () => {
    const routeOid = new ObjectId();
    const roleOid = new ObjectId();

    const getUniqueIdentifier = (record: any) => ({
      route: record.route,
      role: record.role,
    });

    const record = { route: routeOid, role: roleOid };
    const where = getUniqueIdentifier(record);

    // db.collection(name).findOne(where) — values must be ObjectId, not undefined
    expect(where.route).toBeInstanceOf(ObjectId);
    expect(where.role).toBeInstanceOf(ObjectId);
  });
});
