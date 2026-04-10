/**
 * Bootstrap auto processor utilities — snapshot-driven FK transform,
 * unique identifier, and compare fields.
 */

import { ObjectId } from 'mongodb';
import {
  getManyToOneRelations,
  getScalarColumns,
  getUniqueFields,
  getLookupKey,
} from '../../src/core/bootstrap/utils/snapshot-meta.util';

// ─── snapshot-meta.util ─────────────────────────────────────────

describe('snapshot-meta.util', () => {
  describe('getManyToOneRelations', () => {
    it('returns M2O relations for route_handler_definition', () => {
      const rels = getManyToOneRelations('route_handler_definition');
      const names = rels.map((r) => r.propertyName);
      expect(names).toContain('route');
      expect(names).toContain('method');
      expect(rels.every((r) => r.type === 'many-to-one')).toBe(true);
    });

    it('returns M2O relations for route_permission_definition', () => {
      const rels = getManyToOneRelations('route_permission_definition');
      const names = rels.map((r) => r.propertyName);
      expect(names).toContain('role');
      expect(names).toContain('route');
    });

    it('returns M2O relations for user_definition', () => {
      const rels = getManyToOneRelations('user_definition');
      expect(rels.some((r) => r.propertyName === 'role')).toBe(true);
    });

    it('returns M2O relations for flow_step_definition', () => {
      const rels = getManyToOneRelations('flow_step_definition');
      expect(rels.some((r) => r.propertyName === 'flow')).toBe(true);
    });

    it('does not include one-to-many or many-to-many relations', () => {
      const rels = getManyToOneRelations('route_definition');
      const types = rels.map((r) => r.type);
      expect(types.every((t) => t === 'many-to-one')).toBe(true);
      const names = rels.map((r) => r.propertyName);
      expect(names).not.toContain('preHooks');
      expect(names).not.toContain('availableMethods');
    });

    it('returns empty array for non-existent table', () => {
      expect(getManyToOneRelations('nonexistent_table')).toEqual([]);
    });

    it('includes correct targetTable for each relation', () => {
      const rels = getManyToOneRelations('route_handler_definition');
      const routeRel = rels.find((r) => r.propertyName === 'route');
      const methodRel = rels.find((r) => r.propertyName === 'method');
      expect(routeRel?.targetTable).toBe('route_definition');
      expect(methodRel?.targetTable).toBe('method_definition');
    });
  });

  describe('getLookupKey', () => {
    it('returns "path" for route_definition', () => {
      expect(getLookupKey('route_definition')).toBe('path');
    });

    it('returns "name" for table_definition', () => {
      expect(getLookupKey('table_definition')).toBe('name');
    });

    it('returns "method" for method_definition', () => {
      expect(getLookupKey('method_definition')).toBe('method');
    });

    it('returns "name" for role_definition', () => {
      expect(getLookupKey('role_definition')).toBe('name');
    });

    it('falls back to "name" for unknown tables', () => {
      expect(getLookupKey('unknown_table')).toBe('name');
    });
  });

  describe('getScalarColumns', () => {
    it('returns scalar columns for route_handler_definition', () => {
      const cols = getScalarColumns('route_handler_definition');
      expect(cols).toContain('logic');
      expect(cols).toContain('description');
      expect(cols).not.toContain('id');
      expect(cols).not.toContain('createdAt');
      expect(cols).not.toContain('updatedAt');
    });

    it('returns scalar columns for flow_step_definition', () => {
      const cols = getScalarColumns('flow_step_definition');
      expect(cols).toContain('key');
      expect(cols).toContain('stepOrder');
      expect(cols).toContain('type');
      expect(cols).toContain('config');
      expect(cols).toContain('timeout');
    });

    it('returns empty for non-existent table', () => {
      expect(getScalarColumns('nonexistent')).toEqual([]);
    });
  });

  describe('getUniqueFields', () => {
    it('returns uniques for route_handler_definition', () => {
      const uniques = getUniqueFields('route_handler_definition');
      expect(uniques.length).toBeGreaterThan(0);
      expect(uniques[0]).toEqual(['route', 'method']);
    });

    it('returns uniques for websocket_event_definition', () => {
      const uniques = getUniqueFields('websocket_event_definition');
      expect(uniques[0]).toEqual(['gateway', 'eventName']);
    });

    it('returns empty for table without uniques', () => {
      const uniques = getUniqueFields('nonexistent');
      expect(uniques).toEqual([]);
    });
  });
});

// ─── autoTransformFkFields ──────────────────────────────────────

describe('BaseTableProcessor.autoTransformFkFields', () => {
  // Import and instantiate a real processor to test the base class method
  const { BaseTableProcessor } = require('../../src/core/bootstrap/processors/base-table-processor');

  class TestProcessor extends BaseTableProcessor {
    getUniqueIdentifier(record: any) {
      return { name: record.name };
    }
  }

  const processor = new TestProcessor();

  it('transforms M2O FK fields for SQL (propertyName → propertyNameId)', async () => {
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mysql';

    const mockQb = {
      findOneWhere: jest.fn(async (_table: string, where: any) => {
        if (where.path === '/tasks') return { id: 42, path: '/tasks' };
        if (where.method === 'GET') return { id: 1, method: 'GET' };
        return null;
      }),
    };

    const record = { route: '/tasks', method: 'GET', logic: 'return {}' };
    const result = await processor.autoTransformFkFields(
      record,
      'route_handler_definition',
      mockQb,
    );

    expect(result.routeId).toBe(42);
    expect(result.methodId).toBe(1);
    expect(result.route).toBeUndefined();
    expect(result.method).toBeUndefined();
    expect(result.logic).toBe('return {}');

    process.env.DB_TYPE = oldEnv;
  });

  it('transforms M2O FK fields for MongoDB (keeps propertyName, value → ObjectId)', async () => {
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mongodb';

    const oid = new ObjectId();
    const mockQb = {
      findOneWhere: jest.fn(async () => ({ _id: oid })),
    };

    const record = { route: '/tasks', method: 'GET', logic: 'return {}' };
    const result = await processor.autoTransformFkFields(
      record,
      'route_handler_definition',
      mockQb,
    );

    expect(result.route).toBeInstanceOf(ObjectId);
    expect(result.method).toBeInstanceOf(ObjectId);
    expect(result.routeId).toBeUndefined();
    expect(result.logic).toBe('return {}');

    process.env.DB_TYPE = oldEnv;
  });

  it('skips non-string FK values (already transformed)', async () => {
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mongodb';

    const existingOid = new ObjectId();
    const mockQb = { findOneWhere: jest.fn() };

    const record = { route: existingOid, logic: 'x' };
    const result = await processor.autoTransformFkFields(
      record,
      'route_handler_definition',
      mockQb,
    );

    expect(result.route).toBe(existingOid);
    expect(mockQb.findOneWhere).not.toHaveBeenCalled();

    process.env.DB_TYPE = oldEnv;
  });

  it('warns and skips when target record not found', async () => {
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mysql';

    const mockQb = { findOneWhere: jest.fn(async () => null) };

    const record = { route: '/nonexistent', logic: 'x' };
    const result = await processor.autoTransformFkFields(
      record,
      'route_handler_definition',
      mockQb,
    );

    // Route was not resolved, original value kept
    expect(result.route).toBe('/nonexistent');
    expect(result.routeId).toBeUndefined();

    process.env.DB_TYPE = oldEnv;
  });
});

// ─── autoGetUniqueIdentifier ────────────────────────────────────

describe('BaseTableProcessor.autoGetUniqueIdentifier', () => {
  const { BaseTableProcessor } = require('../../src/core/bootstrap/processors/base-table-processor');

  class TestProcessor extends BaseTableProcessor {
    getUniqueIdentifier(record: any) {
      return this.autoGetUniqueIdentifier(record, 'route_handler_definition');
    }
  }

  const processor = new TestProcessor();

  it('SQL: returns FK fields with Id suffix for relation-based uniques', () => {
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mysql';

    const record = { routeId: 42, methodId: 1, logic: 'x' };
    const result = processor.getUniqueIdentifier(record);

    expect(result).toEqual({ routeId: 42, methodId: 1 });

    process.env.DB_TYPE = oldEnv;
  });

  it('MongoDB: returns relation fields as-is (ObjectId)', () => {
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mongodb';

    const routeOid = new ObjectId();
    const methodOid = new ObjectId();
    const record = { route: routeOid, method: methodOid, logic: 'x' };
    const result = processor.getUniqueIdentifier(record);

    expect(result).toEqual({ route: routeOid, method: methodOid });

    process.env.DB_TYPE = oldEnv;
  });

  it('handles mixed unique keys (relation + scalar)', () => {
    const { BaseTableProcessor: BP } = require('../../src/core/bootstrap/processors/base-table-processor');

    class WsProcessor extends BP {
      getUniqueIdentifier(record: any) {
        return this.autoGetUniqueIdentifier(record, 'websocket_event_definition');
      }
    }

    const proc = new WsProcessor();
    const oldEnv = process.env.DB_TYPE;
    process.env.DB_TYPE = 'mysql';

    const record = { gatewayId: 5, eventName: 'message' };
    const result = proc.getUniqueIdentifier(record);
    expect(result).toEqual({ gatewayId: 5, eventName: 'message' });

    process.env.DB_TYPE = oldEnv;
  });
});

// ─── autoGetCompareFields ───────────────────────────────────────

describe('BaseTableProcessor.autoGetCompareFields', () => {
  const { BaseTableProcessor } = require('../../src/core/bootstrap/processors/base-table-processor');

  class TestProcessor extends BaseTableProcessor {
    getUniqueIdentifier() {
      return {};
    }
    getFields(tableName: string) {
      return this.autoGetCompareFields(tableName);
    }
  }

  const processor = new TestProcessor();

  it('returns all scalar columns except id/timestamps for flow_step_definition', () => {
    const fields = processor.getFields('flow_step_definition');
    expect(fields).toContain('key');
    expect(fields).toContain('stepOrder');
    expect(fields).toContain('type');
    expect(fields).toContain('config');
    expect(fields).toContain('isEnabled');
    expect(fields).not.toContain('id');
    expect(fields).not.toContain('createdAt');
    expect(fields).not.toContain('updatedAt');
  });

  it('returns all scalar columns for websocket_event_definition', () => {
    const fields = processor.getFields('websocket_event_definition');
    expect(fields).toContain('eventName');
    expect(fields).toContain('isEnabled');
    expect(fields).toContain('handlerScript');
    expect(fields).toContain('timeout');
  });
});
