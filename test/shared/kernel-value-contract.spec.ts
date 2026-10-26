import { createRequire } from 'node:module';
import { runInNewContext } from 'node:vm';
import { expect, it } from 'vitest';
import { Long } from 'mongodb';
import { encodeMainThreadToIsolate } from '../../../kernel/src/execution/executor-engine/utils/host-value-codec';
import { normalizeAggregateQuery } from '../../../kernel/src/query/query-builder/utils/aggregate-query.util';
import { normalizeMongoDocument } from '../../../kernel/src/query/utils/mongo-runtime.util';

const require = createRequire(import.meta.url);
const codec = require('../../../kernel/src/execution/executor-engine/workers/setup/context-codec.js');

it('preserves reserved-envelope-shaped host business data and exact binary windows', () => {
  const bytes = Uint8Array.from([99, 1, 2, 88]);
  const payload = { nested: { __e: 'u' }, bigint: 9223372036854775807n, explicit: undefined, binary: bytes.buffer, view: bytes.subarray(1, 3), number: NaN, map: new Map([['key', { __bigint: 'business' }]]) };
  const encoded = encodeMainThreadToIsolate(payload);
  const decoded = runInNewContext(`${codec}\n__unwrapMainThreadPayload(JSON.parse(input))`, { input: encoded });
  expect(decoded.nested).toEqual({ __e: 'u' });
  expect(decoded.bigint).toBe(9223372036854775807n);
  expect(Object.hasOwn(decoded, 'explicit')).toBe(true);
  expect(Array.from(new Uint8Array(decoded.binary))).toEqual([99, 1, 2, 88]);
  expect(Array.from(decoded.view)).toEqual([1, 2]);
  expect(Number.isNaN(decoded.number)).toBe(true);
  expect(decoded.map.get('key')).toEqual({ __bigint: 'business' });
});

it.each(['long', 'bigint'])('accepts %s aggregate measures without rounding exact output', type => {
  const metadata = { tables: new Map([['records', { columns: [{ name: 'amount', type }] }]]) };
  expect(() => normalizeAggregateQuery({ measures: { total: { sum: 'amount' } } }, 'records', metadata)).not.toThrow();
  expect(normalizeMongoDocument({ total: Long.fromString('9223372036854775807') })).toEqual({ total: '9223372036854775807' });
});
