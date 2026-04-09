import { encodeMainThreadToIsolate } from '../../src/infrastructure/executor-engine/services/isolated-executor.service';

function unwrapMain(v: unknown): unknown {
  if (
    v !== null &&
    typeof v === 'object' &&
    (v as { __e?: string }).__e === 'u'
  )
    return undefined;
  if (
    v !== null &&
    typeof v === 'object' &&
    (v as { __e?: string }).__e === 'v'
  )
    return unwrapMain((v as { d: unknown }).d);
  return v;
}

function decodeLikeIsolateWorker(s: string | object): unknown {
  const w = typeof s === 'string' ? JSON.parse(s) : s;
  return unwrapMain(w);
}

describe('encodeMainThreadToIsolate (repo/helper/cache bridge)', () => {
  it('undefined distinct from null', () => {
    expect(
      decodeLikeIsolateWorker(encodeMainThreadToIsolate(undefined)),
    ).toBeUndefined();
    expect(decodeLikeIsolateWorker(encodeMainThreadToIsolate(null))).toBeNull();
  });

  it('bigint becomes string in JSON', () => {
    expect(
      decodeLikeIsolateWorker(
        encodeMainThreadToIsolate(BigInt('9007199254740993')),
      ),
    ).toBe('9007199254740993');
  });

  it('plain object round-trips', () => {
    const o = { a: 1, nested: { b: 'x' } };
    expect(decodeLikeIsolateWorker(encodeMainThreadToIsolate(o))).toEqual(o);
  });

  it('double envelope unwraps like worker', () => {
    const inner = { data: [1, 2] };
    const once = JSON.parse(encodeMainThreadToIsolate(inner));
    const twice = { __e: 'v', d: once };
    expect(decodeLikeIsolateWorker(twice as object)).toEqual(inner);
  });

  it('object payload without JSON.parse string (copyInto shape)', () => {
    const inner = { rows: [{ id: 1 }] };
    const env = JSON.parse(encodeMainThreadToIsolate(inner)) as object;
    expect(decodeLikeIsolateWorker(env)).toEqual(inner);
  });
});
