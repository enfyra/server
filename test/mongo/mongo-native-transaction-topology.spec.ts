import { mongoTopologySupportsNativeTransactions } from '../../src/engines/mongo';

describe('mongoTopologySupportsNativeTransactions', () => {
  it('returns true when hello has non-empty setName (replica set)', () => {
    expect(
      mongoTopologySupportsNativeTransactions({ setName: 'rs0', ok: 1 }),
    ).toBe(true);
  });

  it('returns false when setName is empty string', () => {
    expect(
      mongoTopologySupportsNativeTransactions({ setName: '', ok: 1 }),
    ).toBe(false);
  });

  it('returns true when hello.msg is isdbgrid (mongos)', () => {
    expect(
      mongoTopologySupportsNativeTransactions({ msg: 'isdbgrid', ok: 1 }),
    ).toBe(true);
  });

  it('returns false for standalone-like hello without setName or mongos', () => {
    expect(
      mongoTopologySupportsNativeTransactions({
        isWritablePrimary: true,
        ok: 1,
      }),
    ).toBe(false);
  });

  it('returns false for null or non-object', () => {
    expect(mongoTopologySupportsNativeTransactions(null)).toBe(false);
    expect(mongoTopologySupportsNativeTransactions(undefined)).toBe(false);
  });
});
