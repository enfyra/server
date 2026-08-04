import {
  compileSchemaMutationContract,
  hashCanonical,
  verifySchemaMutationContractHash,
} from '../../src/shared/utils/schema-mutation-contract.util';

function buildInput() {
  return {
    contractVersion: 1,
    mutationId: 'mutation-1',
    idempotencyKey: 'request-1',
    backend: 'postgresql' as const,
    origin: 'runtime' as const,
    context: { target: { name: 'posts' }, sourceRevision: 'revision-1' },
    changes: [
      { id: 'change:create-table', kind: 'table', label: 'create posts' },
    ],
    nodes: [
      {
        id: 'metadata:create-table',
        changeId: 'change:create-table',
        dependsOn: [],
        completesChange: false,
        command: { kind: 'create-metadata' },
      },
      {
        id: 'physical:create-table',
        changeId: 'change:create-table',
        dependsOn: ['metadata:create-table'],
        completesChange: true,
        command: { kind: 'create-physical', tableId: 'generated-id-42' },
      },
    ],
  };
}

describe('schema mutation contract', () => {
  it('compiles an immutable contract with a deterministic canonical hash', () => {
    const input = buildInput();
    const contract = compileSchemaMutationContract(input);
    const reordered = compileSchemaMutationContract({
      ...input,
      context: {
        sourceRevision: 'revision-1',
        target: { name: 'posts' },
      },
    });

    expect(contract.contractHash).toBe(reordered.contractHash);
    expect(verifySchemaMutationContractHash(contract)).toBe(true);
    expect(contract.phases.map((phase) => phase.index)).toEqual([0, 1]);
    expect(Object.isFrozen(contract)).toBe(true);
    expect(Object.isFrozen(contract.context)).toBe(true);
    expect(Object.isFrozen(contract.phases[1].nodes[0].command)).toBe(true);
  });

  it('rejects values that cannot be represented by the durable contract', () => {
    expect(() => hashCanonical({ unsafe: undefined })).toThrow(
      /do not support undefined/,
    );
    expect(() => hashCanonical({ unsafe: Number.POSITIVE_INFINITY })).toThrow(
      /finite numbers/,
    );
    expect(() => hashCanonical({ unsafe: new Date() })).toThrow(
      /plain objects and arrays/,
    );
  });
});
