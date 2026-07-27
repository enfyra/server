import { createHash } from 'node:crypto';
import type {
  SchemaMutationContract,
  SchemaMutationContractInput,
  SchemaMutationLogicalChange,
  SchemaMutationNodeOutputs,
  SchemaMutationOutputReference,
} from '../types/schema-mutation-contract.types';
import {
  assertSchemaMutationPlan,
  buildSchemaMutationExecutionPhases,
} from './schema-mutation-plan.util';

export function compileSchemaMutationContract<
  TContext,
  TChange extends SchemaMutationLogicalChange,
  TCommand,
>(
  input: SchemaMutationContractInput<TContext, TChange, TCommand>,
): SchemaMutationContract<TContext, TChange, TCommand> {
  const normalized = cloneCanonical(input);
  assertContractHeader(normalized);
  assertSchemaMutationPlan(normalized.changes, normalized.nodes);
  const phases = buildSchemaMutationExecutionPhases(normalized.nodes);
  const { nodes: _nodes, ...contractFields } = normalized;
  const contract = { ...contractFields, phases };
  return deepFreeze({ ...contract, contractHash: hashCanonical(contract) });
}

export function createSchemaMutationOutputReference(
  nodeId: string,
  ...path: string[]
): SchemaMutationOutputReference {
  if (!nodeId || path.length === 0 || path.some((segment) => !segment)) {
    throw new Error(
      'Schema mutation output references require a node and path.',
    );
  }
  return deepFreeze({
    $schemaMutationOutput: { nodeId, path: [...path] },
  });
}

export function resolveSchemaMutationOutputReferences<T>(
  value: T,
  outputs: SchemaMutationNodeOutputs,
): T {
  return resolveValue(value, outputs) as T;
}

export function hashCanonical(value: unknown): string {
  return createHash('sha256').update(stringifyCanonical(value)).digest('hex');
}

export function verifySchemaMutationContractHash<
  TContext,
  TChange extends SchemaMutationLogicalChange,
  TCommand,
>(contract: SchemaMutationContract<TContext, TChange, TCommand>): boolean {
  const { contractHash, ...contractFields } = contract;
  return contractHash === hashCanonical(contractFields);
}

function assertContractHeader(input: SchemaMutationContractInput): void {
  if (!input.mutationId.trim() || !input.idempotencyKey.trim()) {
    throw new Error(
      'Schema mutation contracts require mutation and idempotency ids.',
    );
  }
}

function cloneCanonical<T>(value: T): T {
  return JSON.parse(stringifyCanonical(value)) as T;
}

function stringifyCanonical(value: unknown): string {
  const visiting = new Set<object>();
  const encode = (current: unknown): string => {
    if (current === null) return 'null';
    if (typeof current === 'string') return JSON.stringify(current);
    if (typeof current === 'boolean') return current ? 'true' : 'false';
    if (typeof current === 'number') {
      if (!Number.isFinite(current)) {
        throw new Error('Schema mutation contracts require finite numbers.');
      }
      return JSON.stringify(current);
    }
    if (
      typeof current === 'undefined' ||
      typeof current === 'function' ||
      typeof current === 'symbol' ||
      typeof current === 'bigint'
    ) {
      throw new Error(
        `Schema mutation contracts do not support ${typeof current} values.`,
      );
    }

    const object = current as object;
    if (visiting.has(object)) {
      throw new Error('Schema mutation contracts cannot contain cycles.');
    }
    visiting.add(object);
    try {
      if (Array.isArray(current)) {
        return `[${current.map((item) => encode(item)).join(',')}]`;
      }
      if (
        Object.getPrototypeOf(current) !== Object.prototype &&
        Object.getPrototypeOf(current) !== null
      ) {
        throw new Error(
          'Schema mutation contracts support only plain objects and arrays.',
        );
      }
      const record = current as Record<string, unknown>;
      return `{${Object.keys(record)
        .sort()
        .map((key) => `${JSON.stringify(key)}:${encode(record[key])}`)
        .join(',')}}`;
    } finally {
      visiting.delete(object);
    }
  };
  return encode(value);
}

function resolveValue(
  value: unknown,
  outputs: SchemaMutationNodeOutputs,
): unknown {
  if (!value || typeof value !== 'object') return value;
  if (isOutputReference(value)) {
    const { nodeId, path } = value.$schemaMutationOutput;
    let resolved: unknown = outputs.get(nodeId);
    if (!resolved) {
      throw new Error(`Schema mutation output is missing for node ${nodeId}.`);
    }
    for (const segment of path) {
      if (!resolved || typeof resolved !== 'object' || !(segment in resolved)) {
        throw new Error(
          `Schema mutation output ${nodeId}.${path.join('.')} is missing.`,
        );
      }
      resolved = (resolved as Record<string, unknown>)[segment];
    }
    return resolved;
  }
  if (Array.isArray(value)) {
    return value.map((item) => resolveValue(item, outputs));
  }
  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [
      key,
      resolveValue(nested, outputs),
    ]),
  );
}

function isOutputReference(
  value: object,
): value is SchemaMutationOutputReference {
  const candidate = (value as Partial<SchemaMutationOutputReference>)
    .$schemaMutationOutput;
  return (
    !!candidate &&
    Object.keys(value).length === 1 &&
    typeof candidate.nodeId === 'string' &&
    Array.isArray(candidate.path)
  );
}

function deepFreeze<T>(value: T): T {
  if (!value || typeof value !== 'object' || Object.isFrozen(value)) {
    return value;
  }
  for (const nested of Object.values(value as Record<string, unknown>)) {
    deepFreeze(nested);
  }
  return Object.freeze(value);
}
