import { executeSingle } from '../helpers/spawn-worker';
import { transformCode } from 'src/kernel/execution';

function createSnapshot(ctx: Record<string, any>): Record<string, unknown> {
  const snapshot: Record<string, unknown> = {
    $body: ctx.$body,
    $query: ctx.$query,
    $params: ctx.$params,
    $user: ctx.$user,
    $share: ctx.$share,
    $data: ctx.$data,
    $api: { request: ctx.$api?.request },
  };
  const flow = ctx.$flow;
  if (flow !== undefined && flow !== null) {
    try {
      snapshot.$flow = JSON.parse(
        JSON.stringify(flow, (_k, val) =>
          typeof val === 'bigint' ? String(val) : val,
        ),
      );
    } catch {
      snapshot.$flow = {};
    }
  }
  return snapshot;
}

function mergeCtxChanges(
  ctx: Record<string, any>,
  changes: Record<string, any>,
) {
  if (!changes) return;
  if (changes.$body !== undefined) ctx.$body = changes.$body;
  if (changes.$query !== undefined) ctx.$query = changes.$query;
  if (changes.$params !== undefined) ctx.$params = changes.$params;
  if (changes.$data !== undefined) ctx.$data = changes.$data;
  if (changes.$share !== undefined) ctx.$share = changes.$share;
  if (
    changes.$flow !== undefined &&
    changes.$flow !== null &&
    typeof changes.$flow === 'object' &&
    ctx.$flow != null &&
    typeof ctx.$flow === 'object'
  ) {
    Object.assign(ctx.$flow, changes.$flow);
  }
}

async function runStep(
  code: string,
  ctx: Record<string, any>,
  timeoutMs = 5000,
  transform = false,
) {
  const finalCode = transform ? transformCode(code) : code;
  const snapshot = createSnapshot(ctx);
  const result = await executeSingle({
    code: finalCode,
    snapshot,
    timeoutMs,
    ctx,
  });
  mergeCtxChanges(ctx, result.ctxChanges || {});
  return result.valueAbsent ? undefined : result.value;
}

describe('Flow step return with macro transform', () => {
  it('@FLOW macro resolves step result correctly', async () => {
    const flowContext: any = {
      $payload: { value: 21 },
      $last: null,
      $meta: {
        flowId: 1,
        flowName: 'test',
        executionId: 'test',
        depth: 0,
        startedAt: new Date().toISOString(),
      },
    };
    const ctx: any = {
      $body: {},
      $query: {},
      $params: {},
      $user: null,
      $share: { $logs: [] },
      $flow: flowContext,
    };

    const r1 = await runStep(
      'return { v: @FLOW_PAYLOAD.value * 2 }',
      ctx,
      5000,
      true,
    );
    flowContext.step1 = r1;
    flowContext.$last = r1;
    expect(r1).toEqual({ v: 42 });

    const r2 = await runStep(
      'return { prev: @FLOW.step1.v, last: @FLOW_LAST.v }',
      ctx,
      5000,
      true,
    );
    flowContext.step2 = r2;
    flowContext.$last = r2;
    expect(r2).toEqual({ prev: 42, last: 42 });
  });

  it('step code transformed twice still works (double-transform safety)', async () => {
    const flowContext: any = {
      $payload: {},
      $last: null,
      $meta: {
        flowId: 1,
        flowName: 'test',
        executionId: 'test',
        depth: 0,
        startedAt: new Date().toISOString(),
      },
    };
    const ctx: any = {
      $body: {},
      $query: {},
      $params: {},
      $user: null,
      $share: { $logs: [] },
      $flow: flowContext,
    };

    const rawCode = 'return { v: 10 }';
    const transformedOnce = transformCode(rawCode);

    const r1 = await runStep(transformedOnce, ctx);
    flowContext.step1 = r1;
    flowContext.$last = r1;

    const r2 = await runStep(
      transformCode('return { doubled: @FLOW.step1.v * 2 }'),
      ctx,
    );
    flowContext.step2 = r2;
    flowContext.$last = r2;

    expect(r2).toEqual({ doubled: 20 });
  });

  it('step with no explicit return stores undefined in $flow', async () => {
    const flowContext: any = {
      $payload: {},
      $last: null,
      $meta: {
        flowId: 1,
        flowName: 'test',
        executionId: 'test',
        depth: 0,
        startedAt: new Date().toISOString(),
      },
    };
    const ctx: any = {
      $body: {},
      $query: {},
      $params: {},
      $user: null,
      $share: { $logs: [] },
      $flow: flowContext,
    };

    const r1 = await runStep('const x = 42', ctx);
    flowContext.step1 = r1;
    flowContext.$last = r1;

    expect(r1).toBeUndefined();

    const r2 = await runStep(
      'return { hasStep1: $ctx.$flow.hasOwnProperty("step1"), step1IsUndefined: $ctx.$flow.step1 === undefined }',
      ctx,
    );
    flowContext.step2 = r2;

    expect(r2.hasStep1).toBe(false);
    expect(r2.step1IsUndefined).toBe(true);
  });
});
