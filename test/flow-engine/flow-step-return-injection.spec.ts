import { executeSingle } from '../helpers/spawn-worker';

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
) {
  const snapshot = createSnapshot(ctx);
  const result = await executeSingle({ code, snapshot, timeoutMs, ctx });
  mergeCtxChanges(ctx, result.ctxChanges || {});
  return result.valueAbsent ? undefined : result.value;
}

describe('Flow step return injection', () => {
  it('step 1 return is accessible as $flow.step1 in step 2', async () => {
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

    const result1 = await runStep('return { v: 42 }', ctx);
    flowContext.step1 = result1;
    flowContext.$last = result1;

    expect(flowContext.step1).toEqual({ v: 42 });
    expect(flowContext.$last).toEqual({ v: 42 });

    const result2 = await runStep(
      'return { doubled: $ctx.$flow.step1.v * 2 }',
      ctx,
    );
    flowContext.step2 = result2;
    flowContext.$last = result2;

    expect(result2).toEqual({ doubled: 84 });
  });

  it('step 2 accesses $flow.$last from step 1', async () => {
    const flowContext: any = {
      $payload: { orderId: 99 },
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

    const result1 = await runStep(
      'return { orderId: $ctx.$flow.$payload.orderId, status: "pending" }',
      ctx,
    );
    flowContext.step1 = result1;
    flowContext.$last = result1;

    expect(result1).toEqual({ orderId: 99, status: 'pending' });

    const result2 = await runStep(
      'return { lastOrderId: $ctx.$flow.$last.orderId }',
      ctx,
    );
    flowContext.step2 = result2;
    flowContext.$last = result2;

    expect(result2).toEqual({ lastOrderId: 99 });
  });

  it('3-step chain propagates all intermediate results', async () => {
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

    const r1 = await runStep('return { n: 1 }', ctx);
    flowContext.step1 = r1;
    flowContext.$last = r1;

    const r2 = await runStep('return { n: $ctx.$flow.step1.n + 1 }', ctx);
    flowContext.step2 = r2;
    flowContext.$last = r2;

    const r3 = await runStep(
      'return { n: $ctx.$flow.step2.n + 1, all: [$ctx.$flow.step1.n, $ctx.$flow.step2.n, $ctx.$flow.$last.n] }',
      ctx,
    );
    flowContext.step3 = r3;
    flowContext.$last = r3;

    expect(r1).toEqual({ n: 1 });
    expect(r2).toEqual({ n: 2 });
    expect(r3).toEqual({ n: 3, all: [1, 2, 2] });
  });

  it('condition step result accessible in branch steps', async () => {
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

    const r1 = await runStep('return { value: 10 }', ctx);
    flowContext.step1 = r1;
    flowContext.$last = r1;

    const conditionResult = await runStep(
      'return $ctx.$flow.step1.value > 5',
      ctx,
    );
    flowContext.check = conditionResult;
    flowContext.$last = conditionResult;

    expect(conditionResult).toBe(true);

    const branchResult = await runStep(
      'return { parentWasTrue: $ctx.$flow.check === true, parentValue: $ctx.$flow.step1.value }',
      ctx,
    );
    flowContext.branch = branchResult;
    flowContext.$last = branchResult;

    expect(branchResult).toEqual({ parentWasTrue: true, parentValue: 10 });
  });
});
