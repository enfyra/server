import type { RuntimeScriptExecutorService } from '../../../engines/cache/services/runtime-script-executor.service';
import type { Response } from 'express';
import type { RequestWithRouteData } from '../../../shared/types';
import type { DynamicCancellationService } from '../services/dynamic-cancellation.service';
import {
  bindDynamicHttpAbort,
  finishDynamicHttpFailure,
  getDynamicHttpFailure,
  recordDynamicClientDisconnect,
  recordDynamicExecutionFailure,
} from './dynamic-http-failure.util';

type RouteExecution = {
  deadline: number;
  controller: AbortController;
  expired: boolean;
  completed: boolean;
  activeBatches: number;
  cleanup: () => void;
};

const executions = new WeakMap<object, RouteExecution>();
const DEFAULT_ROUTE_TIMEOUT_MS = 30000;
const MAX_TIMEOUT_MS = 2147483647;

function timeoutError() {
  return Object.assign(
    new Error('The request did not complete before its timeout.'),
    {
      code: 'ERR_SCRIPT_EXECUTION_TIMEOUT',
      isTimeout: true,
    },
  );
}

export function beginDynamicRouteExecution(
  req: RequestWithRouteData,
  res: Response,
  cancellationService?: DynamicCancellationService,
): void {
  if (executions.has(req)) return;
  const configured = Number(req.routeData?.routeMethodConfig?.timeout);
  const timeoutMs =
    Number.isFinite(configured) && configured > 0
      ? Math.min(MAX_TIMEOUT_MS, Math.max(1, Math.trunc(configured)))
      : DEFAULT_ROUTE_TIMEOUT_MS;
  const controller = new AbortController();
  const unbindAbort = bindDynamicHttpAbort(res, controller);
  const state: RouteExecution = {
    deadline: performance.now() + timeoutMs,
    controller,
    expired: false,
    completed: false,
    activeBatches: 0,
    cleanup: () => {},
  };
  executions.set(req, state);
  const disconnect = () => {
    if (res.writableEnded) return;
    recordDynamicClientDisconnect(res);
    controller.abort();
  };
  const close = () => {
    disconnect();
    if (state.activeBatches === 0) completeDynamicRouteExecution(req);
  };
  const finish = () => {
    if (state.activeBatches === 0) completeDynamicRouteExecution(req);
  };
  const timer = setTimeout(() => {
    if (state.completed) return;
    state.expired = true;
    const failure = recordDynamicExecutionFailure(res, timeoutError());
    controller.abort();
    if (state.activeBatches === 0) {
      completeDynamicRouteExecution(req);
      const finishFailure = () => finishDynamicHttpFailure(res, failure);
      void Promise.resolve()
        .then(() => cancellationService?.run(req, failure))
        .then(finishFailure, finishFailure);
    }
  }, timeoutMs);
  timer.unref();
  state.cleanup = () => {
    clearTimeout(timer);
    unbindAbort();
    req.off?.('aborted', disconnect);
    res.off?.('close', close);
    res.off?.('finish', finish);
  };
  req.once?.('aborted', disconnect);
  res.once?.('close', close);
  res.once?.('finish', finish);
  if (req.aborted || res.destroyed) disconnect();
}

export function completeDynamicRouteExecution(req: object): void {
  const state = executions.get(req);
  if (!state || state.completed) return;
  state.completed = true;
  state.cleanup();
}

export async function runDynamicRouteBatch(
  req: RequestWithRouteData,
  res: Response,
  executor: Pick<RuntimeScriptExecutorService, 'runBatch'>,
): Promise<{ value: unknown; shortCircuit: boolean }> {
  beginDynamicRouteExecution(req, res);
  const state = executions.get(req)!;
  const remainingMs = Math.floor(state.deadline - performance.now());
  if (state.expired || remainingMs <= 0) {
    state.expired = true;
    recordDynamicExecutionFailure(res, timeoutError());
    state.controller.abort();
    throw timeoutError();
  }
  if (state.completed) {
    throw Object.assign(new Error('Route execution is already complete.'), {
      code: 'ERR_EXECUTION_ABORTED',
    });
  }
  state.activeBatches++;
  try {
    const result = await executor.runBatch(req, remainingMs, {
      signal: state.controller.signal,
    });
    if (state.expired) throw timeoutError();
    return result;
  } catch (error) {
    if (state.expired && getDynamicHttpFailure(res)?.origin === 'execution')
      throw timeoutError();
    throw error;
  } finally {
    state.activeBatches--;
    if (res.writableFinished && state.activeBatches === 0)
      completeDynamicRouteExecution(req);
  }
}
