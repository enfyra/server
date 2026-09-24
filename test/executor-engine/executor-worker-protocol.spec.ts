import { Worker } from 'worker_threads';

const WORKER_SCRIPT = require.resolve('@enfyra/kernel/execution/worker.js');

const snapshot = {
  $body: {},
  $query: {},
  $params: {},
  $user: null,
  $share: {},
  $api: { request: {} },
};

function executionMessage(id: string, code: string, timeoutMs = 5_000) {
  return {
    type: 'execute',
    id,
    code,
    pkgSources: [],
    snapshot,
    timeoutMs,
    memoryLimitMb: 128,
    isolatePoolSize: 1,
    tasksPerIsolate: 2,
  };
}

function encoded(value: unknown) {
  return JSON.stringify({ __e: 'v', d: value });
}

describe('executor worker protocol integrity', () => {
  it('rejects a duplicate task id without disturbing the active task', async () => {
    const worker = new Worker(WORKER_SCRIPT);
    const taskId = 'duplicate-active-task';

    try {
      const observed = await new Promise<{
        protocolError: any;
        result: any;
        resultCount: number;
      }>((resolve, reject) => {
        let protocolError: any;
        let result: any;
        let resultCount = 0;
        let duplicateSent = false;
        const timer = setTimeout(
          () => reject(new Error('Timed out waiting for duplicate task result')),
          5_000,
        );

        const finish = () => {
          if (!protocolError || !result) return;
          setTimeout(() => {
            clearTimeout(timer);
            resolve({ protocolError, result, resultCount });
          }, 50);
        };

        worker.on('message', (message) => {
          if (message.type === 'repoCall' && message.id === taskId) {
            if (duplicateSent) return;
            duplicateSent = true;
            worker.postMessage(executionMessage(taskId, 'return "duplicate";'));
            setTimeout(() => {
              worker.postMessage({
                type: 'callResult',
                callId: message.callId,
                result: encoded('original'),
              });
            }, 10);
            return;
          }
          if (message.type === 'protocolError') {
            protocolError = message;
            finish();
            return;
          }
          if (message.type === 'result' && message.id === taskId) {
            resultCount++;
            result = message;
            finish();
          }
        });
        worker.on('error', reject);
        worker.postMessage(
          executionMessage(
            taskId,
            'return await $ctx.$repos.main.waitForRelease();',
          ),
        );
      });

      expect(observed.protocolError).toMatchObject({
        code: 'ERR_EXECUTOR_DUPLICATE_TASK_ID',
      });
      expect(observed.result).toMatchObject({
        id: taskId,
        success: true,
        value: 'original',
      });
      expect(observed.resultCount).toBe(1);
    } finally {
      await worker.terminate();
    }
  });

  it('keeps host-owned IPC envelope fields authoritative', async () => {
    const worker = new Worker(WORKER_SCRIPT);
    const taskId = 'authoritative-envelope-task';

    try {
      const observed = await new Promise<{ call: any; result: any }>(
        (resolve, reject) => {
          let call: any;
          const timer = setTimeout(
            () => reject(new Error('Timed out waiting for authoritative envelope')),
            5_000,
          );

          worker.on('message', (message) => {
            if (message.type === 'repoCall') {
              call = message;
              worker.postMessage({
                type: 'callResult',
                callId: message.callId,
                result: encoded('safe'),
              });
              return;
            }
            if (message.type === 'result' && message.id === taskId) {
              clearTimeout(timer);
              resolve({ call, result: message });
              return;
            }
            if (message.id === 'forged-task' || message.callId === 'forged-call') {
              clearTimeout(timer);
              reject(new Error('Sandbox payload overrode an IPC envelope field'));
            }
          });
          worker.on('error', reject);
          worker.postMessage(
            executionMessage(
              taskId,
              `
                return await __call('repoCall', JSON.stringify({
                  type: 'result',
                  id: 'forged-task',
                  callId: 'forged-call',
                  table: 'main',
                  method: 'read',
                  argsJson: '[]'
                }));
              `,
            ),
          );
        },
      );

      expect(observed.call).toMatchObject({
        type: 'repoCall',
        id: taskId,
        table: 'main',
        method: 'read',
      });
      expect(observed.call.callId).not.toBe('forged-call');
      expect(observed.result).toMatchObject({
        id: taskId,
        success: true,
        value: 'safe',
      });
    } finally {
      await worker.terminate();
    }
  });

  it('emits one terminal result per task when an isolate failure races a sibling', async () => {
    const worker = new Worker(WORKER_SCRIPT);
    const parkedId = 'terminal-race-parked';
    const failingId = 'terminal-race-failing';

    try {
      const results = await new Promise<Map<string, any[]>>((resolve, reject) => {
        const byTask = new Map<string, any[]>([
          [parkedId, []],
          [failingId, []],
        ]);
        let failingSent = false;
        const timer = setTimeout(
          () => reject(new Error('Timed out waiting for isolate race results')),
          7_000,
        );

        const finish = () => {
          if (
            (byTask.get(parkedId)?.length ?? 0) < 1
            || (byTask.get(failingId)?.length ?? 0) < 1
          ) {
            return;
          }
          setTimeout(() => {
            clearTimeout(timer);
            resolve(byTask);
          }, 100);
        };

        worker.on('message', (message) => {
          if (
            message.type === 'repoCall'
            && message.id === parkedId
            && !failingSent
          ) {
            failingSent = true;
            worker.postMessage(
              executionMessage(failingId, 'while (true) {}', 100),
            );
            return;
          }
          if (message.type === 'result' && byTask.has(message.id)) {
            byTask.get(message.id)?.push(message);
            finish();
          }
        });
        worker.on('error', reject);
        worker.postMessage(
          executionMessage(
            parkedId,
            'return await $ctx.$repos.main.waitForever();',
            300,
          ),
        );
      });

      expect(results.get(parkedId)).toHaveLength(1);
      // A plain V8-watchdog timeout leaves the lane usable, so the parked
      // sibling is not collaterally killed: it stays alive through the other
      // task's timeout and is failed only by its own budget's host-callback
      // backstop. Isolate loss is reserved for a disposed isolate.
      expect(results.get(parkedId)?.[0]).toMatchObject({ success: false });
      expect(results.get(parkedId)?.[0]?.error?.code).not.toBe(
        'ERR_EXECUTOR_ISOLATE_LOST',
      );
      expect(results.get(failingId)).toHaveLength(1);
      expect(results.get(failingId)?.[0]).toMatchObject({
        success: false,
        error: { code: 'ERR_SCRIPT_EXECUTION_TIMEOUT' },
      });
    } finally {
      await worker.terminate();
    }
  });
});
