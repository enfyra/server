'use strict';

const { parentPort } = require('worker_threads');

parentPort.on('message', (msg) => {
  if (msg.type !== 'execute' && msg.type !== 'executeBatch') return;
  const { id, triggerHeapRatio, delayMs } = msg;
  const ratio = typeof triggerHeapRatio === 'number' ? triggerHeapRatio : 0;
  const delay = typeof delayMs === 'number' ? delayMs : 0;

  const send = () => {
    parentPort.postMessage({
      type: 'result',
      id,
      success: true,
      value: 'ok',
      heapRatio: ratio,
    });
  };

  if (delay > 0) setTimeout(send, delay);
  else send();
});
