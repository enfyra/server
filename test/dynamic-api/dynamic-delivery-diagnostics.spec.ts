import { EventEmitter } from 'node:events';
import { describe, expect, it } from 'vitest';
import {
  attachDynamicDeliveryDiagnostics,
  recordDynamicDeliveryStage,
  recordDynamicDeliveryTransport,
} from '../../src/modules/dynamic-api/utils/dynamic-delivery-diagnostics.util';
import {
  recordDynamicClientDisconnect,
  recordDynamicStreamFailure,
} from '../../src/modules/dynamic-api/utils/dynamic-http-failure.util';
import {
  peekRuntimeLogs,
  acknowledgeRuntimeLog,
} from '../../src/shared/runtime-log-buffer';

function fixture() {
  const req = Object.assign(new EventEmitter(), {
    method: 'POST',
    url: '/v1/chat/completions?secret=private-query',
    headers: {
      'x-enfyra-delivery-id': 'f72d6065-19ec-4fa7-95ee-d40bd083ca1c',
      authorization: 'Bearer private-key',
    },
    routeData: {
      context: {
        $api: {
          request: { executionId: 'ea93f2a0-95c4-4cbb-a1a9-e0cf8787a4ec' },
        },
      },
    },
  });
  const res = Object.assign(new EventEmitter(), {
    statusCode: 200,
    headersSent: true,
    writableEnded: false,
    writableFinished: false,
    writableLength: 0,
    destroyed: false,
  });
  attachDynamicDeliveryDiagnostics(req as never, res as never);
  return { req, res };
}

function diagnosticRows() {
  return peekRuntimeLogs(2000).filter(
    (row) => row.record.component === 'DynamicHttpDelivery',
  );
}
function clearDiagnostics() {
  for (const row of diagnosticRows()) acknowledgeRuntimeLog(row.record.eventId);
}

describe('private dynamic delivery diagnostics', () => {
  it('retains exact termination event, first cause, delivery milestones and byte counters', async () => {
    clearDiagnostics();
    const { req, res } = fixture();
    recordDynamicDeliveryStage(req, 'upstream_headers', {
      statusCode: 200,
      message: 'private-body',
    });
    recordDynamicDeliveryStage(req, 'upstream_terminal');
    recordDynamicDeliveryTransport(res, 'stream_chunk', 1200);
    recordDynamicDeliveryTransport(res, 'stream_error', undefined, {
      code: 'ERR_EXECUTION_ABORTED',
      message: 'private-error',
    });
    recordDynamicClientDisconnect(res);
    res.emit('close');
    await Promise.resolve();
    const rows = diagnosticRows();
    expect(rows).toHaveLength(1);
    expect(rows[0].record.statusCode).toBe(499);
    const details = rows[0].record.details as any;
    expect(details.deliveryId).toBeUndefined();
    expect(details.executionId).toBe('ea93f2a0-95c4-4cbb-a1a9-e0cf8787a4ec');
    expect(details.transport.response_close).toEqual(expect.any(Number));
    expect(details.transport.streamBytes).toBe(1200);
    expect(details.gateway.upstream_headers.statusCode).toBe(200);
    expect(details.gateway.upstream_terminal).toBeDefined();
    expect(details.gateway.upstream_done).toBeUndefined();
    expect(details.failure).toEqual({
      origin: 'client',
      statusCode: 499,
      code: 'client_closed_request',
    });
    const serialized = JSON.stringify(rows);
    for (const secret of [
      'private-query',
      'private-key',
      'private-body',
      'private-error',
    ])
      expect(serialized).not.toContain(secret);
    clearDiagnostics();
  });

  it('does not misclassify server destruction or persist error payloads', async () => {
    clearDiagnostics();
    const { req, res } = fixture();
    recordDynamicDeliveryStage(req, 'settlement_start');
    recordDynamicStreamFailure(res, new Error('private-stream-payload'));
    res.destroyed = true;
    res.emit('close');
    await Promise.resolve();
    const rows = diagnosticRows();
    expect(rows).toHaveLength(1);
    expect(rows[0].record.statusCode).toBe(502);
    expect((rows[0].record.details as any).failure.code).toBe(
      'response_stream_error',
    );
    expect(JSON.stringify(rows)).not.toContain('private-stream-payload');
    clearDiagnostics();
  });

  it('bounds and validates script-controlled metadata and keeps normal EOF silent', async () => {
    clearDiagnostics();
    const { req, res } = fixture();
    for (let i = 0; i < 1000; i++)
      recordDynamicDeliveryStage(req, 'upstream_headers', {
        statusCode: 200,
        bytes: -10,
        text: 'private',
        injected: true,
      });
    recordDynamicDeliveryStage(req, 'unknown_private' as never, { bytes: 100 });
    res.writableEnded = true;
    res.writableFinished = true;
    res.emit('finish');
    res.emit('close');
    await Promise.resolve();
    expect(diagnosticRows()).toHaveLength(0);
    expect(req.listenerCount('aborted')).toBe(0);
    expect(res.listenerCount('error')).toBe(0);
  });

  it('captures request abort separately from response close without recording caller join IDs', async () => {
    clearDiagnostics();
    const { req, res } = fixture();
    req.headers['x-enfyra-delivery-id'] = 'private-control\nvalue';
    req.emit('aborted');
    recordDynamicClientDisconnect(res);
    res.emit('close');
    await Promise.resolve();
    const detail = diagnosticRows()[0].record.details as any;
    expect(detail.transport.request_aborted).toEqual(expect.any(Number));
    expect(detail.transport.response_close).toEqual(expect.any(Number));
    expect(JSON.stringify(detail)).not.toContain('private-control');
    clearDiagnostics();
  });
});
