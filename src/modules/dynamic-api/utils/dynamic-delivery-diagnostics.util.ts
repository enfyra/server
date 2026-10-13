import type { Request, Response } from 'express';
import type {
  DynamicDeliveryFields,
  DynamicDeliveryStage,
} from '../../../shared/types/dynamic-delivery.types';
import { recordSystemError } from '../../../shared/runtime-log-buffer';
import { getDynamicHttpFailure } from './dynamic-http-failure.util';

type Milestone = DynamicDeliveryFields & {
  elapsedMs: number;
  lastElapsedMs?: number;
};
type DeliveryState = {
  startedAt: number;
  gateway: Partial<Record<DynamicDeliveryStage, Milestone>>;
  transport: Record<string, number | string | boolean>;
};

const requests = new WeakMap<object, DeliveryState>();
const responses = new WeakMap<object, DeliveryState>();
const stages = new Set<DynamicDeliveryStage>([
  'dispatch_start',
  'upstream_headers',
  'upstream_body_first_byte',
  'upstream_body_progress',
  'upstream_body_end',
  'buffered_preflight_start',
  'buffered_preflight_end',
  'buffered_read_start',
  'buffered_read_end',
  'frame_inspection_start',
  'frame_inspection_end',
  'transform_start',
  'transform_end',
  'transform_error',
  'public_relay_start',
  'public_relay_end',
  'upstream_first_frame',
  'upstream_terminal',
  'upstream_usage',
  'upstream_done',
  'upstream_eof',
  'upstream_read_error',
  'protocol_error',
  'public_terminal_ready',
  'public_done_ready',
  'settlement_start',
  'settlement_committed',
]);
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const safeId = (value: unknown): string | undefined =>
  typeof value === 'string' && uuid.test(value) ? value : undefined;

function stateForRequest(req: object): DeliveryState {
  let state = requests.get(req);
  if (!state) {
    state = { startedAt: Date.now(), gateway: {}, transport: {} };
    requests.set(req, state);
  }
  return state;
}

export function recordDynamicDeliveryStage(
  req: object,
  stage: DynamicDeliveryStage,
  fields?: DynamicDeliveryFields,
): void {
  if (!stages.has(stage)) return;
  const state = stateForRequest(req);
  const previous = state.gateway[stage];
  if (previous) {
    if (stage !== 'upstream_body_progress') return;
    if (Number.isSafeInteger(fields?.bytes) && fields!.bytes! >= 0)
      previous.bytes = Math.max(previous.bytes ?? 0, fields!.bytes!);
    if (Number.isSafeInteger(fields?.chunks) && fields!.chunks! >= 0)
      previous.chunks = Math.max(previous.chunks ?? 0, fields!.chunks!);
    previous.lastElapsedMs = Math.max(0, Date.now() - state.startedAt);
    return;
  }
  const milestone: Milestone = {
    elapsedMs: Math.max(0, Date.now() - state.startedAt),
  };
  if (
    Number.isInteger(fields?.statusCode) &&
    fields!.statusCode! >= 100 &&
    fields!.statusCode! <= 599
  )
    milestone.statusCode = fields!.statusCode;
  if (Number.isSafeInteger(fields?.bytes) && fields!.bytes! >= 0)
    milestone.bytes = fields!.bytes;
  if (Number.isSafeInteger(fields?.chunks) && fields!.chunks! >= 0)
    milestone.chunks = fields!.chunks;
  if (typeof fields?.clientStreaming === 'boolean')
    milestone.clientStreaming = fields.clientStreaming;
  if (typeof fields?.upstreamStreaming === 'boolean')
    milestone.upstreamStreaming = fields.upstreamStreaming;
  if (
    fields?.protocol === 'chat' ||
    fields?.protocol === 'responses' ||
    fields?.protocol === 'messages'
  )
    milestone.protocol = fields.protocol;
  if (
    fields?.contentKind === 'sse' ||
    fields?.contentKind === 'json' ||
    fields?.contentKind === 'other'
  )
    milestone.contentKind = fields.contentKind;
  if (
    typeof fields?.errorCode === 'string' &&
    /^(?:ERR|UND_ERR|E)[A-Z0-9_]{1,64}$/.test(fields.errorCode)
  )
    milestone.errorCode = fields.errorCode;
  if (stage === 'upstream_body_progress')
    milestone.lastElapsedMs = milestone.elapsedMs;
  state.gateway[stage] = milestone;
}

export function recordDynamicDeliveryTransport(
  response: object,
  event: 'stream_start' | 'stream_chunk' | 'stream_end' | 'stream_error',
  bytes?: number,
  error?: unknown,
): void {
  const state = responses.get(response);
  if (!state) return;
  state.transport[event] ??= Math.max(0, Date.now() - state.startedAt);
  if (event === 'stream_chunk') {
    state.transport.streamChunks = Math.min(
      Number.MAX_SAFE_INTEGER,
      Number(state.transport.streamChunks || 0) + 1,
    );
    if (Number.isSafeInteger(bytes) && bytes! >= 0)
      state.transport.streamBytes = Math.min(
        Number.MAX_SAFE_INTEGER,
        Number(state.transport.streamBytes || 0) + bytes!,
      );
  }
  const code = (error as { code?: unknown } | undefined)?.code;
  if (
    typeof code === 'string' &&
    /^(?:ERR|UND_ERR|E)[A-Z0-9_]{1,64}$/.test(code)
  )
    state.transport.streamErrorCode ??= code;
}

export function attachDynamicDeliveryDiagnostics(
  req: Request,
  response: Response,
): void {
  if (responses.has(response)) return;
  const state = stateForRequest(req);
  responses.set(response, state);
  const mark = (event: string) => {
    state.transport[event] ??= Math.max(0, Date.now() - state.startedAt);
  };
  const onAborted = () => mark('request_aborted');
  const onError = (error: unknown) => {
    mark('response_error');
    const code = (error as { code?: unknown })?.code;
    if (
      typeof code === 'string' &&
      /^(?:ERR|UND_ERR|E)[A-Z0-9_]{1,64}$/.test(code)
    )
      state.transport.responseErrorCode = code;
  };
  const onFinish = () => mark('response_finish');
  const onClose = () => {
    mark('response_close');
    req.off('aborted', onAborted);
    response.off('error', onError);
    response.off('finish', onFinish);
    queueMicrotask(() => {
      const failure = getDynamicHttpFailure(response);
      if (!failure && response.writableFinished) return;
      const request = req as Request & {
        routeData?: {
          context?: { $api?: { request?: { executionId?: string } } };
        };
      };
      const correlation = response.getHeader?.('x-correlation-id');
      recordSystemError('Dynamic HTTP delivery ended before completion', {
        component: 'DynamicHttpDelivery',
        code: 'HTTP_DELIVERY_INCOMPLETE',
        correlationId:
          typeof correlation === 'string' &&
          /^[\w.:-]{1,128}$/.test(correlation)
            ? correlation
            : undefined,
        executionId: safeId(
          request.routeData?.context?.$api?.request?.executionId,
        ),
        method: /^[A-Z]{1,12}$/.test(req.method) ? req.method : undefined,
        statusCode: failure?.statusCode ?? response.statusCode,
        wireStatusCode: response.statusCode,
        headersSent: response.headersSent,
        writableEnded: response.writableEnded,
        writableFinished: response.writableFinished,
        destroyed: response.destroyed,
        writableLength: response.writableLength,
        gateway: state.gateway,
        transport: state.transport,
        failure: failure
          ? {
              origin: failure.origin,
              statusCode: failure.statusCode,
              code: failure.code,
            }
          : undefined,
      });
    });
  };
  req.once('aborted', onAborted);
  response.once('error', onError);
  response.once('finish', onFinish);
  response.once('close', onClose);
}
