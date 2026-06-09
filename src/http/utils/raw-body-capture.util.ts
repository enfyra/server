import type { IncomingMessage, ServerResponse } from 'node:http';

export type RequestWithRawBody = IncomingMessage & {
  rawBody?: string;
};

export function captureRawBody(
  req: IncomingMessage,
  _res: ServerResponse,
  buf: Buffer,
  encoding: string,
) {
  if (!buf?.length) return;
  const bodyEncoding =
    encoding && Buffer.isEncoding(encoding) ? encoding : 'utf8';
  (req as RequestWithRawBody).rawBody = buf.toString(bodyEncoding);
}
