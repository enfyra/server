export interface ForwardedClientContext {
  version: 1;
  peerIp: string;
  headers: Record<string, string | string[]>;
}

export interface ClientIpRequest {
  headers?: Record<string, unknown>;
  ip?: string | undefined;
  connection?: { remoteAddress?: string | undefined };
  socket?: { remoteAddress?: string | undefined };
}
