import express from 'express';
import { afterEach, describe, expect, it } from 'vitest';
import type { AddressInfo } from 'node:net';
import type { Server } from 'node:http';
import { captureRawBody } from '../../src/http/utils/raw-body-capture.util';

async function listen(app: express.Express) {
  const server = await new Promise<Server>((resolve) => {
    const instance = app.listen(0, () => resolve(instance));
  });
  const address = server.address() as AddressInfo;
  return { server, url: `http://127.0.0.1:${address.port}` };
}

describe('raw body capture', () => {
  const servers: Server[] = [];

  afterEach(async () => {
    await Promise.all(
      servers.splice(0).map(
        (server) =>
          new Promise<void>((resolve, reject) => {
            server.close((error) => (error ? reject(error) : resolve()));
          }),
      ),
    );
  });

  it('keeps the exact JSON payload string while still parsing req.body', async () => {
    const app = express();
    app.use(express.json({ verify: captureRawBody }));
    app.post('/webhook', (req, res) => {
      res.json({ parsed: req.body, rawBody: (req as any).rawBody });
    });

    const { server, url } = await listen(app);
    servers.push(server);
    const payload =
      '{\n  "event_type": "transaction.completed",\n  "data": {"id":"txn_123"}\n}';
    const response = await fetch(`${url}/webhook`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: payload,
    });

    const body = await response.json();
    expect(body.parsed).toEqual({
      event_type: 'transaction.completed',
      data: { id: 'txn_123' },
    });
    expect(body.rawBody).toBe(payload);
    expect(JSON.stringify(body.parsed)).not.toBe(payload);
  });
});
