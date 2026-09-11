import express from 'express';
import { createServer, type Server } from 'node:http';
import { afterEach, describe, expect, it } from 'vitest';
import { registerReadinessRoute } from '../../src/http/routes/readiness.routes';

describe('runtime readiness', () => {
  let server: Server;
  afterEach(async () => {
    if (server) await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it('reports readiness without auth, database queries or a cached success', async () => {
    let ready = false;
    const app = express();
    registerReadinessRoute(app, () => ready);
    app.use((_req, res) => res.sendStatus(401));
    server = createServer(app);
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (!address || typeof address === 'string') throw new Error('Missing test address');
    const url = `http://127.0.0.1:${address.port}/health/ready`;
    const starting = await fetch(url);
    expect(starting.status).toBe(503);
    expect(starting.headers.get('retry-after')).toBe('1');
    expect(starting.headers.get('cache-control')).toBe('no-store');
    expect(await starting.json()).toEqual({ ready: false, code: 'RUNTIME_NOT_READY' });
    ready = true;
    expect((await fetch(url)).status).toBe(200);
    ready = false;
    expect((await fetch(url)).status).toBe(503);
    expect((await fetch(url, { method: 'POST' })).status).toBe(401);
  });
});
