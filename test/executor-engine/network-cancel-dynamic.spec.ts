import { createServer, type Server } from 'node:http';
import { createServer as createNetServer, type Server as NetServer } from 'node:net';
import { AddressInfo } from 'node:net';
import { mkdtemp, rm, writeFile } from 'fs/promises';
import { tmpdir } from 'os';
import path from 'path';
import { afterEach, describe, expect, it } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';

let tempDir: string | null = null;
let silentServer: Server | null = null;
let echoServer: Server | null = null;
let silentTcpServer: NetServer | null = null;

afterEach(async () => {
  if (tempDir) {
    await rm(tempDir, { recursive: true, force: true });
    tempDir = null;
  }
  silentServer?.close();
  silentServer = null;
  echoServer?.close();
  echoServer = null;
  silentTcpServer?.close();
  silentTcpServer = null;
});

function makeService(modulePath: string, packageName: string) {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => [packageName],
    } as any,
    packageCdnLoaderService: {
      getPackageSources: () => [
        {
          name: packageName,
          safeName: packageName.replace(/[^a-z0-9_]/gi, '_'),
          version: '1.0.0',
          sourceCode: '',
          filePath: modulePath,
          fileUrl: modulePath,
        },
      ],
    } as any,
  });
}

function makeContext(): any {
  return {
    $body: {},
    $query: {},
    $params: {},
    $share: { $logs: [] },
    $helpers: {},
    $cache: {},
    $repos: {},
    $user: null,
  };
}

/** Local server that accepts connections and never responds. */
async function startSilentServer(): Promise<{
  port: number;
  waitForConnectionClose: () => Promise<void>;
}> {
  let resolveClose: (() => void) | null = null;
  const closePromise = new Promise<void>((resolve) => {
    resolveClose = resolve;
  });
  silentServer = createServer((socket) => {
    socket.on('close', () => resolveClose?.());
    // Never write a response; keep the connection open.
  });
  await new Promise<void>((resolve) => silentServer!.listen(0, '127.0.0.1', resolve));
  const port = (silentServer.address() as AddressInfo).port;
  return {
    port,
    waitForConnectionClose: () => closePromise,
  };
}

/** Local server that responds immediately with a small body. */
async function startEchoServer(): Promise<number> {
  echoServer = createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ ok: true }));
  });
  await new Promise<void>((resolve) => echoServer!.listen(0, '127.0.0.1', resolve));
  return (echoServer.address() as AddressInfo).port;
}

/** Raw TCP server that accepts connections and never responds. */
async function startSilentTcpServer(): Promise<{
  port: number;
  waitForConnectionClose: () => Promise<void>;
}> {
  let resolveClose: (() => void) | null = null;
  const closePromise = new Promise<void>((resolve) => {
    resolveClose = resolve;
  });
  silentTcpServer = createNetServer((socket) => {
    socket.on('close', () => resolveClose?.());
  });
  await new Promise<void>((resolve) => silentTcpServer!.listen(0, '127.0.0.1', resolve));
  const port = (silentTcpServer.address() as AddressInfo).port;
  return {
    port,
    waitForConnectionClose: () => closePromise,
  };
}

describe('dynamic package network cancellation (not undici-locked)', () => {
  it('cancels a raw net.connect package socket on disconnect', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-net-cancel-'));
    const modulePath = path.join(tempDir, 'fake-tcp-client.mjs');
    await writeFile(
      modulePath,
      `
        import { createRequire } from 'node:module';
        const require = createRequire(import.meta.url);
        const net = require('node:net');

        export default {
          connect(port) {
            return new Promise((resolve, reject) => {
              const socket = net.connect(port, '127.0.0.1');
              socket.on('connect', () => {
                // Hold the connection open and wait for data that never comes.
                socket.on('data', (chunk) => resolve(String(chunk)));
              });
              socket.on('error', (error) => reject(error));
            });
          },
        };
      `,
      'utf8',
    );

    const { port, waitForConnectionClose } = await startSilentTcpServer();
    const service = makeService(modulePath, 'fake-tcp-client');
    const controller = new AbortController();

    try {
      const result = service.runBatch(
        [
          {
            type: 'handler',
            code: `await $ctx.$pkgs['fake-tcp-client'].connect(${port});`,
          },
        ],
        makeContext(),
        10000,
        { signal: controller.signal },
      );
      setTimeout(() => controller.abort(), 150);

      await expect(result).rejects.toMatchObject({
        code: 'ERR_EXECUTION_ABORTED',
      });
      await waitForConnectionClose();
    } finally {
      service.onDestroy();
    }
  });

  it('cancels a node:http-based package request on disconnect (axios/got-style libraries)', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-http-cancel-'));
    const modulePath = path.join(tempDir, 'fake-http-client.mjs');
    await writeFile(
      modulePath,
      `
        import { createRequire } from 'node:module';
        const require = createRequire(import.meta.url);
        const http = require('node:http');

        export default {
          get(url) {
            return new Promise((resolve, reject) => {
              const req = http.request(url, (res) => {
                let body = '';
                res.on('data', (chunk) => (body += chunk));
                res.on('end', () => resolve({ status: res.statusCode, body }));
              });
              req.on('error', (error) => reject(error));
              req.end();
            });
          },
        };
      `,
      'utf8',
    );

    const { port, waitForConnectionClose } = await startSilentServer();
    const service = makeService(modulePath, 'fake-http-client');
    const controller = new AbortController();

    try {
      const result = service.runBatch(
        [
          {
            type: 'handler',
            code: `await $ctx.$pkgs['fake-http-client'].get('http://127.0.0.1:${port}/');`,
          },
        ],
        makeContext(),
        10000,
        { signal: controller.signal },
      );
      // Give the request time to reach the silent server, then disconnect.
      setTimeout(() => controller.abort(), 150);

      await expect(result).rejects.toMatchObject({
        code: 'ERR_EXECUTION_ABORTED',
      });
      await waitForConnectionClose();
    } finally {
      service.onDestroy();
    }
  });

  it('cancels a global fetch package request on disconnect', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-fetch-cancel-'));
    const modulePath = path.join(tempDir, 'fake-fetch-client.mjs');
    await writeFile(
      modulePath,
      `
        export default {
          async get(url) {
            const res = await fetch(url);
            return { status: res.status };
          },
        };
      `,
      'utf8',
    );

    const { port, waitForConnectionClose } = await startSilentServer();
    const service = makeService(modulePath, 'fake-fetch-client');
    const controller = new AbortController();

    try {
      const result = service.runBatch(
        [
          {
            type: 'handler',
            code: `await $ctx.$pkgs['fake-fetch-client'].get('http://127.0.0.1:${port}/');`,
          },
        ],
        makeContext(),
        10000,
        { signal: controller.signal },
      );
      setTimeout(() => controller.abort(), 150);

      await expect(result).rejects.toMatchObject({
        code: 'ERR_EXECUTION_ABORTED',
      });
      await waitForConnectionClose();
    } finally {
      service.onDestroy();
    }
  });

  it('keeps the happy path intact through the patched network layer', async () => {
    tempDir = await mkdtemp(path.join(tmpdir(), 'enfyra-http-happy-'));
    const modulePath = path.join(tempDir, 'fake-http-client.mjs');
    await writeFile(
      modulePath,
      `
        import { createRequire } from 'node:module';
        const require = createRequire(import.meta.url);
        const http = require('node:http');

        export default {
          get(url) {
            return new Promise((resolve, reject) => {
              const req = http.request(url, (res) => {
                let body = '';
                res.on('data', (chunk) => (body += chunk));
                res.on('end', () => resolve({ status: res.statusCode, body }));
              });
              req.on('error', (error) => reject(error));
              req.end();
            });
          },
        };
      `,
      'utf8',
    );

    const port = await startEchoServer();
    const service = makeService(modulePath, 'fake-http-client');

    try {
      const result: any = await service.runBatch(
        [
          {
            type: 'handler',
            code: `return await $ctx.$pkgs['fake-http-client'].get('http://127.0.0.1:${port}/');`,
          },
        ],
        makeContext(),
        5000,
      );
      expect(result.value).toEqual({
        status: 200,
        body: JSON.stringify({ ok: true }),
      });
    } finally {
      service.onDestroy();
    }
  });
});
