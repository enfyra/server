import * as http from 'http';
import { access, writeFile } from 'fs/promises';
import { buildContainer } from '../../../src/container';
import { buildExpressApp } from '../../../src/express-app';
import { init, shutdown } from '../../../src/init';
import { env } from '../../../src/env';

const markerFile = process.env.BOOTSTRAP_LEASE_TEST_MARKER;
const releaseFile = process.env.BOOTSTRAP_LEASE_TEST_RELEASE;

if (!markerFile || !releaseFile) {
  throw new Error(
    'Bootstrap lease-loss fixture requires marker and release files',
  );
}

const container = buildContainer();
const initializer = container.cradle.firstRunInitializer as unknown as {
  markInitialized(): Promise<void>;
};
const markInitialized = initializer.markInitialized.bind(initializer);

initializer.markInitialized = async () => {
  await writeFile(markerFile, 'publication-blocked');
  process.stdout.write('[lease-loss] publication-blocked\n');
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    try {
      await access(releaseFile);
      return markInitialized();
    } catch {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
  throw new Error('Timed out waiting for lease-loss release latch');
};

let server: http.Server | undefined;

async function stop(exitCode: number): Promise<never> {
  if (server) {
    await new Promise<void>((resolve) => server?.close(() => resolve()));
  }
  await shutdown(container).catch(() => undefined);
  process.exit(exitCode);
}

async function main(): Promise<void> {
  await init(container);
  server = http.createServer(buildExpressApp(container));
  await new Promise<void>((resolve, reject) => {
    server?.once('error', reject);
    server?.listen(env.PORT, '127.0.0.1', resolve);
  });
  process.stdout.write(`HTTP listening on port ${env.PORT}\n`);
}

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.on(signal, () => void stop(0));
}

main().catch(async (error) => {
  process.stderr.write(
    `${error instanceof Error ? error.stack || error.message : String(error)}\n`,
  );
  await stop(1);
});
