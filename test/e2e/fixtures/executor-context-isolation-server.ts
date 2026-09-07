import * as http from 'http';
import { asValue } from 'awilix';
import {
  computeEngineTuning,
  ExecutorEngineService,
  IsolatedExecutorService,
} from '@enfyra/kernel';
import { buildContainer } from '../../../src/container';
import { buildExpressApp } from '../../../src/express-app';
import { init, shutdown } from '../../../src/init';
import { env } from '../../../src/env';

const container = buildContainer();
const isolatedExecutorService = new IsolatedExecutorService({
  packageCacheService: {
    getPackages: () => container.cradle.runtimeRegistryService.getPackages(),
  },
  packageCdnLoaderService: container.cradle.packageCdnLoaderService,
  tuning: computeEngineTuning({
    logicalCpuCount: 1,
    totalMemoryBytes: 512 * 1024 * 1024,
    maxLanesPerRunner: 1,
    tasksPerIsolate: 1,
  }),
});
const kernelExecutorEngineService = new ExecutorEngineService({
  isolatedExecutorService,
});
container.register({
  isolatedExecutorService: asValue(isolatedExecutorService),
  kernelExecutorEngineService: asValue(kernelExecutorEngineService),
});

let server: http.Server | undefined;

async function stop(exitCode: number): Promise<never> {
  if (server) {
    await new Promise<void>((resolve) => server?.close(() => resolve()));
  }
  isolatedExecutorService.onDestroy();
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
