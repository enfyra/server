import 'reflect-metadata';
import { buildContainer } from './container';
import { bootstrap, shutdown } from './bootstrap';
import { buildExpressApp } from './express-app';
import { env } from './env';
import { Logger } from './shared/logger';

async function main() {
  process.stdout.write('\x1Bc');
  const startTime = Date.now();
  const logger = new Logger('Server');

  logger.log('Starting Cold Start...');

  const containerStart = Date.now();
  const container = buildContainer();
  logger.log(`Container built: ${Date.now() - containerStart}ms`);

  const bootstrapStart = Date.now();
  await bootstrap(container);
  logger.log(`Bootstrap completed: ${Date.now() - bootstrapStart}ms`);

  const appStart = Date.now();
  const app = buildExpressApp(container);

  const listenWithRetry = async (): Promise<import('http').Server> => {
    const maxAttempts = process.env.DEV_WATCH ? 25 : 1;
    const delayMs = 200;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return await new Promise<import('http').Server>((resolve, reject) => {
          const s = app.listen(env.PORT, '0.0.0.0', () => resolve(s));
          s.once('error', reject);
        });
      } catch (err: any) {
        if (err?.code === 'EADDRINUSE' && attempt < maxAttempts) {
          if (attempt === 1) {
            logger.warn(`Port ${env.PORT} busy, waiting for previous instance to release...`);
          }
          await new Promise((r) => setTimeout(r, delayMs));
          continue;
        }
        throw err;
      }
    }
    throw new Error(`Failed to bind port ${env.PORT} after ${maxAttempts} attempts`);
  };

  let server: import('http').Server;
  try {
    server = await listenWithRetry();
    logger.log(`HTTP Listen: ${Date.now() - appStart}ms`);
    logger.log(`Cold Start completed! Total: ${Date.now() - startTime}ms`);
  } catch (err: any) {
    if (err?.code === 'EADDRINUSE') {
      logger.error(`Port ${env.PORT} is already in use. Another server instance may be running.`);
    } else {
      logger.error('HTTP server error', err);
    }
    process.exit(1);
  }

  server.on('error', (err: NodeJS.ErrnoException) => {
    logger.error('HTTP server runtime error', err);
    process.exit(1);
  });

  const gateway = container.cradle.dynamicWebSocketGateway;
  if (gateway) {
    const { Server } = require('socket.io');
    const io = new Server(server, {
      cors: { origin: true, credentials: true },
    });
    gateway.server = io;
    gateway.afterInit(io);
  }

  if (!process.env.DEV_WATCH) {
    for (const sig of ['SIGINT', 'SIGTERM'] as const) {
      process.on(sig, async () => {
        logger.log(`Received ${sig}, shutting down gracefully...`);

        let shutdownHandled = false;
        const handleShutdown = (force = false) => {
          if (shutdownHandled) return;
          shutdownHandled = true;
          if (force) {
            logger.warn('Forcing shutdown after timeout');
          }
          shutdown(container).then(() => {
            logger.log('Shutdown complete');
            process.exit(0);
          }).catch((error) => {
            logger.error('Shutdown error:', error);
            process.exit(1);
          });
        };

        server.close(() => {
          logger.log('HTTP server closed');
          handleShutdown(false);
        });

        setTimeout(() => handleShutdown(true), 5000);
      });
    }
  }
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
