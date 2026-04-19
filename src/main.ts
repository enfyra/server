import 'reflect-metadata';
import * as http from 'http';
import { buildContainer } from './container';
import { init, shutdown } from './init';
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

  const initStart = Date.now();
  await init(container);
  logger.log(`Init completed: ${Date.now() - initStart}ms`);

  const app = buildExpressApp(container);
  const server = http.createServer(app);

  server.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EADDRINUSE') return;
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
    await gateway.afterInit(io);
  }

  const listenWithRetry = async (): Promise<void> => {
    const maxAttempts = process.env.DEV_WATCH ? 25 : 1;
    const delayMs = 200;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        await new Promise<void>((resolve, reject) => {
          const onError = (err: NodeJS.ErrnoException) => {
            server.removeListener('listening', onListening);
            reject(err);
          };
          const onListening = () => {
            server.removeListener('error', onError);
            resolve();
          };
          server.once('error', onError);
          server.once('listening', onListening);
          server.listen(env.PORT, '0.0.0.0');
        });
        return;
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

  try {
    await listenWithRetry();
    logger.log(`HTTP listening on port ${env.PORT}`);
  } catch (err: any) {
    if (err?.code === 'EADDRINUSE') {
      logger.error(`Port ${env.PORT} is already in use. Another server instance may be running.`);
    } else {
      logger.error('HTTP server error', err);
    }
    process.exit(1);
  }

  logger.log(`Cold Start completed! Total: ${Date.now() - startTime}ms`);

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
