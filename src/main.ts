import * as http from 'http';
import { Server } from 'socket.io';
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
    const io = new Server(server, {
      cors: { origin: true, credentials: true },
    });
    gateway.server = io;
    await gateway.afterInit(io);
    container.cradle.runtimeMonitorService?.start?.();
  }

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
    logger.log(`HTTP listening on port ${env.PORT}`);
    await container.cradle.flowExecutionQueueService?.init?.();
  } catch (err: any) {
    if (err?.code === 'EADDRINUSE') {
      logger.error(
        `Port ${env.PORT} is already in use. Another server instance may be running.`,
      );
    } else {
      logger.error('HTTP server error', err);
    }
    process.exit(1);
  }

  logger.log(`Cold Start completed! Total: ${Date.now() - startTime}ms`);

  if (!process.env.DEV_WATCH) {
    let shuttingDown = false;
    for (const sig of ['SIGINT', 'SIGTERM'] as const) {
      process.on(sig, async () => {
        if (shuttingDown) return;
        shuttingDown = true;
        logger.log(`Received ${sig}, shutting down gracefully...`);

        let shutdownHandled = false;
        const handleShutdown = (closedByForce = false) => {
          if (shutdownHandled) return;
          shutdownHandled = true;
          if (closedByForce) {
            logger.warn('Closing active HTTP connections for shutdown');
          }
          shutdown(container)
            .then(() => {
              logger.log('Shutdown complete');
              process.exit(0);
            })
            .catch((error) => {
              logger.error('Shutdown error:', error);
              process.exit(1);
            });
        };

        server.close(() => {
          logger.log('HTTP server closed');
          handleShutdown(false);
        });
        server.closeIdleConnections?.();
      });
    }
  }
}

main().catch((err) => {
  console.error('Fatal:', err);
  process.exit(1);
});
