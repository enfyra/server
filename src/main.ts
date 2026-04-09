import { Logger, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { GraphqlService } from './modules/graphql/services/graphql.service';
import { AppModule } from './app.module';
import { initializeDatabase } from '../scripts/init-db';
import { CACHE_EVENTS } from './shared/utils/cache-events.constants';
import { AppLogger } from './shared/utils/app-logger';

async function bootstrap() {
  const startTime = Date.now();
  const logger = new Logger('Main');
  logger.log('Starting Cold Start');
  try {
    const initStart = Date.now();
    await initializeDatabase();
    logger.log(`DB Init: ${Date.now() - initStart}ms`);
  } catch (err) {
    logger.error('Error during initialization:', err);
    process.exit(1);
  }
  const nestStart = Date.now();
  const app = await NestFactory.create(AppModule, {
    bodyParser: true,
    rawBody: true,
    bufferLogs: true,
  });
  app.useLogger(new AppLogger());
  const expressApp = app.getHttpAdapter().getInstance();
  expressApp.use(require('express').json({ limit: '50mb' }));
  expressApp.use(
    require('express').urlencoded({ limit: '50mb', extended: true }),
  );
  const qs = require('qs');
  expressApp.set('query parser', (str: string) => {
    return qs.parse(str, {
      allowPrototypes: false,
      depth: 10,
      parameterLimit: 1000,
      strictNullHandling: false,
      arrayLimit: 200,
    });
  });
  logger.log(`NestJS Create: ${Date.now() - nestStart}ms`);
  try {
    const graphqlService = app.get(GraphqlService);
    const expressApp = app.getHttpAdapter().getInstance();
    expressApp.use('/graphql', (req, res, next) => {
      return graphqlService.getYogaInstance()(req, res, next);
    });
    logger.log('GraphQL endpoint mounted at /graphql');
  } catch (error) {
    logger.warn('GraphQL endpoint not available:', error.message);
  }
  const configService = app.get(ConfigService);
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
      forbidNonWhitelisted: true,
      transformOptions: {
        enableImplicitConversion: true,
      },
    }),
  );
  const eventEmitter = app.get(EventEmitter2);

  const BOOT_EVENTS = [
    CACHE_EVENTS.METADATA_LOADED,
    CACHE_EVENTS.ROUTE_LOADED,
    CACHE_EVENTS.PACKAGE_LOADED,
    CACHE_EVENTS.STORAGE_LOADED,
    CACHE_EVENTS.OAUTH_CONFIG_LOADED,
    CACHE_EVENTS.WEBSOCKET_LOADED,
    CACHE_EVENTS.FLOW_LOADED,
    CACHE_EVENTS.GRAPHQL_LOADED,
  ];

  const received = new Set<string>();
  const systemReadyPromise = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      const missing = BOOT_EVENTS.filter((e) => !received.has(e));
      logger.warn(
        `Boot timeout after 60s. Missing events: ${missing.join(', ')}`,
      );
      resolve();
    }, 60000);

    const check = () => {
      if (received.size === BOOT_EVENTS.length) {
        clearTimeout(timeout);
        eventEmitter.emit(CACHE_EVENTS.SYSTEM_READY);
        resolve();
      }
    };

    for (const event of BOOT_EVENTS) {
      eventEmitter.on(event, () => {
        received.add(event);
        check();
      });
    }
  });

  const appInitStart = Date.now();
  await app.init();
  logger.log(`App Init (Bootstrap): ${Date.now() - appInitStart}ms`);

  const readyStart = Date.now();
  await systemReadyPromise;
  logger.log(`System caches ready: ${Date.now() - readyStart}ms`);

  const listenStart = Date.now();
  await app.listen(configService.get('PORT') || 1105);
  logger.log(`HTTP Listen: ${Date.now() - listenStart}ms`);

  const totalTime = Date.now() - startTime;
  logger.log(`Cold Start completed! Total time: ${totalTime}ms`);
}
bootstrap();
