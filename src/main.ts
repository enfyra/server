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
  expressApp.use(require('express').urlencoded({ limit: '50mb', extended: true }));
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
  const appInitStart = Date.now();
  await app.init();
  logger.log(`App Init (Bootstrap): ${Date.now() - appInitStart}ms`);
  const listenStart = Date.now();
  await app.listen(configService.get('PORT') || 1105);
  logger.log(`HTTP Listen: ${Date.now() - listenStart}ms`);

  // Wait for system to be fully ready (all caches loaded, GraphQL & Swagger generated)
  const eventEmitter = app.get(EventEmitter2);
  await new Promise<void>((resolve) => {
    eventEmitter.once(CACHE_EVENTS.SYSTEM_READY, () => {
      resolve();
    });
  });

  const totalTime = Date.now() - startTime;
  logger.log(`Cold Start completed! Total time: ${totalTime}ms`);
}
bootstrap();