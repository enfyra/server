// External packages
import * as cors from 'cors';
import * as express from 'express';
import * as qs from 'qs';

// @nestjs packages
import { Logger, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';

// Internal imports
import { GraphqlService } from './modules/graphql/services/graphql.service';

// Relative imports
import { AppModule } from './app.module';
import { initializeDatabase } from '../scripts/init-db';

async function bootstrap() {
  const startTime = Date.now();
  const logger = new Logger('Main');
  logger.log('🚀 Starting Cold Start');

  // Sequential initialization - DB init must complete before build
  try {
    // DB initialization first - direct import instead of node command
    const initStart = Date.now();
    await initializeDatabase();
    logger.log(`⏱️  DB Init: ${Date.now() - initStart}ms`);

    // Note: buildToJs is no longer needed since init-db now handles compilation
  } catch (err) {
    logger.error('Error during initialization:', err);
  }

  const nestStart = Date.now();
  const app = await NestFactory.create(AppModule, {
    logger: ['error', 'warn', 'log'], // Reduce logging overhead
    bufferLogs: true, // Buffer logs during initialization
  });
  logger.log(`⏱️  NestJS Create: ${Date.now() - nestStart}ms`);
  const graphqlService = app.get(GraphqlService);
  const expressApp = app.getHttpAdapter().getInstance();
  const configService = app.get(ConfigService);

  app.use(
    cors({
      origin: '*',
      credentials: true,
      methods: ['POST', 'GET', 'OPTIONS', 'PATCH', 'DELETE', 'PUT'],
      allowedHeaders: [
        'Content-Type',
        'Authorization',
        'x-apollo-operation-name',
        'x-requested-with',
      ],
    }),
  );
  app.use(express.json());

  expressApp.set('query parser', (str) => qs.parse(str, { depth: 10 }));

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
    }),
  );

  // Initialize app (triggers onApplicationBootstrap)
  const initStart = Date.now();
  await app.init();
  logger.log(`⏱️  App Init (Bootstrap): ${Date.now() - initStart}ms`);

  // Setup GraphQL endpoint
  expressApp.use('/graphql', (req, res, next) => {
    return graphqlService.getYogaInstance()(req, res, next);
  });

  // Start listening
  const listenStart = Date.now();
  await app.listen(configService.get('PORT') || 1105);
  logger.log(`⏱️  HTTP Listen: ${Date.now() - listenStart}ms`);

  const totalTime = Date.now() - startTime;
  logger.log(`🎉 Cold Start completed! Total time: ${totalTime}ms`);
}
bootstrap();
