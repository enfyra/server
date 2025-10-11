// @nestjs packages
import { Logger, ValidationPipe } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';

// Relative imports
import { AppModule } from './app.module';
import { initializeDatabaseKnex } from '../scripts/init-db-knex';

async function bootstrap() {
  const startTime = Date.now();
  const logger = new Logger('Main');
  logger.log('🚀 Starting Cold Start (Knex Mode)');

  try {
    const initStart = Date.now();
    await initializeDatabaseKnex();
    logger.log(`⏱️  DB Init (Knex): ${Date.now() - initStart}ms`);
  } catch (err) {
    logger.error('Error during initialization:', err);
    process.exit(1);
  }

  const nestStart = Date.now();
  const app = await NestFactory.create(AppModule, {
    logger: ['error', 'warn', 'log'],
    bufferLogs: true,
  });
  logger.log(`⏱️  NestJS Create: ${Date.now() - nestStart}ms`);

  const configService = app.get(ConfigService);

  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
      whitelist: true,
    }),
  );

  const appInitStart = Date.now();
  await app.init();
  logger.log(`⏱️  App Init (Bootstrap): ${Date.now() - appInitStart}ms`);

  const listenStart = Date.now();
  await app.listen(configService.get('PORT') || 1105);
  logger.log(`⏱️  HTTP Listen: ${Date.now() - listenStart}ms`);

  const totalTime = Date.now() - startTime;
  logger.log(`🎉 Cold Start completed! Total time: ${totalTime}ms`);
}
bootstrap();
