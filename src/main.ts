// @nestjs packages
import { Logger, ValidationPipe, LoggerService } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';

// Internal imports
import { GraphqlService } from './modules/graphql/services/graphql.service';

// Relative imports
import { AppModule } from './app.module';
import { initializeDatabase } from '../scripts/init-db';

// Custom logger that filters out noisy NestJS internal logs
class FilteredLogger implements LoggerService {
  private filteredContexts = ['InstanceLoader', 'RoutesResolver', 'RouterExplorer'];

  // ANSI color codes matching NestJS
  private colors = {
    green: '\x1b[32m',
    yellow: '\x1b[38;5;3m', // Bright yellow/orange for context
    red: '\x1b[31m',
    magenta: '\x1b[35m',
    cyan: '\x1b[36m',
    reset: '\x1b[39m',
  };

  private formatTimestamp(): string {
    const now = new Date();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const year = now.getFullYear();
    let hours = now.getHours();
    const minutes = String(now.getMinutes()).padStart(2, '0');
    const seconds = String(now.getSeconds()).padStart(2, '0');
    const ampm = hours >= 12 ? 'PM' : 'AM';
    hours = hours % 12 || 12;
    const hoursStr = String(hours).padStart(2, '0');
    return `${month}/${day}/${year}, ${hoursStr}:${minutes}:${seconds} ${ampm}`;
  }

  private printMessage(level: string, message: any, context?: string, color?: string) {
    const timestamp = this.formatTimestamp();
    const pid = process.pid;
    const levelColor = color || this.colors.green;
    const ctx = context ? `${this.colors.yellow}[${context}] ${this.colors.reset}` : '';

    // Print header without message
    const header = `${levelColor}[Nest] ${pid}  - ${this.colors.reset}${timestamp}     ${levelColor}${level}${this.colors.reset} ${ctx}`;

    // If message is object, print header then use console.dir for the object
    if (typeof message === 'object' && message !== null) {
      console.log(header);
      console.dir(message, { depth: null, colors: true });
    } else {
      // String message - print on same line
      console.log(`${header}${levelColor}${message}${this.colors.reset}`);
    }
  }

  log(message: any, context?: string) {
    if (context && this.filteredContexts.includes(context)) return;
    this.printMessage('LOG', message, context, this.colors.green);
  }

  error(message: any, trace?: string, context?: string) {
    this.printMessage('ERROR', message, context, this.colors.red);
    if (trace) console.error(trace);
  }

  warn(message: any, context?: string) {
    this.printMessage('WARN', message, context, this.colors.yellow);
  }

  debug(message: any, context?: string) {
    if (context && this.filteredContexts.includes(context)) return;
    this.printMessage('DEBUG', message, context, this.colors.magenta);
  }

  verbose(message: any, context?: string) {
    if (context && this.filteredContexts.includes(context)) return;
    this.printMessage('VERBOSE', message, context, this.colors.cyan);
  }
}

async function bootstrap() {
  const startTime = Date.now();
  const logger = new Logger('Main');
  logger.log('🚀 Starting Cold Start');

  try {
    const initStart = Date.now();
    await initializeDatabase();
    logger.log(`⏱️  DB Init: ${Date.now() - initStart}ms`);
  } catch (err) {
    logger.error('Error during initialization:', err);
    process.exit(1);
  }

  const nestStart = Date.now();
  const app = await NestFactory.create(AppModule, {
    logger: new FilteredLogger(),
    bufferLogs: true,
  });
  logger.log(`⏱️  NestJS Create: ${Date.now() - nestStart}ms`);

  // Setup GraphQL endpoint
  try {
    const graphqlService = app.get(GraphqlService);
    const expressApp = app.getHttpAdapter().getInstance();
    
    expressApp.use('/graphql', (req, res, next) => {
      return graphqlService.getYogaInstance()(req, res, next);
    });
    logger.log('✅ GraphQL endpoint mounted at /graphql');
  } catch (error) {
    logger.warn('⚠️ GraphQL endpoint not available:', error.message);
  }

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
