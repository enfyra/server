import * as path from 'path';
import * as fs from 'fs';
import * as winston from 'winston';
import * as DailyRotateFile from 'winston-daily-rotate-file';
import { EventEmitter } from 'events';

// Increase max listeners to prevent memory leak warnings from multiple transports
EventEmitter.defaultMaxListeners = 20;

const EXCLUDED_CONTEXTS = [
  'InstanceLoader',
  'RouterExplorer',
  'RoutesResolver',
  'NestFactory',
  'NestApplication',
];

const LOG_DIR = path.join(process.cwd(), 'logs');

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

let logCounter = 0;
const generateLogId = (): string => {
  const timestamp = Date.now().toString(36);
  const counter = (logCounter++).toString(36).padStart(4, '0');
  const random = Math.random().toString(36).substring(2, 6);
  return `log_${timestamp}_${counter}_${random}`;
};

const addLogId = winston.format((info) => {
  info.id = generateLogId();
  return info;
});

const logFormat = winston.format.combine(
  addLogId(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
  winston.format.errors({ stack: true }),
  winston.format.json(),
);

const consoleFormat = winston.format.combine(
  addLogId(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.colorize({ all: true }),
  winston.format.printf(({ id, timestamp, level, message, context, correlationId, stack, ...meta }) => {
    const idStr = id ? `[${id}] ` : '';
    const contextStr = context ? `[${context}] ` : '';
    const corrStr = correlationId ? `[${correlationId}] ` : '';
    const metaStr = Object.keys(meta).length > 0 ? ` ${JSON.stringify(meta)}` : '';
    const stackStr = stack ? `\n${stack}` : '';
    return `${timestamp} ${level} ${idStr}${contextStr}${corrStr}${message}${metaStr}${stackStr}`;
  }),
);

// Create a single crash transport to reuse
let crashTransport: DailyRotateFile | null = null;
const getCrashTransport = (): DailyRotateFile => {
  if (!crashTransport) {
    crashTransport = createRotateTransport('crash', undefined, '7d');
  }
  return crashTransport;
};

const createRotateTransport = (
  filename: string,
  level?: string,
  maxFiles = '14d',
): DailyRotateFile => {
  const transport = new DailyRotateFile({
    filename: path.join(LOG_DIR, `${filename}-%DATE%.log`),
    datePattern: 'YYYY-MM-DD',
    maxSize: '20m',
    maxFiles,
    level,
    format: logFormat,
  });

  // Prevent listener accumulation
  transport.setMaxListeners(20);

  return transport;
};

const transports: winston.transport[] = [
  createRotateTransport('app', undefined, '7d'),
  createRotateTransport('error', 'error', '14d'),
];

const consoleTransport = new winston.transports.Console({
  format: consoleFormat,
});

if (process.env.NODE_ENV !== 'test') {
  transports.push(consoleTransport);
}

export const winstonLogger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  defaultMeta: { service: 'enfyra-server' },
  transports,
  exceptionHandlers: [getCrashTransport()],
  rejectionHandlers: [getCrashTransport()],
  exitOnError: false,
});

// Set max listeners on the logger itself
winstonLogger.setMaxListeners(20);

export const shouldLog = (context?: string): boolean => {
  if (!context) return true;
  return !EXCLUDED_CONTEXTS.includes(context);
};

export const logToFile = (
  level: string,
  message: string,
  context?: string,
  meta?: any,
  trace?: string,
): void => {
  if (!shouldLog(context)) return;

  const metaObj: any = { ...meta };
  if (context) metaObj.context = context;
  if (trace) metaObj.trace = trace;

  winstonLogger.log(level, message, metaObj);
};