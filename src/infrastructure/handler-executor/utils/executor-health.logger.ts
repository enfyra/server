import * as path from 'path';
import * as fs from 'fs';
import * as winston from 'winston';
import * as DailyRotateFile from 'winston-daily-rotate-file';
import { HealthCheckStats } from '../types/health.types';

const LOG_DIR = path.join(process.cwd(), 'logs');

if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

let logCounter = 0;
const generateLogId = (): string => {
  const timestamp = Date.now().toString(36);
  const counter = (logCounter++).toString(36).padStart(4, '0');
  const random = Math.random().toString(36).substring(2, 6);
  return `eh_${timestamp}_${counter}_${random}`;
};

const addLogId = winston.format((info) => {
  info.id = generateLogId();
  return info;
});

const logFormat = winston.format.combine(
  addLogId(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
  winston.format.printf(({ id, timestamp, level, message }) => {
    const idStr = id ? `[${id}] ` : '';
    const contextStr = '[ExecutorHealth] ';
    return `${timestamp} ${level} ${idStr}${contextStr}${message}`;
  }),
);

const consoleFormat = winston.format.combine(
  addLogId(),
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.colorize({ all: true }),
  winston.format.printf(({ id, timestamp, level, message }) => {
    const idStr = id ? `[${id}] ` : '';
    const contextStr = '[ExecutorHealth] ';
    return `${timestamp} ${level} ${idStr}${contextStr}${message}`;
  }),
);

const createHealthTransport = (): DailyRotateFile => {
  return new DailyRotateFile({
    filename: path.join(LOG_DIR, 'executor-health-%DATE%.log'),
    datePattern: 'YYYY-MM-DD',
    maxSize: '10m',
    maxFiles: '7d',
    format: logFormat,
  });
};

const logger = winston.createLogger({
  level: 'debug',
  transports: [createHealthTransport()],
});

if (process.env.NODE_ENV !== 'production' && process.env.NODE_ENV !== 'test') {
  logger.add(
    new winston.transports.Console({
      level: 'warn',
      format: consoleFormat,
    }),
  );
}

export class ExecutorHealthLogger {
  logPoolInit(config: { min: number; max: number }): void {}

  logProcessCreated(pid: number): void {}

  logProcessDestroyed(pid: number, reason?: string): void {}

  logHealthStats(stats: HealthCheckStats): void {}

  logRecycle(
    pid: number,
    reasons: string[],
    details: {
      executionCount?: number;
      errorCount?: number;
      ageMs?: number;
      lastError?: string;
    },
  ): void {
    const reasonsStr = reasons.join(', ');

    const parts: string[] = [];
    if (details.executionCount !== undefined) {
      parts.push(`Execs: ${details.executionCount}`);
    }
    if (details.errorCount !== undefined && details.errorCount > 0) {
      parts.push(`Errors: ${details.errorCount}`);
    }
    if (details.ageMs !== undefined) {
      parts.push(`Age: ${Math.round(details.ageMs / 60000)}min`);
    }

    let message = `[Recycle] PID: ${pid} | Reasons: ${reasonsStr}`;

    if (parts.length > 0) {
      message += ` | ${parts.join(' | ')}`;
    }

    if (details.lastError) {
      const sanitizedError = details.lastError.replace(/\n/g, '\\n').replace(/\r/g, '\\r');
      const truncatedError =
        sanitizedError.length > 100 ? sanitizedError.substring(0, 100) + '...' : sanitizedError;
      message += ` | LastError: "${truncatedError}"`;
    }

    logger.warn(message);
  }

  logQuickCheckFailed(pid: number, reason: string): void {
    logger.warn(`[QuickCheckFailed] PID: ${pid} | Reason: ${reason}`);
  }

  logExecutionError(pid: number, errorCount: number, error: string): void {
    const sanitizedError = error.replace(/\n/g, '\\n').replace(/\r/g, '\\r');
    const truncatedError = sanitizedError.length > 80 ? sanitizedError.substring(0, 80) + '...' : sanitizedError;
    logger.error(`[ExecutionError] PID: ${pid} | ErrorCount: ${errorCount} | Error: "${truncatedError}"`);
  }

  logUnexpectedExit(pid: number, exitCode: number | null, signal: string | null): void {
    logger.error(`[UnexpectedExit] PID: ${pid} | ExitCode: ${exitCode} | Signal: ${signal}`);
  }

  logScaleUp(fromSize: number, toSize: number, reason: string, configMax?: number, autoScaleMax?: number): void {
    const limitInfo = configMax && autoScaleMax ? ` | ConfigMax: ${configMax} → AutoScaleMax: ${autoScaleMax}` : '';
    logger.warn(`[ScaleUp] Pool: ${fromSize} → ${toSize}${limitInfo} | Reason: ${reason}`);
  }

  logScaleDown(fromSize: number, toSize: number, reason: string): void {
    logger.warn(`[ScaleDown] Pool: ${fromSize} → ${toSize} | Reason: ${reason}`);
  }

  logPoolMetrics(metrics: { size: number; borrowed: number; pending: number; utilization: number; pressureRatio: number }): void {
    logger.warn(`[PoolMetrics] Size: ${metrics.size} | Borrowed: ${metrics.borrowed} | Pending: ${metrics.pending} | Util: ${(metrics.utilization * 100).toFixed(0)}% | Pressure: ${metrics.pressureRatio.toFixed(2)}`);
  }
}