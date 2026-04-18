import { winstonLogger, shouldLog } from './utils/winston-logger';

const MIN_LEVEL: Record<string, number> = {
  error: 0,
  warn: 1,
  log: 2,
  debug: 3,
  verbose: 4,
};

const currentLevel = MIN_LEVEL[process.env.LOG_LEVEL || 'info'] ?? 2;

const RESET = '\x1b[0m';
const DIM = '\x1b[38;5;245m';
const BRACKET = '\x1b[38;5;8m';
const ARROW = '\x1b[38;5;7m→';
const SERVICE = '\x1b[38;5;220m';

const LEVEL_ICONS: Record<string, string> = {
  log: '◈',
  error: '✖',
  warn: '⚠',
  debug: '○',
  verbose: '·',
};

const LEVEL_COLORS: Record<string, string> = {
  log: '\x1b[32m',
  error: '\x1b[31m',
  warn: '\x1b[33m',
  debug: '\x1b[36m',
  verbose: '\x1b[90m',
};

function formatTime(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0');
  const h = pad(d.getHours());
  const m = pad(d.getMinutes());
  const s = pad(d.getSeconds());
  return `${h}:${m}:${s}`;
}

export class Logger {
  private context?: string;
  private shortContext?: string;

  constructor(context?: string) {
    this.context = context;
    if (context) {
      this.shortContext = context
        .replace(/Service$/, '')
        .replace(/Cache$/, '')
        .replace(/Manager$/, '')
        .replace(/Definition$/, '')
        .replace(/Processor$/, '')
        .replace(/Controller$/, '')
        .replace(/Guard$/, '')
        .replace(/Hook$/, '')
        .substring(0, 12);
    }
  }

  log(message: any, context?: string) {
    const ctx = context || this.context;
    if (!shouldLog(ctx)) return;
    this.print('log', message, ctx);
    winstonLogger.info(String(message), { context: ctx });
  }

  error(message: any, trace?: string | Error, context?: string) {
    const ctx = context || this.context;
    if (!shouldLog(ctx)) return;
    this.print('error', message, ctx);
    if (trace) {
      const traceStr = trace instanceof Error ? trace.stack || trace.message : String(trace);
      console.error(`${DIM}  ${traceStr}${RESET}`);
    }
    winstonLogger.error(String(message), { context: ctx, trace: trace instanceof Error ? trace.stack : trace });
  }

  warn(message: any, context?: string) {
    const ctx = context || this.context;
    if (!shouldLog(ctx)) return;
    this.print('warn', message, ctx);
    winstonLogger.warn(String(message), { context: ctx });
  }

  debug(message: any, context?: string) {
    if (currentLevel < MIN_LEVEL.debug) return;
    const ctx = context || this.context;
    if (!shouldLog(ctx)) return;
    this.print('debug', message, ctx);
    winstonLogger.debug(String(message), { context: ctx });
  }

  verbose(message: any, context?: string) {
    if (currentLevel < MIN_LEVEL.verbose) return;
    const ctx = context || this.context;
    if (!shouldLog(ctx)) return;
    this.print('verbose', message, ctx);
    winstonLogger.verbose(String(message), { context: ctx });
  }

  private print(level: string, message: any, context?: string) {
    const time = formatTime(new Date());
    const icon = LEVEL_ICONS[level];
    const iconColor = LEVEL_COLORS[level];

    const shortCtx = context
      ? context
          .replace(/Service$/, '')
          .replace(/Cache$/, '')
          .replace(/Manager$/, '')
          .replace(/Definition$/, '')
          .replace(/Processor$/, '')
          .replace(/Controller$/, '')
          .replace(/Guard$/, '')
          .replace(/Hook$/, '')
          .substring(0, 12)
      : '';

    let msgStr: string;
    if (typeof message === 'object' && message !== null) {
      if (message.message && typeof message.message === 'string') {
        msgStr = message.message;
      } else {
        msgStr = JSON.stringify(message, null, 2);
      }
    } else {
      msgStr = String(message);
    }

    const timeStr = `${BRACKET}[${time}]${RESET}`;
    const iconStr = iconColor + icon + RESET;
    const ctxStr = shortCtx ? `${SERVICE}${shortCtx.padEnd(12)}${RESET} ` : '';
    const arrowStr = `${ARROW} `;

    console.log(`${timeStr} ${iconStr} ${ctxStr}${arrowStr}${msgStr}`);
  }
}
