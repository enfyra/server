import * as path from 'path';
import * as fs from 'fs';
import pino, { Logger as PinoLogger } from 'pino';
import { logStore } from './log-store';
import { getBootstrapLogMode } from './bootstrap-log-context';

const LOG_DIR = path.join(process.cwd(), 'logs');
if (!fs.existsSync(LOG_DIR)) {
  fs.mkdirSync(LOG_DIR, { recursive: true });
}

type LevelName = 'error' | 'warn' | 'log' | 'debug' | 'verbose';

const RESET = '\x1b[0m';
const DIM = '\x1b[38;5;245m';
const BRACKET = '\x1b[38;5;8m';
const ARROW = '\x1b[38;5;7m→';
const SERVICE = '\x1b[38;5;220m';
const CORR = '\x1b[38;5;141m';

const LEVEL_ICONS: Record<LevelName, string> = {
  log: '◈',
  error: '✖',
  warn: '⚠',
  debug: '○',
  verbose: '·',
};

const LEVEL_COLORS: Record<LevelName, string> = {
  log: '\x1b[32m',
  error: '\x1b[31m',
  warn: '\x1b[33m',
  debug: '\x1b[36m',
  verbose: '\x1b[90m',
};

const PINO_LEVEL: Record<
  LevelName,
  'info' | 'warn' | 'error' | 'debug' | 'trace'
> = {
  log: 'info',
  error: 'error',
  warn: 'warn',
  debug: 'debug',
  verbose: 'trace',
};

const LEVEL_PRIORITY: Record<LevelName, number> = {
  error: 0,
  warn: 1,
  log: 2,
  debug: 3,
  verbose: 4,
};

const LOG_LEVEL_PRIORITY: Record<string, number> = {
  error: 0,
  warn: 1,
  info: 2,
  log: 2,
  debug: 3,
  verbose: 4,
  trace: 4,
};

const BOOTSTRAP_QUIET_CONTEXTS = new Set([
  'TableDefinitionProcessor',
  'ColumnDefinitionProcessor',
  'RelationDefinitionProcessor',
  'UserDefinitionProcessor',
  'MenuDefinitionProcessor',
  'RouteDefinitionProcessor',
  'RouteHandlerDefinitionProcessor',
  'MethodDefinitionProcessor',
  'PreHookDefinitionProcessor',
  'PostHookDefinitionProcessor',
  'FieldPermissionDefinitionProcessor',
  'SettingDefinitionProcessor',
  'ExtensionDefinitionProcessor',
  'FolderDefinitionProcessor',
  'BootstrapScriptDefinitionProcessor',
  'RoutePermissionDefinitionProcessor',
  'WebsocketDefinitionProcessor',
  'WebsocketEventDefinitionProcessor',
  'FlowDefinitionProcessor',
  'FlowStepDefinitionProcessor',
  'FlowExecutionDefinitionProcessor',
  'GraphQLDefinitionProcessor',
  'GenericTableProcessor',
  'DataMigrationService',
  'DataProvisionService',
  'MetadataMigrationService',
  'SchemaHealingService',
  'MetadataProvisionMongoService',
  'MetadataProvisionSqlService',
]);

let logCounter = 0;
function generateLogId(): string {
  const t = Date.now().toString(36);
  const c = (logCounter++).toString(36).padStart(4, '0');
  const r = Math.random().toString(36).substring(2, 6);
  return `log_${t}_${c}_${r}`;
}

function formatTime(d: Date): string {
  const pad = (n: number) => n.toString().padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

const NOOP_STREAM: pino.DestinationStream = {
  write: () => {},
};

function buildTransport(): pino.DestinationStream | undefined {
  if (process.env.LOG_DISABLE_FILES === '1') return NOOP_STREAM;
  try {
    return pino.transport({
      targets: [
        {
          level: 'info',
          target: 'pino-roll',
          options: {
            file: path.join(LOG_DIR, 'app'),
            frequency: 'daily',
            size: '20m',
            extension: '.log',
            mkdir: true,
          },
        },
        {
          level: 'error',
          target: 'pino-roll',
          options: {
            file: path.join(LOG_DIR, 'error'),
            frequency: 'daily',
            size: '20m',
            extension: '.log',
            mkdir: true,
          },
        },
      ],
    });
  } catch {
    return undefined;
  }
}

const pinoInstance: PinoLogger = pino(
  {
    level: 'info',
    messageKey: 'message',
    base: { service: 'enfyra-server' },
    formatters: {
      level: (label: string) => ({ level: label }),
    },
    timestamp: () => `,"timestamp":"${new Date().toISOString()}"`,
    mixin: () => {
      const store = logStore.getStore();
      const mix: Record<string, any> = { id: generateLogId() };
      if (store?.correlationId) mix.correlationId = store.correlationId;
      if (store?.context) {
        for (const [k, v] of Object.entries(store.context)) {
          if (mix[k] === undefined) mix[k] = v;
        }
      }
      return mix;
    },
  },
  buildTransport(),
);

export function __pinoInstanceForTests(): PinoLogger {
  return pinoInstance;
}

function extractObjectMessage(
  payload: Record<string, any>,
  fallback: string,
): { msg: string; meta: Record<string, any> } {
  const { message, msg: msgField, ...rest } = payload;
  const picked =
    (typeof message === 'string' && message) ||
    (typeof msgField === 'string' && msgField) ||
    fallback;
  return { msg: picked, meta: rest };
}

function printPretty(
  level: LevelName,
  msg: string,
  context: string | undefined,
  correlationId: string | undefined,
  trace?: string,
): void {
  if (process.env.LOG_DISABLE_CONSOLE === '1') return;
  const icon = LEVEL_ICONS[level];
  const iconColor = LEVEL_COLORS[level];
  const time = formatTime(new Date());
  const emphasize = level === 'error' || level === 'warn';
  const ctxColor = emphasize ? iconColor : SERVICE;
  const ctxStr = context ? `${ctxColor}${context}${RESET} ` : '';
  const corrStr = correlationId ? `${CORR}[${correlationId}]${RESET} ` : '';
  const msgColored = emphasize ? `${iconColor}${msg}${RESET}` : msg;
  const line = `${BRACKET}[${time}]${RESET} ${iconColor}${icon}${RESET} ${ctxStr}${corrStr}${ARROW} ${msgColored}`;
  const target = level === 'error' ? console.error : console.log;
  target(line);
  if (trace) {
    console.error(`${DIM}  ${trace}${RESET}`);
  }
}

function shouldEmit(level: LevelName, context: string | undefined): boolean {
  if (level === 'error' || level === 'warn') return true;

  const configured = process.env.LOG_LEVEL || 'info';
  const maxPriority = LOG_LEVEL_PRIORITY[configured] ?? LOG_LEVEL_PRIORITY.info;
  if (LEVEL_PRIORITY[level] > maxPriority) return false;

  if (
    getBootstrapLogMode() === 'quiet' &&
    context &&
    BOOTSTRAP_QUIET_CONTEXTS.has(context)
  ) {
    return false;
  }

  return true;
}

export class Logger {
  private readonly context?: string;

  constructor(context?: string) {
    this.context = context;
  }

  log(message: any, context?: string): void {
    this.emit('log', message, undefined, context);
  }

  error(
    message: any,
    trace?: unknown,
    context?: string,
  ): void {
    this.emit('error', message, trace, context);
  }

  warn(message: any, context?: string): void {
    this.emit('warn', message, undefined, context);
  }

  debug(message: any, context?: string): void {
    this.emit('debug', message, undefined, context);
  }

  verbose(message: any, context?: string): void {
    this.emit('verbose', message, undefined, context);
  }

  fatal(message: any, trace?: unknown, context?: string): void {
    this.emit('error', message, trace, context, { fatal: true });
  }

  private emit(
    level: LevelName,
    message: any,
    trace: unknown,
    context: string | undefined,
    extraMeta?: Record<string, any>,
  ): void {
    const ctx = context || this.context;
    const fallback =
      level === 'log'
        ? 'Log'
        : level === 'error'
          ? 'Error'
          : level === 'warn'
            ? 'Warning'
            : level === 'debug'
              ? 'Debug'
              : 'Verbose';

    let msg: string;
    const meta: Record<string, any> = { ...(extraMeta || {}) };

    if (typeof message === 'object' && message !== null) {
      if (message instanceof Error) {
        msg = message.message || fallback;
        meta.stack = message.stack;
      } else {
        const extracted = extractObjectMessage(message, fallback);
        msg = extracted.msg;
        Object.assign(meta, extracted.meta);
      }
    } else if (message === undefined || message === null) {
      msg = String(message);
    } else {
      msg = String(message);
    }

    if (trace instanceof Error) {
      meta.stack = trace.stack || trace.message;
    } else if (typeof trace === 'string') {
      meta.stack = trace;
    } else if (trace && typeof trace === 'object') {
      const { message: _m, ...rest } = trace as Record<string, any>;
      for (const [k, v] of Object.entries(rest)) {
        if (meta[k] === undefined) meta[k] = v;
      }
    }

    if (ctx) meta.context = ctx;

    if (!shouldEmit(level, ctx)) {
      return;
    }

    const pinoLevel = PINO_LEVEL[level];
    pinoInstance[pinoLevel](meta, msg);

    const correlationId = logStore.getStore()?.correlationId;
    const consoleTrace = level === 'error' ? meta.stack : undefined;
    printPretty(level, msg, ctx, correlationId, consoleTrace);
  }
}
