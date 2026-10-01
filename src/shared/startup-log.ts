import { AsyncLocalStorage } from 'node:async_hooks';
import {
  getBootstrapLogMode,
  runWithBootstrapLogMode,
} from './bootstrap-log-context';
import type { StartupProgressMode, StartupStep } from './types/startup.types';

const startupStore = new AsyncLocalStorage<{ active: boolean }>();
let progressDepth = 0;
let lineLength = 0;
let consoleDepth = 0;
const consoleMethods = ['log', 'info', 'warn', 'debug', 'error'] as const;
const originalConsole = new Map<
  (typeof consoleMethods)[number],
  typeof console.log
>();

export function isStartupVerbose(): boolean {
  return process.env.STARTUP_VERBOSE === '1';
}

export function suppressRawConsole(): void {
  if (++consoleDepth !== 1) return;
  for (const method of consoleMethods) {
    const original = console[method];
    originalConsole.set(method, original);
    console[method] = (...args: unknown[]) => {
      if (method !== 'error' && getBootstrapLogMode() === 'quiet') return;
      clearStartupProgressLine();
      original.apply(console, args);
    };
  }
}

export function restoreRawConsole(): void {
  if (consoleDepth === 0 || --consoleDepth !== 0) return;
  for (const [method, original] of originalConsole) console[method] = original;
  originalConsole.clear();
}

export function beginStartupProgressLine(): void {
  progressDepth++;
}

export function endStartupProgressLine(): void {
  if (progressDepth === 0 || --progressDepth !== 0) return;
  clearStartupProgressLine();
}

export function isStartupProgressLineActive(): boolean {
  return progressDepth > 0;
}

export function isStartupOutputActive(): boolean {
  return startupStore.getStore()?.active === true;
}

function terminalWidth(): number {
  const columns = Number(process.stdout.columns);
  return Number.isFinite(columns) && columns > 1
    ? Math.floor(columns) - 1
    : Infinity;
}

function fitProgressLine(line: string): string {
  const width = terminalWidth();
  const chars = Array.from(line.replace(/[\r\n]/g, ' '));
  if (chars.length <= width) return chars.join('');
  const suffix = line.match(/ \(\d+\/\d+\)$/)?.[0] ?? '';
  if (suffix && width > suffix.length + 1) {
    return `${chars.slice(0, width - suffix.length - 1).join('')}…${suffix}`;
  }
  return `${chars.slice(0, Math.max(0, width - 1)).join('')}…`;
}

export function startupProgressWrite(line: string, terminal = false): void {
  if (process.env.LOG_DISABLE_CONSOLE === '1') return;
  if (getBootstrapLogMode() === 'verbose' || isStartupVerbose()) return;
  if (!process.stdout.isTTY) {
    if (terminal && !isStartupOutputActive()) process.stdout.write(`${line}\n`);
    return;
  }
  const fitted = fitProgressLine(line);
  const width = Array.from(fitted).length;
  const padding = ' '.repeat(
    Math.max(0, Math.min(lineLength, terminalWidth()) - width),
  );
  process.stdout.write(`\r${fitted}${padding}`);
  lineLength = width;
}

export function clearStartupProgressLine(): void {
  if (
    lineLength > 0 &&
    process.stdout.isTTY &&
    process.env.LOG_DISABLE_CONSOLE !== '1'
  ) {
    process.stdout.write(
      `\r${' '.repeat(Math.min(lineLength, terminalWidth()))}\r`,
    );
  }
  lineLength = 0;
}

export function formatStartupProgressLine(
  mode: StartupProgressMode,
  percent: number,
  message: string,
): string {
  const normalized = Math.min(100, Math.max(0, Math.round(percent * 10) / 10));
  const filled = Math.round((normalized * 30) / 100);
  const bar = '█'.repeat(filled) + '░'.repeat(30 - filled);
  const now = new Date();
  const pad = (value: number) => value.toString().padStart(2, '0');
  const time = `${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`;
  const percentText = Number.isInteger(normalized)
    ? normalized.toFixed(0)
    : normalized.toFixed(1);
  return `[${time}] ${mode} [${bar}] ${percentText.padStart(5, ' ')}% ${message}`;
}

export async function runStartupSteps(
  steps: readonly StartupStep[],
): Promise<void> {
  let completed = 0;
  const report = (label: string) => {
    if (!isStartupOutputActive()) return;
    startupProgressWrite(
      formatStartupProgressLine(
        'Starting',
        (completed * 100) / steps.length,
        `${label} (${completed}/${steps.length})`,
      ),
    );
  };
  for (const step of steps) {
    report(step.label);
    await step.run();
    completed++;
    report(step.label);
  }
}

export function runWithStartupOutput<T>(
  callback: () => Promise<T>,
): Promise<T> {
  const state = { active: true };
  return startupStore.run(state, () =>
    runWithBootstrapLogMode(
      isStartupVerbose() ? 'verbose' : 'quiet',
      async () => {
        beginStartupProgressLine();
        suppressRawConsole();
        try {
          startupProgressWrite(
            formatStartupProgressLine('Starting', 0, 'preparing runtime'),
          );
          return await callback();
        } finally {
          state.active = false;
          endStartupProgressLine();
          restoreRawConsole();
        }
      },
    ),
  );
}
