import * as path from 'path';

export function resolveLogDirectory(): string {
  const configured = process.env.LOG_DIR?.trim();
  return configured
    ? path.resolve(configured)
    : path.resolve(process.cwd(), 'logs');
}
