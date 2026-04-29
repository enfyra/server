import { Logger } from '../../../shared/logger';
import {
  BadRequestException,
  NotFoundException,
} from '../../../domain/exceptions';
import * as fs from 'fs';
import * as readline from 'readline';
import * as path from 'path';
import type { LogContent, LogFile, ParsedLogEntry } from '../types';

const DEFAULT_PAGE_SIZE = 100;
const MAX_PAGE_SIZE = 1000;

export class LogReaderService {
  private readonly logger = new Logger(LogReaderService.name);
  private readonly logDir: string;

  constructor() {
    this.logDir = path.resolve(process.cwd(), 'logs');
  }

  private validateFilePath(filename: string): string {
    const resolvedPath = path.resolve(this.logDir, filename);

    if (
      !resolvedPath.startsWith(this.logDir + path.sep) &&
      resolvedPath !== this.logDir
    ) {
      this.logger.warn(`Path traversal attempt blocked: ${filename}`);
      throw new BadRequestException('Invalid file path');
    }

    if (filename.includes('..') || path.isAbsolute(filename)) {
      throw new BadRequestException('Invalid file path');
    }

    return resolvedPath;
  }

  private parseLogLine(line: string): ParsedLogEntry | null {
    try {
      const parsed = JSON.parse(line);
      return {
        id: parsed.id || '',
        timestamp: parsed.timestamp || '',
        level: parsed.level || 'info',
        context: parsed.context,
        correlationId: parsed.correlationId || parsed.context?.correlationId,
        message: parsed.message || line,
        data: parsed.data,
        trace: parsed.trace,
        stack: parsed.stack,
      };
    } catch {
      return {
        id: '',
        timestamp: '',
        level: 'info',
        message: line,
      };
    }
  }

  private formatBytes(bytes: number): string {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  }

  private matchesFilter(
    line: string,
    filter?: string,
    level?: string,
    id?: string,
    correlationId?: string,
  ): boolean {
    if (!filter && !level && !id && !correlationId) return true;

    let parsed: any;
    try {
      parsed = JSON.parse(line);
    } catch {
      return filter ? line.toLowerCase().includes(filter.toLowerCase()) : false;
    }

    if (id && parsed.id !== id) return false;
    if (
      correlationId &&
      parsed.correlationId !== correlationId &&
      parsed.context?.correlationId !== correlationId
    )
      return false;
    if (level && parsed.level !== level) return false;
    if (filter && !line.toLowerCase().includes(filter.toLowerCase()))
      return false;

    return true;
  }

  getLogFiles(): LogFile[] {
    if (!fs.existsSync(this.logDir)) {
      return [];
    }

    const files = fs.readdirSync(this.logDir);
    const logFiles: LogFile[] = [];

    for (const file of files) {
      if (
        file.startsWith('.') ||
        !file.endsWith('.log') ||
        file.startsWith('pm2-')
      ) {
        continue;
      }

      const filePath = path.join(this.logDir, file);
      try {
        const stats = fs.statSync(filePath);

        logFiles.push({
          name: file,
          size: stats.size,
          createdAt: stats.birthtime,
          lastModified: stats.mtime,
        });
      } catch (error) {
        this.logger.warn(`Failed to read log file: ${file}`);
      }
    }

    return logFiles.sort(
      (a, b) => b.lastModified.getTime() - a.lastModified.getTime(),
    );
  }

  async getLogContent(
    filename: string,
    page: number = 1,
    pageSize: number = DEFAULT_PAGE_SIZE,
    filter?: string,
    level?: string,
    id?: string,
    correlationId?: string,
    raw: boolean = false,
  ): Promise<LogContent> {
    const filePath = this.validateFilePath(filename);

    if (!fs.existsSync(filePath)) {
      throw new NotFoundException(`Log file not found: ${filename}`);
    }

    if (id && id.startsWith('req_')) {
      correlationId = id;
      id = undefined;
    }

    pageSize = Math.min(Math.max(1, pageSize), MAX_PAGE_SIZE);

    const matchingLines: string[] = [];
    const fetchCount = page * pageSize + 1;

    await new Promise<void>((resolve, reject) => {
      const fileStream = fs.createReadStream(filePath, { encoding: 'utf-8' });
      const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity,
      });

      rl.on('line', (line) => {
        if (!line.trim()) return;
        if (this.matchesFilter(line, filter, level, id, correlationId)) {
          matchingLines.push(line);
          if (matchingLines.length >= fetchCount) {
            rl.close();
            fileStream.destroy();
          }
        }
      });

      rl.on('close', () => resolve());
      rl.on('error', (err) => reject(err));
    });

    matchingLines.reverse();

    const startIndex = (page - 1) * pageSize;
    const hasMore = matchingLines.length > page * pageSize;
    const paginatedLines = matchingLines.slice(
      startIndex,
      startIndex + pageSize,
    );

    const lines = raw
      ? undefined
      : paginatedLines
          .map((line) => this.parseLogLine(line))
          .filter((line): line is ParsedLogEntry => Boolean(line));

    return {
      file: filename,
      lines: lines || [],
      rawLines: raw ? paginatedLines : undefined,
      page,
      pageSize,
      hasMore,
    };
  }

  getLogStats(): {
    totalSize: number;
    totalSizeFormatted: string;
    fileCount: number;
    oldestFile?: string;
    newestFile?: string;
  } {
    const files = this.getLogFiles();

    if (files.length === 0) {
      return { totalSize: 0, totalSizeFormatted: '0 B', fileCount: 0 };
    }

    const totalSize = files.reduce((sum, f) => sum + f.size, 0);

    const sortedByDate = [...files].sort(
      (a, b) => a.lastModified.getTime() - b.lastModified.getTime(),
    );

    return {
      totalSize,
      totalSizeFormatted: this.formatBytes(totalSize),
      fileCount: files.length,
      oldestFile: sortedByDate[0]?.name,
      newestFile: sortedByDate[sortedByDate.length - 1]?.name,
    };
  }

  tailLog(
    filename: string,
    lines: number = 50,
    raw: boolean = false,
  ): { lines: (ParsedLogEntry | string)[] } {
    const filePath = this.validateFilePath(filename);

    if (!fs.existsSync(filePath)) {
      throw new NotFoundException(`Log file not found: ${filename}`);
    }

    const content = fs.readFileSync(filePath, 'utf-8');
    const allLines = content.split('\n').filter((line) => line.trim());
    const lastLines = allLines.slice(-lines).reverse();

    if (raw) {
      return { lines: lastLines };
    }

    return {
      lines: lastLines
        .map((line) => this.parseLogLine(line))
        .filter((line): line is ParsedLogEntry => Boolean(line)),
    };
  }
}
