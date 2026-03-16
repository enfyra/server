import { Injectable, Logger, BadRequestException, NotFoundException } from '@nestjs/common';
import * as fs from 'fs';
import * as path from 'path';

export interface LogFile {
  name: string;
  size: number;
  lineCount: number;
  createdAt: Date;
  lastModified: Date;
  compressed: boolean;
}

export interface ParsedLogEntry {
  id: string;
  timestamp: string;
  level: string;
  context?: string;
  correlationId?: string;
  message: string;
  data?: any;
  trace?: string;
  stack?: string;
}

export interface LogContent {
  file: string;
  lines: ParsedLogEntry[];
  rawLines?: string[];
  totalLines: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

@Injectable()
export class LogReaderService {
  private readonly logger = new Logger(LogReaderService.name);
  private readonly logDir: string;

  constructor() {
    this.logDir = path.join(process.cwd(), 'logs');
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

  getLogFiles(): LogFile[] {
    if (!fs.existsSync(this.logDir)) {
      return [];
    }

    const files = fs.readdirSync(this.logDir);
    const logFiles: LogFile[] = [];

    for (const file of files) {
      // Skip hidden files, non-log files, compressed files, and PM2 logs
      if (file.startsWith('.') || (!file.endsWith('.log') && !file.endsWith('.gz')) || file.startsWith('pm2-')) {
        continue;
      }

      const filePath = path.join(this.logDir, file);
      try {
        const stats = fs.statSync(filePath);
        const isCompressed = file.endsWith('.gz');

        let lineCount = 0;
        if (!isCompressed && stats.size > 0) {
          const content = fs.readFileSync(filePath, 'utf-8');
          lineCount = content.split('\n').filter(line => line.trim()).length;
        }

        logFiles.push({
          name: file,
          size: stats.size,
          lineCount,
          createdAt: stats.birthtime,
          lastModified: stats.mtime,
          compressed: isCompressed,
        });
      } catch (error) {
        this.logger.warn(`Failed to read log file: ${file}`);
      }
    }

    return logFiles.sort((a, b) => b.lastModified.getTime() - a.lastModified.getTime());
  }

  getLogContent(
    filename: string,
    page: number = 1,
    pageSize: number = 100,
    filter?: string,
    level?: string,
    id?: string,
    correlationId?: string,
    raw: boolean = false,
  ): LogContent {
    const filePath = path.join(this.logDir, filename);

    if (!fs.existsSync(filePath)) {
      throw new NotFoundException(`Log file not found: ${filename}`);
    }

    if (filename.endsWith('.gz')) {
      throw new BadRequestException('Cannot read compressed log files directly');
    }

    const content = fs.readFileSync(filePath, 'utf-8');
    let rawLines = content.split('\n').filter(line => line.trim());

    // Smart detection: if id starts with "req_", treat it as correlationId
    if (id && id.startsWith('req_')) {
      correlationId = id;
      id = undefined;
    }

    if (id) {
      rawLines = rawLines.filter(line => {
        try {
          const parsed = JSON.parse(line);
          return parsed.id === id;
        } catch {
          return false;
        }
      });
    }

    if (correlationId) {
      rawLines = rawLines.filter(line => {
        try {
          const parsed = JSON.parse(line);
          return parsed.correlationId === correlationId ||
                 parsed.context?.correlationId === correlationId;
        } catch {
          return false;
        }
      });
    }

    if (level) {
      rawLines = rawLines.filter(line => {
        const parsed = this.parseLogLine(line);
        return parsed?.level === level;
      });
    }

    if (filter) {
      const filterLower = filter.toLowerCase();
      rawLines = rawLines.filter(line => line.toLowerCase().includes(filterLower));
    }

    const totalLines = rawLines.length;
    rawLines = rawLines.reverse();
    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const paginatedRawLines = rawLines.slice(startIndex, endIndex);

    const lines = raw ? undefined : paginatedRawLines.map(line => this.parseLogLine(line)).filter(Boolean);

    return {
      file: filename,
      lines: lines || [],
      rawLines: raw ? paginatedRawLines : undefined,
      totalLines,
      page,
      pageSize,
      hasMore: endIndex < totalLines,
    };
  }

  getLogStats(): {
    totalSize: number;
    totalSizeFormatted: string;
    totalLines: number;
    fileCount: number;
    oldestFile?: string;
    newestFile?: string;
  } {
    const files = this.getLogFiles();

    if (files.length === 0) {
      return { totalSize: 0, totalSizeFormatted: '0 B', totalLines: 0, fileCount: 0 };
    }

    const totalSize = files.reduce((sum, f) => sum + f.size, 0);
    const totalLines = files.reduce((sum, f) => sum + f.lineCount, 0);
    const uncompressed = files.filter(f => !f.compressed);

    const sortedByDate = [...uncompressed].sort(
      (a, b) => a.lastModified.getTime() - b.lastModified.getTime()
    );

    return {
      totalSize,
      totalSizeFormatted: this.formatBytes(totalSize),
      totalLines,
      fileCount: files.length,
      oldestFile: sortedByDate[0]?.name,
      newestFile: sortedByDate[sortedByDate.length - 1]?.name,
    };
  }

  tailLog(filename: string, lines: number = 50, raw: boolean = false): { lines: (ParsedLogEntry | string)[] } {
    const filePath = path.join(this.logDir, filename);

    if (!fs.existsSync(filePath)) {
      throw new NotFoundException(`Log file not found: ${filename}`);
    }

    if (filename.endsWith('.gz')) {
      throw new BadRequestException('Cannot read compressed log files directly');
    }

    const content = fs.readFileSync(filePath, 'utf-8');
    const allLines = content.split('\n').filter(line => line.trim());
    const lastLines = allLines.slice(-lines).reverse();

    if (raw) {
      return { lines: lastLines };
    }

    return { lines: lastLines.map(line => this.parseLogLine(line)).filter(Boolean) };
  }
}