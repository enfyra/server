import { Response } from 'express';
import { Logger } from '../../../shared/logger';
import { PassThrough, type Readable } from 'stream';
import * as fs from 'fs';
import * as sharp from 'sharp';
import type { StorageByteRange } from '../storage/storage.interface';

export type ParsedHttpRange =
  | { type: 'full' }
  | { type: 'partial'; range: StorageByteRange; contentLength: number }
  | { type: 'invalid' };

export class StreamHelper {
  private readonly logger = new Logger(StreamHelper.name);

  parseHttpRange(
    rangeHeader: string | undefined,
    totalSize?: number,
  ): ParsedHttpRange {
    if (!rangeHeader) return { type: 'full' };
    if (!totalSize || totalSize <= 0) return { type: 'invalid' };
    if (!rangeHeader.startsWith('bytes=') || rangeHeader.includes(',')) {
      return { type: 'invalid' };
    }

    const rangeValue = rangeHeader.slice('bytes='.length).trim();
    const match = /^(\d*)-(\d*)$/.exec(rangeValue);
    if (!match) return { type: 'invalid' };

    const [, startRaw, endRaw] = match;
    if (!startRaw && !endRaw) return { type: 'invalid' };

    let start: number;
    let end: number;

    if (!startRaw) {
      const suffixLength = Number(endRaw);
      if (!Number.isSafeInteger(suffixLength) || suffixLength <= 0) {
        return { type: 'invalid' };
      }
      start = Math.max(totalSize - suffixLength, 0);
      end = totalSize - 1;
    } else {
      start = Number(startRaw);
      end = endRaw ? Number(endRaw) : totalSize - 1;
      if (
        !Number.isSafeInteger(start) ||
        !Number.isSafeInteger(end) ||
        start < 0 ||
        end < start ||
        start >= totalSize
      ) {
        return { type: 'invalid' };
      }
      end = Math.min(end, totalSize - 1);
    }

    return {
      type: 'partial',
      range: { start, end },
      contentLength: end - start + 1,
    };
  }

  sendRangeNotSatisfiable(res: Response, totalSize?: number): void {
    if (totalSize && totalSize > 0) {
      res.setHeader('Content-Range', `bytes */${totalSize}`);
    }
    res.status(416).end();
  }

  async streamRegularFile(
    filePath: string,
    res: Response,
    filename: string,
    mimetype: string,
    shouldDownload?: boolean,
    rangeHeader?: string,
  ): Promise<void> {
    const stats = await fs.promises.stat(filePath);
    const parsedRange = this.parseHttpRange(rangeHeader, stats.size);
    if (parsedRange.type === 'invalid') {
      this.sendRangeNotSatisfiable(res, stats.size);
      return;
    }

    const range =
      parsedRange.type === 'partial' ? parsedRange.range : undefined;
    res.setHeader('Content-Type', mimetype);
    res.setHeader('Accept-Ranges', 'bytes');
    res.setHeader(
      'Content-Length',
      parsedRange.type === 'partial' ? parsedRange.contentLength : stats.size,
    );
    if (parsedRange.type === 'partial') {
      res.status(206);
      res.setHeader(
        'Content-Range',
        `bytes ${parsedRange.range.start}-${parsedRange.range.end}/${stats.size}`,
      );
    }
    res.setHeader(
      'Content-Disposition',
      shouldDownload
        ? `attachment; filename="${filename}"`
        : `inline; filename="${filename}"`,
    );
    const fileStream = fs.createReadStream(filePath, range);
    this.handleStreamError(fileStream, res, 'File stream error');
    fileStream.pipe(res);
  }
  async streamCloudFile(
    stream: any,
    res: Response,
    filename: string,
    mimetype: string,
    shouldDownload?: boolean,
    range?: StorageByteRange,
    totalSize?: number,
  ): Promise<void> {
    res.setHeader('Content-Type', mimetype);
    if (totalSize && totalSize > 0) {
      res.setHeader('Accept-Ranges', 'bytes');
    }
    res.setHeader(
      'Content-Disposition',
      shouldDownload
        ? `attachment; filename="${filename}"`
        : `inline; filename="${filename}"`,
    );
    if (range && totalSize && totalSize > 0) {
      res.status(206);
      res.setHeader('Content-Length', range.end - range.start + 1);
      res.setHeader(
        'Content-Range',
        `bytes ${range.start}-${range.end}/${totalSize}`,
      );
    } else if (stream.contentLength) {
      res.setHeader('Content-Length', stream.contentLength);
    }
    this.handleStreamError(stream, res, 'Cloud stream error');
    stream.pipe(res);
  }
  setupImageStream(
    sharpStream: sharp.Sharp,
    res: Response,
    shouldCache: boolean,
    onCache?: (buffer: Buffer) => Promise<void>,
  ): void {
    this.handleStreamError(sharpStream, res, 'Image processing failed');
    if (shouldCache && onCache) {
      const cacheBuffer: Buffer[] = [];
      const teeStream = new PassThrough();
      this.bindResponseLifecycle(teeStream, res);
      teeStream.on('data', (chunk: Buffer) => {
        cacheBuffer.push(chunk);
      });
      teeStream.on('end', async () => {
        const fullBuffer = Buffer.concat(cacheBuffer);
        if (fullBuffer.length < 10 * 1024 * 1024) {
          await onCache(fullBuffer);
        }
      });
      sharpStream
        .pipe(teeStream)
        .pipe(res)
        .on('error', (error) => {
          this.logger.error('Response stream error:', error);
        });
    } else {
      sharpStream.pipe(res).on('error', (error) => {
        this.logger.error('Response stream error:', error);
      });
    }
  }
  private bindResponseLifecycle(stream: Readable, res: Response): void {
    const closeSource = () => { stream.destroy(); };
    const cleanup = () => {
      res.off('close', closeSource);
      res.off('error', closeSource);
    };
    res.once('close', closeSource);
    res.once('error', closeSource);
    stream.once('close', cleanup);
    if (res.destroyed || res.writableEnded) closeSource();
  }

  handleStreamError(stream: Readable, res: Response, errorMessage: string): void {
    this.bindResponseLifecycle(stream, res);
    stream.on('error', (error: Error) => {
      this.logger.error(`${errorMessage}:`, error);
      if (!res.headersSent && !res.destroyed) res.status(500).json({ error: errorMessage });
      else if (!res.destroyed) res.destroy(error);
    });
  }
}
