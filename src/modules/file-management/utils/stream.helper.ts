import { Response } from 'express';
import { Logger } from '@nestjs/common';
import { PassThrough } from 'stream';
import * as fs from 'fs';
import * as sharp from 'sharp';

export class StreamHelper {
  private readonly logger = new Logger(StreamHelper.name);

  async streamRegularFile(
    filePath: string,
    res: Response,
    filename: string,
    mimetype: string,
    shouldDownload?: boolean,
  ): Promise<void> {
    const stats = await fs.promises.stat(filePath);
    res.setHeader('Content-Type', mimetype);
    res.setHeader('Content-Length', stats.size);
    res.setHeader('Content-Disposition', shouldDownload 
      ? `attachment; filename="${filename}"` 
      : `inline; filename="${filename}"`);

    const fileStream = fs.createReadStream(filePath);
    fileStream.on('error', (error) => {
      this.logger.error('File stream error:', error);
      if (!res.headersSent) res.status(500).json({ error: 'Stream error' });
    });
    fileStream.pipe(res);
  }

  async streamCloudFile(
    stream: any,
    res: Response,
    filename: string,
    mimetype: string,
    shouldDownload?: boolean,
  ): Promise<void> {
    res.setHeader('Content-Type', mimetype);
    res.setHeader('Content-Disposition', shouldDownload 
      ? `attachment; filename="${filename}"` 
      : `inline; filename="${filename}"`);

    // Set Content-Length if available (for better performance)
    if (stream.contentLength) {
      res.setHeader('Content-Length', stream.contentLength);
    }

    stream.on('error', (error) => {
      this.logger.error('Cloud stream error:', error);
      if (!res.headersSent) res.status(500).json({ error: 'Stream error' });
    });

    // Direct pipe - no buffering, streams directly to response
    stream.pipe(res);
  }

  setupImageStream(
    sharpStream: sharp.Sharp,
    res: Response,
    shouldCache: boolean,
    onCache?: (buffer: Buffer) => Promise<void>,
  ): void {
    if (shouldCache && onCache) {
      const cacheBuffer: Buffer[] = [];
      const teeStream = new PassThrough();

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
      sharpStream
        .pipe(res)
        .on('error', (error) => {
          this.logger.error('Response stream error:', error);
        });
    }
  }

  handleStreamError(
    stream: any,
    res: Response,
    errorMessage: string,
  ): void {
    stream.on('error', (error: Error) => {
      this.logger.error(`${errorMessage}:`, error);
      if (!res.headersSent) res.status(500).json({ error: errorMessage });
    });
  }
}

