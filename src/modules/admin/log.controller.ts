import { Controller, Get, Query, Param, ParseIntPipe } from '@nestjs/common';
import { LogReaderService, LogFile, LogContent } from './services/log-reader.service';

@Controller('logs')
export class LogController {
  constructor(private readonly logReaderService: LogReaderService) {}

  @Get()
  listLogFiles(): {
    files: LogFile[];
    stats: ReturnType<LogReaderService['getLogStats']>;
  } {
    const files = this.logReaderService.getLogFiles();
    const stats = this.logReaderService.getLogStats();
    return { files, stats };
  }

  @Get('stats')
  getStats(): ReturnType<LogReaderService['getLogStats']> {
    return this.logReaderService.getLogStats();
  }

  @Get(':filename')
  async getLogContent(
    @Param('filename') filename: string,
    @Query('page', new ParseIntPipe({ optional: true })) page: number = 1,
    @Query('pageSize', new ParseIntPipe({ optional: true })) pageSize: number = 100,
    @Query('filter') filter?: string,
    @Query('level') level?: string,
    @Query('id') id?: string,
    @Query('correlationId') correlationId?: string,
    @Query('raw') raw?: string,
  ): Promise<LogContent> {
    return this.logReaderService.getLogContent(filename, page, pageSize, filter, level, id, correlationId, raw === 'true');
  }

  @Get(':filename/tail')
  tailLog(
    @Param('filename') filename: string,
    @Query('lines', new ParseIntPipe({ optional: true })) lines: number = 50,
    @Query('raw') raw?: string,
  ): { lines: any[] } {
    return this.logReaderService.tailLog(filename, lines, raw === 'true');
  }
}