import { Inject, Injectable, Logger, forwardRef } from '@nestjs/common';
import { MetadataSyncService } from './metadata-sync.service';
import { DataSourceService } from '../../../core/database/data-source/data-source.service';
import * as crypto from 'crypto';

@Injectable()
export class SchemaHistoryService {
  private readonly logger = new Logger(SchemaHistoryService.name);

  constructor(
    @Inject(forwardRef(() => MetadataSyncService))
    private readonly metadataSyncService: MetadataSyncService,
    private dataSourceService: DataSourceService,
  ) {}

  async backup() {
    const tableDefRepo =
      this.dataSourceService.getRepository('table_definition');
    const schemaHistoryRepo =
      this.dataSourceService.getRepository('schema_history');

    const tables = await tableDefRepo
      .createQueryBuilder('table')
      .leftJoinAndSelect('table.columns', 'columns')
      .leftJoinAndSelect('table.relations', 'relations')
      .getMany();

    const oldestSchema: any = await schemaHistoryRepo.findOne({
      where: {},
      order: { createdAt: 'DESC' },
    });
    // Normalize tables by removing timestamps and sorting
    const normalizedTables = tables.map((table: any) => ({
      ...table,
      createdAt: undefined,
      updatedAt: undefined,
      columns: table.columns?.map((col: any) => ({
        ...col,
        createdAt: undefined,
        updatedAt: undefined
      })).sort((a: any, b: any) => a.id - b.id),
      relations: table.relations?.map((rel: any) => ({
        ...rel,
        createdAt: undefined,
        updatedAt: undefined
      })).sort((a: any, b: any) => a.id - b.id)
    })).sort((a: any, b: any) => a.id - b.id);
    
    const tableJson = JSON.stringify(normalizedTables);
    const hash = crypto
      .createHash('sha256')
      .update(tableJson)
      .digest('hex');
      
    
    if (hash === oldestSchema?.hash) {
      this.logger.debug(`Schema unchanged, skipping backup`);
      return oldestSchema.id;
    }
    const historyCount = await schemaHistoryRepo.count();
    if (historyCount > 20) {
      const oldestRecord: any = await schemaHistoryRepo.findOne({
        where: {},
        order: { createdAt: 'ASC' },
      });
      if (oldestRecord) {
        await schemaHistoryRepo.delete(oldestRecord.id);
        this.logger.debug(`Cleaned up old schema history record: ${oldestRecord.id}`);
      }
    }

    const result: any = await schemaHistoryRepo.save({ schema: tables, hash });
    this.logger.log('✅ Đã backup metadata hiện tại vào schema_history');
    return result.id;
  }

  async restore(options?: { entityName?: string; type: 'create' | 'update' }) {
    const tableDefRepo =
      this.dataSourceService.getRepository('table_definition');
    const schemaHistoryRepo =
      this.dataSourceService.getRepository('schema_history');
    if (options.type === 'create') {
      await tableDefRepo.delete({ name: options.entityName });
    }

    const oldest: any = await schemaHistoryRepo.findOne({
      where: {},
      order: { createdAt: 'DESC' },
    });

    if (oldest) {
      await tableDefRepo.save(oldest.schema);
      this.logger.warn('⚠️ Đã khôi phục metadata từ schema_history');
      // Fire & forget syncAll
      this.metadataSyncService.syncAll({
        fromRestore: true,
        type: options?.type,
      }).catch(error => {
        this.logger.error('Restore syncAll failed:', error.message);
      });
    } else {
      this.logger.warn('⚠️ Không có bản backup schema nào để khôi phục');
    }
  }
}
