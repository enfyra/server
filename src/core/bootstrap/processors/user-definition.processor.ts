import { Injectable } from '@nestjs/common';
import { BaseTableProcessor } from './base-table-processor';
import { BcryptService } from '../../auth/services/bcrypt.service';

@Injectable()
export class UserDefinitionProcessor extends BaseTableProcessor {
  constructor(private readonly bcryptService: BcryptService) {
    super();
  }

  async transformRecords(records: any[]): Promise<any[]> {
    // Hash passwords before upsert
    return Promise.all(
      records.map(async (record) => ({
        ...record,
        password: await this.bcryptService.hash(record.password),
      })),
    );
  }

  getUniqueIdentifier(record: any): object {
    return { username: record.username };
  }

  protected getCompareFields(): string[] {
    return ['email', 'isRootAdmin', 'isSystem'];
  }
}