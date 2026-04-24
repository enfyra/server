import { InsertOptions } from '../../../shared/types/query-builder.types';

export interface IQueryBuilder {
  find(options: {
    table: string;
    filter?: any;
    fields?: string[];
    sort?: string | string[];
    limit?: number;
    page?: number;
  }): Promise<{ data: any[] }>;
  findOne(options: { table: string; where?: any; filter?: any }): Promise<any>;
  insert(table: string, data: any): Promise<any>;
  update(table: string, id: any, data: any): Promise<any>;
  delete(table: string, idOrOptions: any): Promise<any>;
  insertWithOptions(options: InsertOptions): Promise<any>;
  isMongoDb(): boolean;
  getPkField(): string;
  getKnex(): any;
  getMongoDb(): any;
}
