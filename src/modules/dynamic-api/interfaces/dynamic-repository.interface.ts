export interface IDynamicRepository {
  fields(fields: string): IDynamicRepository;
  filter(filter: any): IDynamicRepository;
  page(page: number): IDynamicRepository;
  limit(limit: number): IDynamicRepository;
  meta(meta: 'filterCount' | 'totalCount' | '*'): IDynamicRepository;
  sort(sort: string | string[]): IDynamicRepository;
  aggregate(options: Record<string, unknown>): Promise<any>;
  findMany(): Promise<any>;
  findOne(): Promise<any>;
  findAndCount(): Promise<{ data: any[]; count: number }>;
  exists(filter: any): Promise<boolean>;
  create(data: any): Promise<any>;
  createMany(options: {
    data: Array<Record<string, unknown>>;
    fields?: string | string[];
  }): Promise<{ data: Array<Record<string, unknown>>; count: number }>;
  update(id: any, data: any): Promise<any>;
  updateMany(options: {
    ids: Array<string | number>;
    data: Record<string, unknown>;
    fields?: string | string[];
  }): Promise<{ data: Array<Record<string, unknown>>; count: number }>;
  delete(id: any): Promise<boolean>;
  deleteMany(options: { ids: Array<string | number> }): Promise<{
    message: 'Delete successfully!';
    statusCode: 200;
    count: number;
  }>;
}
