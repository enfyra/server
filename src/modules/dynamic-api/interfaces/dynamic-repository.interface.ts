export interface IDynamicRepository {
  fields(fields: string): IDynamicRepository;
  filter(filter: any): IDynamicRepository;
  page(page: number): IDynamicRepository;
  limit(limit: number): IDynamicRepository;
  meta(meta: 'filterCount' | 'totalCount' | '*'): IDynamicRepository;
  sort(sort: string | string[]): IDynamicRepository;
  findMany(): Promise<any>;
  findOne(): Promise<any>;
  findAndCount(): Promise<{ data: any[]; count: number }>;
  exists(filter: any): Promise<boolean>;
  create(data: any): Promise<any>;
  update(id: any, data: any): Promise<any>;
  delete(id: any): Promise<boolean>;
}
