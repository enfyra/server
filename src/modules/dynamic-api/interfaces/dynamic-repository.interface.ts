// Repository interface for dynamic data operations
export interface IDynamicRepository {
  // Configuration methods
  fields(fields: string): IDynamicRepository;
  filter(filter: any): IDynamicRepository;
  page(page: number): IDynamicRepository;
  limit(limit: number): IDynamicRepository;
  meta(meta: 'filterCount' | 'totalCount' | '*'): IDynamicRepository;
  sort(sort: string | string[]): IDynamicRepository;
  aggregate(aggregate: any): IDynamicRepository;

  // Query execution methods
  findMany(): Promise<any>;
  findOne(): Promise<any>;
  findAndCount(): Promise<{ data: any[]; count: number }>;

  // Data manipulation methods
  create(data: any): Promise<any>;
  update(id: any, data: any): Promise<any>;
  delete(id: any): Promise<boolean>;
}
