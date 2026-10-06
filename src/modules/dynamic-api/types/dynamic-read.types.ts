export interface DynamicReadOptions {
  filter?: unknown;
  fields?: string | string[];
  limit?: number;
  page?: number;
  sort?: string;
  meta?: string | string[];
  deep?: Record<string, unknown>;
}
