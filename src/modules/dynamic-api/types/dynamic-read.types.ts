export interface DynamicReadOptions {
  filter?: unknown;
  fields?: string | string[];
  limit?: number;
  sort?: string;
  meta?: string | string[];
  aggregate?: unknown;
  deep?: Record<string, unknown>;
}
