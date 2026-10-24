export interface TRequestedReadShape {
  includeAll?: boolean;
  columns?: Set<string>;
  relations?: Set<string>;
  deep?: Record<string, TRequestedReadShape>;
}
