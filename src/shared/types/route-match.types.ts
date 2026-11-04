export interface RouteMatchResult {
  route: any;
  params: Record<string, string>;
}

export interface RouteMatchIndexEntry {
  key?: string;
  path: string;
  methods: string[];
  order?: number;
}
