export interface ConfigService {
  get(key: string, defaultValue?: unknown): unknown;
  getOrThrow(key: string): unknown;
}
