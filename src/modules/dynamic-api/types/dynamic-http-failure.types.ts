export type DynamicHttpFailureOrigin = 'client' | 'stream' | 'execution';

export interface DynamicHttpFailure {
  origin: DynamicHttpFailureOrigin;
  statusCode: number;
  code: string;
  message: string;
}
