export interface CrossBoundaryCustomException {
  message: string;
  statusCode: number;
  errorCode: string;
  details?: unknown;
  messages?: string[];
}
