import { RawScriptErrorCarrier } from '@enfyra/kernel';
import { HttpException } from '../../domain/exceptions';

export class ScriptErrorFactory {
  static createThrowHandlers() {
    return {
      http: (statusCode: number, message?: string) => {
        throw new HttpException(message || 'Error', statusCode);
      },
      json: (
        body: unknown,
        options: {
          statusCode?: number;
          headers?: Record<
            string,
            string | number | readonly string[] | undefined | null
          >;
        } = {},
      ) => {
        const statusCode = options.statusCode ?? 500;
        if (!Number.isInteger(statusCode) || statusCode < 400 || statusCode > 599) {
          throw new TypeError(
            '$throw.json statusCode must be an integer HTTP status from 400 to 599',
          );
        }
        const errorJsonText = JSON.stringify(body);
        if (errorJsonText === undefined) {
          throw new TypeError('$throw.json requires a JSON-serializable value');
        }
        throw new RawScriptErrorCarrier(
          'Custom JSON error response',
          statusCode,
          `HTTP_${statusCode}`,
          '$throw.json',
          {
            errorJsonText,
            errorJsonOptions: { ...options, statusCode },
          },
        );
      },
    };
  }

  static createErrorBuilders() {
    return {
      build: (code: string, message: string, details?: any) => ({
        code,
        message,
        details,
        isError: true,
      }),
      isError: (obj: any) => obj?.isError === true,
    };
  }
}
