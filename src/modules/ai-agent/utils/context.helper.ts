import { TDynamicContext } from '../../../shared/interfaces/dynamic-context.interface';

export function createLLMContext(user?: any, conversationId?: string | number): TDynamicContext {
  return {
    $body: {},
    $data: undefined,
    $statusCode: undefined,
    $throw: {
      badRequest: (message: string) => {
        throw new Error(message);
      },
      unauthorized: (message: string) => {
        throw new Error(message);
      },
      forbidden: (message: string) => {
        throw new Error(message);
      },
      notFound: (message: string) => {
        throw new Error(message);
      },
      internalServerError: (message: string) => {
        throw new Error(message);
      },
    },
    $logs: (...args: any[]) => {},
    $helpers: {},
    $cache: undefined,
    $params: { conversationId },
    $query: {},
    $user: user || null,
    $repos: {},
    $req: {} as any,
    $share: {
      $logs: [],
    },
    $api: {
      request: {
        method: 'POST',
        url: '/ai-agent',
        timestamp: new Date().toISOString(),
        correlationId: '',
        userAgent: 'ai-agent',
        ip: '127.0.0.1',
      },
    },
  };
}

