import { Request, Response } from 'express';
import type { UploadedFileInfo } from './file-management.types';
import type { CryptoHelper } from '../helpers/crypto.helper';
import type { FetchHelper } from '../helpers/fetch.helper';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number;
  retryAfter: number;
  limit: number;
  window: number;
}

export interface RateLimitOptions {
  maxRequests: number;
  perSeconds: number;
}

export interface RateLimitHelper {
  check: (key: string, options: RateLimitOptions) => Promise<RateLimitResult>;
  byIp: (options: RateLimitOptions) => Promise<RateLimitResult>;
  byUser: (options: RateLimitOptions) => Promise<RateLimitResult>;
  byRoute: (options: RateLimitOptions) => Promise<RateLimitResult>;
  byIpGlobal: (options: RateLimitOptions) => Promise<RateLimitResult>;
  byUserGlobal: (options: RateLimitOptions) => Promise<RateLimitResult>;
  reset: (key: string) => Promise<void>;
  status: (key: string, options: RateLimitOptions) => Promise<RateLimitResult>;
}

export type EnvSnapshot = Record<string, string | undefined>;

export type DynamicRequestContext = Request & {
  rawBody?: string;
};

export interface TDynamicContext {
  $body?: any;
  $data?: any;
  $throw?: any;
  $error?: any;
  $statusCode?: number;
  $logs?: (...args: any[]) => void;
  $helpers: {
    $jwt?: (payload: any, exp: string) => string;
    $bcrypt?: {
      hash?: (plain: string) => Promise<string>;
      compare?: (p: string, h: string) => Promise<boolean>;
    };
    autoSlug?: (text: string) => string;
    $rateLimit?: RateLimitHelper;
    $fetch?: FetchHelper;
    $sleep?: (ms: number) => Promise<void>;
    $crypto?: CryptoHelper;
  };
  $storage?: {
    $upload?: (options: {
      file?: UploadedFileInfo;
      originalname?: string;
      filename?: string;
      mimetype?: string;
      buffer?: Buffer;
      size?: number;
      encoding?: string;
      folder?: number | { id: number };
      storageConfig?: number;
      title?: string;
      description?: string;
    }) => Promise<any>;
    $update?: (
      fileId: string | number,
      options: {
        file?: UploadedFileInfo;
        buffer?: Buffer;
        originalname?: string;
        filename?: string;
        mimetype?: string;
        size?: number;
        folder?: number | { id: number };
        storageConfig?: number;
        title?: string;
        description?: string;
      },
    ) => Promise<any>;
    $delete?: (fileId: string | number) => Promise<any>;
    $registerFile?: (options: {
      filename?: string;
      originalname?: string;
      mimetype: string;
      location: string;
      size?: number;
      filesize?: number;
      type?: string;
      folder?: number | string | { id: number | string };
      storageConfig: number | string | { id: number | string };
      title?: string;
      description?: string;
      verifyExists?: boolean;
    }) => Promise<any>;
  };
  $cache: {
    acquire?: (key: string, value: any, ttlMs: number) => Promise<boolean>;
    release?: (key: string, value: any) => Promise<boolean>;
    get?: (key: string) => Promise<any>;
    set?: (key: string, value: any, ttlMs: number) => Promise<void>;
    exists?: (key: string, value: any) => Promise<boolean>;
    deleteKey?: (key: string) => Promise<void>;
    setNoExpire?: (key: string, value: any) => Promise<void>;
  };
  $params?: any;
  $query?: any;
  $env?: EnvSnapshot;
  $user?: any;
  $repos: Record<string, any>;
  $req?: DynamicRequestContext;
  $res?: Response & {
    stream?: (
      stream: NodeJS.ReadableStream | ReadableStream,
      options?: {
        statusCode?: number;
        mimetype?: string;
        filename?: string;
        headers?: Record<
          string,
          string | number | readonly string[] | undefined | null
        >;
      },
    ) => void;
  };
  $share: {
    $logs: any[];
  };
  $api?: {
    request?: {
      method?: string;
      url?: string;
      timestamp?: string;
      correlationId?: string;
      userAgent?: string;
      ip?: string;
    };
    response?: {
      statusCode?: number;
      responseTime?: number;
      timestamp?: string;
    };
    error?: {
      message?: string;
      stack?: string;
      name?: string;
      timestamp?: string;
      statusCode?: number;
      details?: any;
    };
  };
  $uploadedFile?: UploadedFileInfo;
  $debug?: any;
  $socket?: {
    join?: (room: string) => void;
    leave?: (room: string) => void;
    reply?: (event: string, data: any) => void;
    emitToUser?: (userId: any, event: string, data: any) => void;
    emitToRoom?: (path: string, room: string, event: string, data: any) => void;
    emitToCurrentRoom?: (room: string, event: string, data: any) => void;
    broadcastToRoom?: (room: string, event: string, data: any) => void;
    emitToGateway?: (path: string, event: string, data: any) => void;
    broadcast?: (event: string, data: any) => void;
    roomSize?: (room: string) => Promise<number>;
    disconnect?: () => void;
  };
  $flow?: Record<string, any>;
  $trigger?: (
    flowIdOrName: string | number,
    payload?: any,
  ) => Promise<{ jobId: string; flowId: number | string }>;
}

export interface RequestWithRouteData extends Request {
  rawBody?: string;
  routeData?: {
    context: TDynamicContext;
    params: any;
    handler: string;
    handlers?: any[];
    preHooks: any[];
    postHooks: any[];
    isPublic: boolean;
    mainTable?: any;
    route?: any;
    res?: Response;
  };
  user?: any;
  file?: any; // Multer file object
}

// GraphQL Context (simpler version for GraphQL resolvers)
export type TGqlDynamicContext = {
  $repos: any;
  $args: any;
  $user: any;
  $helpers: {
    [key: string]: any;
  };
  $req: Request & {
    [key: string]: any;
  };
  $throw: {
    http: (statusCode: number, msg: string, details?: any) => never;
    notFound: (resource: string, id?: string) => never;
    duplicate: (resource: string, field: string, value: string) => never;
    '400': (msg: string) => never;
    '401': (msg?: string) => never;
    '403': (msg?: string) => never;
    '404': (msg: string, details?: any) => never;
    '409': (msg: string, details?: any) => never;
    '422': (msg: string, details?: any) => never;
    '429': (msg: string, details?: any) => never;
    '500': (msg: string, details?: any) => never;
    '503': (msg: string, details?: any) => never;
  };
  $error?: any;
  $result?: any;
  $data?: any;
  $share?: {
    $data?: any;
    [key: string]: any;
  };
  $api?: {
    request: {
      method: string;
      url: string;
      timestamp: string;
      correlationId: string;
      userAgent?: string;
      ip?: string;
    };
    response?: {
      statusCode: number;
      responseTime: number;
      timestamp: string;
    };
    error?: {
      message: string;
      stack: string;
      name: string;
      timestamp: string;
      statusCode: number;
      details: any;
    };
  };
};
