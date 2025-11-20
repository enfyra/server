import { Request, Response } from 'express';
import { UploadedFileInfo } from './file-management.interface';

export interface TDynamicContext {
  $body?: any;
  $data?: any;
  $statusCode?: number;
  $throw?: any;
  $error?: any;
  $logs?: (...args: any[]) => void;
  $helpers?: {
    $jwt?: (payload: any, exp: string) => string;
    $bcrypt?: {
      hash?: (plain: string) => Promise<string>;
      compare?: (p: string, h: string) => Promise<boolean>;
    };
    autoSlug?: (text: string) => string;
    $uploadFile?: (options: {
      originalname?: string;
      filename?: string;
      mimetype: string;
      buffer: Buffer;
      size: number;
      encoding?: string;
      folder?: number | { id: number };
      storageConfig?: number;
      title?: string;
      description?: string;
    }) => Promise<any>;
    $updateFile?: (fileId: string | number, options: {
      buffer?: Buffer;
      originalname?: string;
      filename?: string;
      mimetype?: string;
      size?: number;
      folder?: number | { id: number };
      storageConfig?: number;
      title?: string;
      description?: string;
    }) => Promise<any>;
    $deleteFile?: (fileId: string | number) => Promise<any>;
  };
  $cache?: {
    acquire?: (key: string, value: any, ttlMs: number) => Promise<boolean>;
    release?: (key: string, value: any) => Promise<boolean>;
    get?: (key: string) => Promise<any>;
    set?: (key: string, value: any, ttlMs?: number) => Promise<void>;
    exists?: (key: string, value: any) => Promise<boolean>;
    deleteKey?: (key: string) => Promise<void>;
    setNoExpire?: (key: string, value: any) => Promise<void>;
  };
  $params?: any;
  $query?: any;
  $user?: any;
  $repos?: Record<string, any>;
  $req?: Request;
  $res?: Response;
  $share?: {
    $logs?: any[];
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
}

export interface RequestWithRouteData extends Request {
  routeData?: {
    context: TDynamicContext;
    params: any;
    handler: string;
    handlers?: any[];
    hooks: any[];
    isPublished: boolean;
    mainTable?: any;
    targetTables?: any[];
    route?: any;
    res?: Response;
  };
  user?: any;
  file?: any; // Multer file object
}