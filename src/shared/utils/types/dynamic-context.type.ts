import { Request } from 'express';

export type TDynamicContext = {
  $repos: any;
  $body: any;
  $query: any;
  $params: any;
  $user: any;
  $logs: (...args: any[]) => void;
  $helpers: {
    [key: string]: any;
  };
  $req: Request & {
    [key: string]: any;
  };
  $throw: {};
  $error?: any;
  $result?: any;
  $data?: any;
  $statusCode?: number;
  $uploadedFile?: {
    originalname: string;
    mimetype: string;
    buffer: Buffer;
    size: number;
    fieldname: string;
  };
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
    '400': (msg: string) => never;
    '401': (msg?: string) => never;
    '403': (msg?: string) => never;
    '404': (resource: string, id?: string) => never;
    '409': (resource: string, field: string, value: string) => never;
    '422': (msg: string, details?: any) => never;
    '429': (limit: number, window: string) => never;
    '500': (msg: string, details?: any) => never;
    '503': (service: string) => never;
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
