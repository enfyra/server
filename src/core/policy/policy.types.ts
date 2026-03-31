export type TPolicyDeny = {
  allow: false;
  statusCode: 401 | 403 | 409 | 422;
  code: string;
  message: string;
  details?: any;
};

export type TPolicyAllow = {
  allow: true;
  details?: any;
};

export type TPolicyDecision = TPolicyAllow | TPolicyDeny;

export function isPolicyDeny(decision: TPolicyDecision): decision is TPolicyDeny {
  return decision.allow === false;
}

export type TSchemaOperation = 'create' | 'update' | 'delete';

export type TPolicyRequestContext = {
  method: string;
  routeData?: any;
  user?: any;
};

export type TPolicyMutationContext = {
  operation: TSchemaOperation;
  tableName: string;
  data: any;
  existing?: any;
  currentUser?: any;
};

export type TPolicySchemaMigrationContext = {
  operation: TSchemaOperation;
  tableName: string;
  data?: any;
  existing?: any;
  currentUser?: any;
  beforeMetadata?: any;
  afterMetadata?: any;
  requestContext?: any;
};
