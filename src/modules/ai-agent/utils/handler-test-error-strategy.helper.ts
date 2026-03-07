export type HandlerTestErrorKind =
  | 'SYNTAX_ERROR'
  | 'REFERENCE_ERROR'
  | 'TYPE_ERROR'
  | 'SCRIPT_TIMEOUT'
  | 'BUSINESS_LOGIC'
  | 'RESOURCE_NOT_FOUND'
  | 'VALIDATION_ERROR'
  | 'DATABASE_QUERY'
  | 'HELPER_NOT_FOUND'
  | 'TABLE_NOT_FOUND'
  | 'MISSING_TABLE'
  | 'MISSING_HANDLER_CODE'
  | 'UNKNOWN';

export interface HandlerTestErrorStrategy {
  kind: HandlerTestErrorKind;
  fixGuidance: string;
  nextSteps: string[];
}

const STRATEGIES: Record<HandlerTestErrorKind, HandlerTestErrorStrategy> = {
  SYNTAX_ERROR: {
    kind: 'SYNTAX_ERROR',
    fixGuidance: 'Fix JavaScript syntax in handler code. Check: missing bracket/paren, typo in keyword, stray character, unclosed string.',
    nextSteps: [
      'Review the code at the reported line (check codeContext in response)',
      'Fix syntax error: brackets, parentheses, quotes, commas',
      'Retry run_handler_test with corrected handlerCode',
    ],
  },
  REFERENCE_ERROR: {
    kind: 'REFERENCE_ERROR',
    fixGuidance: 'Variable or property is undefined. Common: wrong #table_name (must match run_handler_test table param), @BODY property typo, or typo in #identifier.',
    nextSteps: [
      'Verify #<table_name> matches the table param (e.g. table:"products" → use #products)',
      'Check @BODY property names match body param keys',
      'Ensure no typos in @PARAMS, @QUERY, @USER',
      'Retry with corrected handlerCode',
    ],
  },
  TYPE_ERROR: {
    kind: 'TYPE_ERROR',
    fixGuidance: 'Wrong type used. Common: calling method on null/undefined, .find()/.create()/.update()/.delete() with wrong args, or res?.data when res is not object.',
    nextSteps: [
      'Check .find() expects { filter?, limit?, fields? }. Returns { data: [] }',
      'Check .create({ data: {...} }) and .update({ id, data: {...} })',
      'Check .delete({ id })',
      'Add null checks: res?.data?.length before accessing',
      'Retry with corrected handlerCode',
    ],
  },
  SCRIPT_TIMEOUT: {
    kind: 'SCRIPT_TIMEOUT',
    fixGuidance: 'Handler ran too long. Simplify logic, reduce limit, or increase timeoutMs.',
    nextSteps: [
      'Reduce limit in .find() (e.g. limit: 5 instead of 0)',
      'Remove unnecessary loops or async calls',
      'Pass timeoutMs (e.g. 30000) if handler needs more time',
      'Retry run_handler_test',
    ],
  },
  BUSINESS_LOGIC: {
    kind: 'BUSINESS_LOGIC',
    fixGuidance: 'Handler threw via @THROW400/404/etc. This is expected when validation fails. Fix the condition or the test body/params.',
    nextSteps: [
      'Review the throw message – it indicates what went wrong',
      'Adjust body/params to satisfy validation, or fix handler logic',
      'Retry run_handler_test with corrected body/params or handlerCode',
    ],
  },
  RESOURCE_NOT_FOUND: {
    kind: 'RESOURCE_NOT_FOUND',
    fixGuidance: 'Record or resource not found. Ensure test data exists or create it first.',
    nextSteps: [
      'For update/delete: create_records first, then test on new record',
      'Verify where/filter matches existing data',
      'Check @PARAMS.id exists when testing update/delete',
      'Retry with correct body/params or create test record first',
    ],
  },
  VALIDATION_ERROR: {
    kind: 'VALIDATION_ERROR',
    fixGuidance: 'Invalid data (e.g. required field missing, wrong type). Check schema and payload.',
    nextSteps: [
      'get_table_details to verify required fields and types',
      'Ensure body/params include all required fields',
      'Use correct relation format: { propertyName: { id: value } }',
      'Retry with valid body/params',
    ],
  },
  DATABASE_QUERY: {
    kind: 'DATABASE_QUERY',
    fixGuidance: 'Query/filter structure is invalid. Check where clause operators and field names.',
    nextSteps: [
      'Use operators: _eq, _neq, _gt, _gte, _lt, _lte, _contains, _in, _and, _or',
      'Verify field names with get_table_details',
      'Format: { field: { _eq: value } } not { field: value }',
      'Retry with corrected where clause',
    ],
  },
  HELPER_NOT_FOUND: {
    kind: 'HELPER_NOT_FOUND',
    fixGuidance: 'Use @HELPERS.$bcrypt not $bcrypt. Other helpers may not be available in test.',
    nextSteps: [
      'Use @HELPERS.$bcrypt.hash(plain) and @HELPERS.$bcrypt.compare(plain, hash)',
      'Avoid $bcrypt, $jwt directly – use @HELPERS',
      'Retry with corrected handlerCode',
    ],
  },
  TABLE_NOT_FOUND: {
    kind: 'TABLE_NOT_FOUND',
    fixGuidance: 'Table does not exist. Verify table name with find_records on table_definition.',
    nextSteps: [
      'find_records({"table":"table_definition","fields":"name","limit":0}) to list tables',
      'Use exact table name (snake_case, case-sensitive)',
      'Retry run_handler_test with correct table name',
    ],
  },
  MISSING_TABLE: {
    kind: 'MISSING_TABLE',
    fixGuidance: 'Table param is required for run_handler_test.',
    nextSteps: ['Pass table param with the target table name', 'Retry run_handler_test with table'],
  },
  MISSING_HANDLER_CODE: {
    kind: 'MISSING_HANDLER_CODE',
    fixGuidance: 'handlerCode param is required.',
    nextSteps: ['Pass handlerCode with the handler logic', 'Retry run_handler_test with handlerCode'],
  },
  UNKNOWN: {
    kind: 'UNKNOWN',
    fixGuidance: 'Unexpected error. Review error message and code context, fix and retry.',
    nextSteps: [
      'Read the error message and codeContext (if present)',
      'Check handler follows: return await #<table>.find/create/update/delete(...)',
      'Ensure proper async/await and return',
      'Retry run_handler_test with fixes',
    ],
  },
};

export function getHandlerTestErrorStrategy(error: any): HandlerTestErrorStrategy {
  const msg = (error?.message ?? String(error)).toLowerCase();
  const errName = (error?.constructor?.name ?? error?.name ?? '').toLowerCase();
  const errCode = (error?.errorCode ?? error?.code ?? '').toLowerCase();
  const details = error?.details ?? error?.response ?? {};

  if (errCode === 'missing_table' || msg.includes('table parameter is required')) {
    return STRATEGIES.MISSING_TABLE;
  }
  if (errCode === 'missing_handler_code' || msg.includes('handlercode parameter is required')) {
    return STRATEGIES.MISSING_HANDLER_CODE;
  }
  if (
    errName === 'syntacterror' ||
    details?.type === 'SyntaxError' ||
    msg.includes('syntaxerror') ||
    msg.includes('unexpected token') ||
    msg.includes('unexpected end')
  ) {
    return STRATEGIES.SYNTAX_ERROR;
  }
  if (
    errName === 'referenceerror' ||
    details?.type === 'ReferenceError' ||
    msg.includes('referenceerror') ||
    msg.includes('is not defined') ||
    msg.includes('cannot access')
  ) {
    return STRATEGIES.REFERENCE_ERROR;
  }
  if (
    errName === 'typeerror' ||
    details?.type === 'TypeError' ||
    msg.includes('typeerror') ||
    msg.includes('is not a function') ||
    msg.includes('cannot read propert')
  ) {
    return STRATEGIES.TYPE_ERROR;
  }
  if (
    errName === 'scripttimeoutexception' ||
    errCode === 'script_timeout' ||
    msg.includes('timed out') ||
    msg.includes('timeout')
  ) {
    return STRATEGIES.SCRIPT_TIMEOUT;
  }
  if (
    errCode === 'business_logic_error' ||
    msg.includes('bad request') ||
    (details?.statusCode === 400 && msg.includes('script'))
  ) {
    return STRATEGIES.BUSINESS_LOGIC;
  }
  if (
    errCode === 'resource_not_found' ||
    msg.includes('not found') ||
    msg.includes('does not exist')
  ) {
    return STRATEGIES.RESOURCE_NOT_FOUND;
  }
  if (
    errCode === 'validation_error' ||
    msg.includes('validation') ||
    msg.includes('unprocessable')
  ) {
    return STRATEGIES.VALIDATION_ERROR;
  }
  if (
    errCode === 'database_query_error' ||
    msg.includes('database') ||
    msg.includes('query')
  ) {
    return STRATEGIES.DATABASE_QUERY;
  }
  if (msg.includes('helper') && (msg.includes('not found') || msg.includes('undefined'))) {
    return STRATEGIES.HELPER_NOT_FOUND;
  }
  if (
    msg.includes('table') &&
    (msg.includes('not found') || msg.includes('does not exist') || msg.includes('lookup'))
  ) {
    return STRATEGIES.TABLE_NOT_FOUND;
  }

  return STRATEGIES.UNKNOWN;
}

export function formatHandlerTestErrorResponse(
  error: any,
  logs?: any[],
): {
  success: false;
  error: string;
  errorCode: string;
  errorKind: HandlerTestErrorKind;
  fixGuidance: string;
  nextSteps: string[];
  location?: { line: number };
  codeContext?: string[];
  logs?: any[];
} {
  const strategy = getHandlerTestErrorStrategy(error);
  const details = error?.details ?? error?.response ?? {};
  return {
    success: false,
    error: error?.message ?? String(error),
    errorCode: error?.errorCode ?? error?.constructor?.name ?? 'SCRIPT_ERROR',
    errorKind: strategy.kind,
    fixGuidance: strategy.fixGuidance,
    nextSteps: strategy.nextSteps,
    location: details?.location,
    codeContext: details?.code,
    logs: logs?.length ? logs : undefined,
  };
}
