import { Logger } from '@nestjs/common';

const logger = new Logger('ErrorRecovery');

export type ErrorType =
  | 'TIMEOUT'
  | 'RATE_LIMIT'
  | 'PERMISSION_DENIED'
  | 'RESOURCE_NOT_FOUND'
  | 'RESOURCE_EXISTS'
  | 'INVALID_INPUT'
  | 'NETWORK_ERROR'
  | 'SERVER_ERROR'
  | 'UNKNOWN_ERROR';

export type RecoveryAction =
  | 'retry'
  | 'ask_user'
  | 'escalate_to_user'
  | 'ask_clarification'
  | 'wait_and_retry'
  | 'fail';

export interface RecoveryStrategy {
  maxRetries: number;
  backoffMs?: number[];
  fallback: RecoveryAction;
  message: string;
  suggestion?: string;
}

export interface RecoveryResult {
  action: RecoveryAction;
  maxRetries: number;
  backoffMs?: number[];
  message: string;
  suggestion?: string;
  errorType: ErrorType;
}

/**
 * Error recovery strategy matrix
 * Maps error types to recovery strategies following best practices from Anthropic & OpenAI
 */
const RECOVERY_STRATEGIES: Record<ErrorType, RecoveryStrategy> = {
  TIMEOUT: {
    maxRetries: 3,
    backoffMs: [1000, 2000, 4000],
    fallback: 'retry',
    message: 'Operation timed out. Retrying with exponential backoff...',
  },
  RATE_LIMIT: {
    maxRetries: 2,
    backoffMs: [5000, 15000],
    fallback: 'wait_and_retry',
    message: 'Rate limit reached. Waiting before retry...',
    suggestion: 'Consider reducing request frequency or upgrading API limits.',
  },
  PERMISSION_DENIED: {
    maxRetries: 0,
    fallback: 'escalate_to_user',
    message: 'Permission denied. Please check your access rights.',
    suggestion:
      'You may need to: 1) Request permission from an administrator, 2) Verify you are logged in with the correct account, 3) Check if the resource has specific access restrictions.',
  },
  RESOURCE_NOT_FOUND: {
    maxRetries: 1,
    fallback: 'ask_user',
    message: 'Resource not found.',
    suggestion:
      'Would you like me to: 1) Search for similar resources, 2) Create a new resource, 3) Check if the resource name is correct?',
  },
  RESOURCE_EXISTS: {
    maxRetries: 0,
    fallback: 'ask_user',
    message: 'Resource already exists.',
    suggestion:
      'Would you like me to: 1) Update the existing resource instead, 2) Use a different name, 3) Delete and recreate the resource?',
  },
  INVALID_INPUT: {
    maxRetries: 0,
    fallback: 'ask_clarification',
    message: 'Invalid input provided.',
    suggestion:
      'Please provide correct parameters. I can help clarify what information is needed.',
  },
  NETWORK_ERROR: {
    maxRetries: 3,
    backoffMs: [1000, 2000, 4000],
    fallback: 'retry',
    message: 'Network error occurred. Retrying...',
  },
  SERVER_ERROR: {
    maxRetries: 3,
    backoffMs: [2000, 5000, 10000],
    fallback: 'retry',
    message: 'Server error occurred. Retrying...',
  },
  UNKNOWN_ERROR: {
    maxRetries: 1,
    backoffMs: [2000],
    fallback: 'fail',
    message: 'An unexpected error occurred.',
    suggestion: 'Please review the error details and try again.',
  },
};

/**
 * Classifies an error into a specific error type
 */
export function classifyError(error: any): ErrorType {
  const message = error?.message?.toLowerCase() || '';
  const status = error?.status || error?.statusCode || error?.response?.status;
  const errorCode = error?.code || error?.errorCode;

  // Permission errors
  if (
    message.includes('permission denied') ||
    message.includes('unauthorized') ||
    message.includes('forbidden') ||
    message.includes('access denied') ||
    status === 401 ||
    status === 403
  ) {
    return 'PERMISSION_DENIED';
  }

  // Resource not found
  if (
    message.includes('not found') ||
    message.includes('does not exist') ||
    message.includes('no such') ||
    status === 404
  ) {
    return 'RESOURCE_NOT_FOUND';
  }

  // Resource already exists
  if (
    message.includes('already exists') ||
    message.includes('duplicate') ||
    message.includes('conflict') ||
    status === 409
  ) {
    return 'RESOURCE_EXISTS';
  }

  // Invalid input
  if (
    message.includes('invalid') ||
    message.includes('malformed') ||
    message.includes('bad request') ||
    message.includes('validation') ||
    status === 400
  ) {
    return 'INVALID_INPUT';
  }

  // Rate limit
  if (
    status === 429 ||
    message.includes('rate limit') ||
    message.includes('too many requests') ||
    errorCode === 'rate_limit_exceeded'
  ) {
    return 'RATE_LIMIT';
  }

  // Timeout
  if (
    message.includes('timeout') ||
    message.includes('timed out') ||
    errorCode === 'ETIMEDOUT' ||
    status === 504
  ) {
    return 'TIMEOUT';
  }

  // Network errors
  if (
    message.includes('network') ||
    message.includes('econnrefused') ||
    message.includes('enotfound') ||
    message.includes('enetunreach') ||
    errorCode === 'ECONNRESET' ||
    errorCode === 'ECONNREFUSED' ||
    errorCode === 'ENOTFOUND'
  ) {
    return 'NETWORK_ERROR';
  }

  // Server errors
  if (
    status >= 500 ||
    message.includes('server error') ||
    message.includes('service unavailable') ||
    message.includes('gateway')
  ) {
    return 'SERVER_ERROR';
  }

  return 'UNKNOWN_ERROR';
}

/**
 * Gets the recovery strategy for a given error
 */
export function getRecoveryStrategy(error: any): RecoveryResult {
  const errorType = classifyError(error);
  const strategy = RECOVERY_STRATEGIES[errorType];

  logger.debug(`Classified error as ${errorType}: ${error.message}`);

  return {
    action: strategy.fallback,
    maxRetries: strategy.maxRetries,
    backoffMs: strategy.backoffMs,
    message: strategy.message,
    suggestion: strategy.suggestion,
    errorType,
  };
}

/**
 * Formats an error message for the user with recovery suggestions
 */
export function formatErrorForUser(error: any): string {
  const recovery = getRecoveryStrategy(error);
  const errorMessage = error?.message || String(error);

  let message = `❌ **Error:** ${errorMessage}\n\n`;
  message += `📋 **Status:** ${recovery.message}\n`;

  if (recovery.suggestion) {
    message += `\n💡 **Suggestions:**\n${recovery.suggestion}\n`;
  }

  return message;
}

/**
 * Determines if an operation should be escalated to human
 */
export interface EscalationTrigger {
  shouldEscalate: boolean;
  reason?: string;
  context?: string;
  suggestedActions?: string[];
}

export function shouldEscalateToHuman(params: {
  operation: string;
  table?: string;
  error?: any;
  retryCount?: number;
  confidenceLevel?: number;
}): EscalationTrigger {
  const { operation, table, error, retryCount = 0, confidenceLevel = 1.0 } = params;

  // High-stakes operations requiring confirmation
  if (operation === 'delete' && table?.includes('_definition')) {
    return {
      shouldEscalate: true,
      reason: 'high_stakes_operation',
      context: `Attempting to delete system definition: ${table}`,
      suggestedActions: [
        'Backup data before proceeding',
        'Verify this is the correct resource',
        'Consider soft-delete alternative',
      ],
    };
  }

  // Repeated failures
  if (retryCount >= 2 && error) {
    const errorType = classifyError(error);
    return {
      shouldEscalate: true,
      reason: 'repeated_failure',
      context: `Operation failed ${retryCount + 1} times (${errorType})`,
      suggestedActions: [
        'Check if the operation parameters are correct',
        'Verify system configuration and permissions',
        'Review error details for specific issues',
      ],
    };
  }

  // Low confidence operations
  if (confidenceLevel < 0.5) {
    return {
      shouldEscalate: true,
      reason: 'low_confidence',
      context: 'AI agent has low confidence in this operation',
      suggestedActions: [
        'Review the operation parameters',
        'Provide more specific instructions',
        'Manually perform critical steps',
      ],
    };
  }

  // Security-sensitive operations
  if (
    operation === 'update' &&
    (table === 'user_definition' ||
      table === 'role_definition' ||
      table === 'route_definition')
  ) {
    return {
      shouldEscalate: true,
      reason: 'security_concern',
      context: `Modifying security-sensitive table: ${table}`,
      suggestedActions: [
        'Verify the changes are authorized',
        'Review affected permissions and access',
        'Test changes in a safe environment first',
      ],
    };
  }

  return {
    shouldEscalate: false,
  };
}

/**
 * Formats escalation message for user
 */
export function formatEscalationMessage(trigger: EscalationTrigger): string {
  if (!trigger.shouldEscalate) return '';

  let message = `⚠️ **Human Confirmation Required**\n\n`;
  message += `📌 **Reason:** ${trigger.reason}\n`;

  if (trigger.context) {
    message += `📝 **Context:** ${trigger.context}\n`;
  }

  if (trigger.suggestedActions && trigger.suggestedActions.length > 0) {
    message += `\n💡 **Suggested Actions:**\n`;
    trigger.suggestedActions.forEach((action, i) => {
      message += `${i + 1}. ${action}\n`;
    });
  }

  message += `\n**Please confirm if you want to proceed with this operation.**`;

  return message;
}
