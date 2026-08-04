import { describe, expect, it, vi } from 'vitest';
import { GraphQLError } from 'graphql';
import { throwGqlError } from '../../src/modules/graphql/utils/throw-error';

describe('throwGqlError extensions contract', () => {
  it('legacy 3-arg form (string detail) is ignored — only structured options are supported', () => {
    expect(() => throwGqlError('400', 'Missing table name')).toThrowError(
      GraphQLError,
    );
    try {
      throwGqlError('400', 'Missing table name', 'some-detail' as any);
    } catch (e: any) {
      expect(e).toBeInstanceOf(GraphQLError);
      expect(e.message).toBe('Missing table name');
      expect(e.extensions.code).toBe('400');
      expect(e.extensions.detail).toBeUndefined();
      expect(e.extensions.details).toBeUndefined();
      expect(e.extensions.statusCode).toBeUndefined();
    }
  });

  it('legacy 3-arg form (object detail) is ignored — only structured options are supported', () => {
    try {
      throwGqlError('SCRIPT_ERROR', 'boom', { foo: 1 } as any);
    } catch (e: any) {
      expect(e.extensions.code).toBe('SCRIPT_ERROR');
      expect(e.extensions.detail).toBeUndefined();
      expect(e.extensions.details).toBeUndefined();
      expect(e.extensions.statusCode).toBeUndefined();
    }
  });

  it('structured options form carries statusCode + details in extensions', () => {
    const details = {
      reason: 'rate_limit',
      scope: 'ip',
      limit: 100,
      remaining: 0,
      windowSeconds: 60,
      retryAfterSeconds: 27,
      resetAt: 1785690473123,
    };
    try {
      throwGqlError('RATE_LIMIT_EXCEEDED', 'Too Many Requests', {
        statusCode: 429,
        details,
      });
    } catch (e: any) {
      expect(e).toBeInstanceOf(GraphQLError);
      expect(e.message).toBe('Too Many Requests');
      expect(e.extensions.code).toBe('RATE_LIMIT_EXCEEDED');
      expect(e.extensions.statusCode).toBe(429);
      expect(e.extensions.details).toEqual(details);
      expect(e.extensions.detail).toBeUndefined();
    }
  });

  it('IP_NOT_ALLOWED structured form preserves only structured fields', () => {
    try {
      throwGqlError('IP_NOT_ALLOWED', 'Forbidden', {
        statusCode: 403,
        details: { reason: 'ip_not_allowed' },
      });
    } catch (e: any) {
      expect(e.extensions.code).toBe('IP_NOT_ALLOWED');
      expect(e.extensions.statusCode).toBe(403);
      expect(e.extensions.details).toEqual({
        reason: 'ip_not_allowed',
      });
    }
  });

  it('does NOT leak guardName or Redis key when passed via options', () => {
    try {
      throwGqlError('RATE_LIMIT_EXCEEDED', 'Too Many Requests', {
        statusCode: 429,
        details: {
          reason: 'rate_limit',
          scope: 'ip',
          limit: 100,
          remaining: 0,
          windowSeconds: 60,
          retryAfterSeconds: 27,
          resetAt: 1,
        },
      });
    } catch (e: any) {
      const json = JSON.stringify(e.extensions);
      expect(json).not.toContain('guard_rule');
      expect(json).not.toContain('guardName');
      expect(json).not.toContain('ruleId');
    }
  });

  it('omits statusCode + details keys entirely when not provided', () => {
    try {
      throwGqlError('AUTH_ERROR', 'Unauthorized');
    } catch (e: any) {
      expect(e.extensions).toEqual({ code: 'AUTH_ERROR' });
      expect('statusCode' in e.extensions).toBe(false);
      expect('details' in e.extensions).toBe(false);
    }
  });
});