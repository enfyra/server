import { describe, it, expect } from 'vitest';
import {
  loginSchema,
  refreshTokenSchema,
  logoutSchema,
} from '../../src/core/auth/schemas/auth.schemas';
import { parseOrBadRequest } from '../../src/shared/utils/zod-parse.util';

describe('loginSchema', () => {
  it('happy path', () => {
    const body = parseOrBadRequest(loginSchema, {
      email: 'a@b.com',
      password: 'pw',
    });
    expect(body).toEqual({ email: 'a@b.com', password: 'pw' });
  });

  it('remember optional boolean', () => {
    const body = parseOrBadRequest(loginSchema, {
      email: 'a@b.com',
      password: 'pw',
      remember: true,
    });
    expect(body.remember).toBe(true);
  });

  it('missing email → "email is required"', () => {
    expect(() => parseOrBadRequest(loginSchema, { password: 'pw' })).toThrow(
      /email is required/,
    );
  });

  it('missing password → "password is required"', () => {
    expect(() => parseOrBadRequest(loginSchema, { email: 'a@b.com' })).toThrow(
      /password is required/,
    );
  });

  it('invalid email → "valid email"', () => {
    expect(() =>
      parseOrBadRequest(loginSchema, { email: 'not-email', password: 'pw' }),
    ).toThrow(/email/);
  });

  it('password empty string → "should not be empty"', () => {
    expect(() =>
      parseOrBadRequest(loginSchema, { email: 'a@b.com', password: '' }),
    ).toThrow(/password should not be empty/);
  });

  it('extra key rejected in strict mode', () => {
    expect(() =>
      parseOrBadRequest(loginSchema, {
        email: 'a@b.com',
        password: 'pw',
        admin: true,
      }),
    ).toThrow(/admin is not allowed/);
  });

  it('remember wrong type', () => {
    expect(() =>
      parseOrBadRequest(loginSchema, {
        email: 'a@b.com',
        password: 'pw',
        remember: 'yes',
      }),
    ).toThrow(/remember must be a boolean/);
  });
});

describe('refreshTokenSchema / logoutSchema', () => {
  it('happy path', () => {
    const body = parseOrBadRequest(refreshTokenSchema, { refreshToken: 't' });
    expect(body).toEqual({ refreshToken: 't' });
  });

  it('missing → required', () => {
    expect(() => parseOrBadRequest(refreshTokenSchema, {})).toThrow(
      /refreshToken is required/,
    );
  });

  it('wrong type', () => {
    expect(() =>
      parseOrBadRequest(refreshTokenSchema, { refreshToken: 123 }),
    ).toThrow(/refreshToken must be a string/);
  });

  it('empty string', () => {
    expect(() =>
      parseOrBadRequest(refreshTokenSchema, { refreshToken: '' }),
    ).toThrow(/refreshToken should not be empty/);
  });

  it('logoutSchema has same shape as refreshTokenSchema', () => {
    expect(logoutSchema).toBe(refreshTokenSchema);
  });
});
