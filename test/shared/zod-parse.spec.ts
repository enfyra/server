import { describe, it, expect } from 'vitest';
import { z } from 'zod';
import {
  formatZodErrors,
  parseOrBadRequest,
} from '../../src/shared/utils/zod-parse.util';
import { BadRequestException } from '../../src/domain/exceptions';

describe('formatZodErrors — message strings', () => {
  it('missing required field → "<field> is required"', () => {
    const schema = z.object({ email: z.string() });
    const r = schema.safeParse({});
    expect(r.success).toBe(false);
    if (!r.success) {
      expect(formatZodErrors(r.error)).toEqual(['email is required']);
    }
  });

  it('wrong type → "<field> must be a <expected>"', () => {
    const schema = z.object({ age: z.number() });
    const r = schema.safeParse({ age: 'abc' });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe('age must be a number');
    }
  });

  it('too_small string min 1 → "should not be empty"', () => {
    const schema = z.object({ name: z.string().min(1) });
    const r = schema.safeParse({ name: '' });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe('name should not be empty');
    }
  });

  it('too_small string min N → "must be at least N characters"', () => {
    const schema = z.object({ pw: z.string().min(8) });
    const r = schema.safeParse({ pw: 'abc' });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe(
        'pw must be at least 8 characters',
      );
    }
  });

  it('too_big string → "must be at most N characters"', () => {
    const schema = z.object({ s: z.string().max(3) });
    const r = schema.safeParse({ s: 'abcdef' });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe(
        's must be at most 3 characters',
      );
    }
  });

  it('email format → "must be a valid email"', () => {
    const schema = z.object({ email: z.string().email() });
    const r = schema.safeParse({ email: 'nope' });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toMatch(
        /email must be a valid email/,
      );
    }
  });

  it('too_small number min → "must be at least N"', () => {
    const schema = z.object({ n: z.number().min(10) });
    const r = schema.safeParse({ n: 5 });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe('n must be at least 10');
    }
  });

  it('enum value → "must be one of: a, b, c"', () => {
    const schema = z.object({ role: z.enum(['admin', 'user', 'guest']) });
    const r = schema.safeParse({ role: 'nope' });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe(
        'role must be one of: admin, user, guest',
      );
    }
  });

  it('strict unknown key → "<key> is not allowed"', () => {
    const schema = z.object({ a: z.string() }).strict();
    const r = schema.safeParse({ a: 'x', extra: 1 });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toContain('extra is not allowed');
    }
  });

  it('nested path → "a.b.c is required"', () => {
    const schema = z.object({
      a: z.object({ b: z.object({ c: z.string() }) }),
    });
    const r = schema.safeParse({ a: { b: {} } });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe('a.b.c is required');
    }
  });

  it('array index path → "items.0.name is required"', () => {
    const schema = z.object({ items: z.array(z.object({ name: z.string() })) });
    const r = schema.safeParse({ items: [{}] });
    if (!r.success) {
      expect(formatZodErrors(r.error)[0]).toBe('items.0.name is required');
    }
  });

  it('multiple errors → all mapped in order', () => {
    const schema = z.object({
      email: z.string().email(),
      password: z.string().min(8),
    });
    const r = schema.safeParse({ email: 'bad', password: 'x' });
    if (!r.success) {
      const msgs = formatZodErrors(r.error);
      expect(msgs).toHaveLength(2);
      expect(msgs[0]).toMatch(/email/);
      expect(msgs[1]).toMatch(/password/);
    }
  });
});

describe('parseOrBadRequest', () => {
  it('returns parsed data on success', () => {
    const schema = z.object({ x: z.number() });
    const r = parseOrBadRequest(schema, { x: 1 });
    expect(r).toEqual({ x: 1 });
  });

  it('throws BadRequestException with messages array', () => {
    const schema = z.object({ x: z.number() });
    try {
      parseOrBadRequest(schema, { x: 'bad' });
      throw new Error('should have thrown');
    } catch (e: any) {
      expect(e).toBeInstanceOf(BadRequestException);
      expect(e.statusCode).toBe(400);
      expect(e.messages).toEqual(['x must be a number']);
    }
  });

  it('thrown exception.messages is array of strings', () => {
    const schema = z.object({ a: z.string(), b: z.number() });
    try {
      parseOrBadRequest(schema, {});
    } catch (e: any) {
      expect(Array.isArray(e.messages)).toBe(true);
      expect(e.messages).toEqual(['a is required', 'b is required']);
    }
  });

  it('preserves inferred type', () => {
    const schema = z.object({ name: z.string(), age: z.number() });
    const r = parseOrBadRequest(schema, { name: 'x', age: 1 });
    // Type check: r.name is string, r.age is number (compile-time)
    expect(r.name.toUpperCase()).toBe('X');
    expect(r.age + 1).toBe(2);
  });
});
