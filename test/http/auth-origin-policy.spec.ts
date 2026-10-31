import { describe, expect, it, vi } from 'vitest';
import { createOAuthRedirectValidator } from '../../src/domain/auth/utils/oauth-redirect.util';

const appOrigin = 'https://admin.example.test';
const authOrigin = 'https://console.example.test';

describe('Auth origin authority is independent of API CORS', () => {
  it('reads only registered auth origins and never grants a CORS-only origin', async () => {
    const find = vi.fn(async ({ table }: { table: string }) => ({
      data: table === 'enfyra_auth_origin'
        ? [{ value: authOrigin, isEnabled: true }]
        : [{ value: 'https://cors-only.example.test', isEnabled: true }],
    }));
    const validate = await createOAuthRedirectValidator({ find } as never, [appOrigin]);
    expect(validate(`${authOrigin}/login`)).toBe(`${authOrigin}/login`);
    expect(validate(`${appOrigin}/settings`)).toBe(`${appOrigin}/settings`);
    expect(() => validate('https://cors-only.example.test/return')).toThrow('Redirect origin is not allowed');
    expect(find).toHaveBeenCalledWith({ table: 'enfyra_auth_origin', fields: ['value', 'isEnabled'], limit: 0 });
  });

  it.each([
    { data: [] },
    { data: [{ value: authOrigin, isEnabled: false }] },
    { data: [{ value: '*', isEnabled: true }] },
    { data: [{ value: 'null', isEnabled: true }] },
    { data: [{ value: 'https://user:pass@console.example.test', isEnabled: true }] },
    { data: [{ value: authOrigin + '/path', isEnabled: true }] },
  ])('does not grant invalid, disabled or absent external origins: %j', async ({ data }) => {
    const validate = await createOAuthRedirectValidator({ find: vi.fn(async () => ({ data })) } as never, [appOrigin]);
    expect(() => validate(`${authOrigin}/login`)).toThrow();
    expect(validate(`${appOrigin}/login`)).toBe(`${appOrigin}/login`);
  });

  it('does not retain a partial grant from malformed auth metadata', async () => {
    const validate = await createOAuthRedirectValidator({ find: vi.fn(async () => ({ data: [{ value: authOrigin }, { value: '*' }] })) } as never, [appOrigin]);
    expect(() => validate(`${authOrigin}/login`)).toThrow();
  });

  it('fails closed for external destinations when auth metadata is unavailable', async () => {
    const validate = await createOAuthRedirectValidator({ find: vi.fn(async () => { throw new Error('unavailable'); }) } as never, [appOrigin]);
    expect(() => validate(`${authOrigin}/return`)).toThrow();
    expect(validate(`${appOrigin}/login`)).toBe(`${appOrigin}/login`);
  });
});
