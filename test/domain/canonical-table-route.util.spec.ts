import {
  isCanonicalTableRoutePath,
  DEFAULT_REST_HANDLER_LOGIC,
} from '../../src/domain/bootstrap';

describe('isCanonicalTableRoutePath', () => {
  it('accepts only single-segment path equal to table name', () => {
    expect(
      isCanonicalTableRoutePath('/enfyra_menu', 'enfyra_menu'),
    ).toBe(true);
    expect(
      isCanonicalTableRoutePath('enfyra_menu', 'enfyra_menu'),
    ).toBe(true);
  });

  it('rejects auth and profile-style paths even when mainTable differs', () => {
    expect(isCanonicalTableRoutePath('/auth/login', 'enfyra_user')).toBe(
      false,
    );
    expect(isCanonicalTableRoutePath('/me', 'enfyra_user')).toBe(false);
    expect(
      isCanonicalTableRoutePath(
        '/me/oauth-accounts',
        'enfyra_oauth_account',
      ),
    ).toBe(false);
  });

  it('rejects nested resource paths', () => {
    expect(
      isCanonicalTableRoutePath(
        '/enfyra_extension/preview',
        'enfyra_extension',
      ),
    ).toBe(false);
    expect(
      isCanonicalTableRoutePath('/enfyra_folder/tree', 'enfyra_folder'),
    ).toBe(false);
  });

  it('rejects missing or mismatched names', () => {
    expect(isCanonicalTableRoutePath('/post', 'comment')).toBe(false);
    expect(isCanonicalTableRoutePath(undefined, 'post')).toBe(false);
    expect(isCanonicalTableRoutePath('/post', undefined)).toBe(false);
  });
});

describe('default REST handler map', () => {
  it('defines executable logic for every built-in default handler', () => {
    for (const [method, logic] of Object.entries(DEFAULT_REST_HANDLER_LOGIC)) {
      expect(method.length).toBeGreaterThan(0);
      expect(typeof logic).toBe('string');
      expect(logic.length).toBeGreaterThan(10);
    }
  });
});
