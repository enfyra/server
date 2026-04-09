import {
  isCanonicalTableRoutePath,
  DEFAULT_REST_HANDLER_LOGIC,
  REST_HANDLER_METHOD_NAMES,
} from '../../src/core/bootstrap/utils/canonical-table-route.util';

describe('isCanonicalTableRoutePath', () => {
  it('accepts only single-segment path equal to table name', () => {
    expect(
      isCanonicalTableRoutePath('/menu_definition', 'menu_definition'),
    ).toBe(true);
    expect(
      isCanonicalTableRoutePath('menu_definition', 'menu_definition'),
    ).toBe(true);
  });

  it('rejects auth and profile-style paths even when mainTable differs', () => {
    expect(isCanonicalTableRoutePath('/auth/login', 'user_definition')).toBe(
      false,
    );
    expect(isCanonicalTableRoutePath('/me', 'user_definition')).toBe(false);
    expect(
      isCanonicalTableRoutePath(
        '/me/oauth-accounts',
        'oauth_account_definition',
      ),
    ).toBe(false);
  });

  it('rejects nested resource paths', () => {
    expect(
      isCanonicalTableRoutePath(
        '/extension_definition/preview',
        'extension_definition',
      ),
    ).toBe(false);
    expect(
      isCanonicalTableRoutePath('/folder_definition/tree', 'folder_definition'),
    ).toBe(false);
  });

  it('rejects missing or mismatched names', () => {
    expect(isCanonicalTableRoutePath('/post', 'comment')).toBe(false);
    expect(isCanonicalTableRoutePath(undefined, 'post')).toBe(false);
    expect(isCanonicalTableRoutePath('/post', undefined)).toBe(false);
  });
});

describe('default REST handler map', () => {
  it('covers all wired method names', () => {
    for (const m of REST_HANDLER_METHOD_NAMES) {
      expect(typeof DEFAULT_REST_HANDLER_LOGIC[m]).toBe('string');
      expect(DEFAULT_REST_HANDLER_LOGIC[m].length).toBeGreaterThan(10);
    }
  });
});
