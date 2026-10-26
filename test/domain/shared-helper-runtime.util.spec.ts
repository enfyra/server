import { describe, expect, it } from 'vitest';
import {
  buildSharedHelperRuntime,
  wrapWithSharedHelpers,
} from '../../src/shared/utils/shared-helper-runtime.util';

describe('Server helper runtime composition', () => {
  it('rejects invalid names, duplicate names and enabled empty definitions', () => {
    expect(() =>
      buildSharedHelperRuntime([
        { name: '__proto__', isEnabled: true, code: 'return true;' },
      ]),
    ).toThrow(/name/i);
    expect(() =>
      buildSharedHelperRuntime([
        { name: 'echo', isEnabled: false, code: '' },
        { name: 'echo', isEnabled: false, code: '' },
      ]),
    ).toThrow(/duplicate/i);
    expect(() =>
      buildSharedHelperRuntime([{ name: 'echo', isEnabled: true, code: '' }]),
    ).toThrow(/definition/i);
  });

  it('enforces the installed executor definition and combined script bounds', () => {
    expect(() =>
      buildSharedHelperRuntime(
        Array.from({ length: 1025 }, (_, i) => ({
          name: `helper_${i}`,
          isEnabled: false,
          code: '',
        })),
      ),
    ).toThrow(/definition limit/i);
    expect(() =>
      buildSharedHelperRuntime([
        {
          name: 'huge',
          isEnabled: true,
          code: ' '.repeat(4 * 1024 * 1024) + 'return true;',
        },
      ]),
    ).toThrow(/script limit/i);
    const runtime = buildSharedHelperRuntime([
      { name: 'echo', isEnabled: true, code: 'return $ctx.$params;' },
    ]);
    expect(() =>
      wrapWithSharedHelpers(' '.repeat(4 * 1024 * 1024), runtime, true),
    ).toThrow(/script limit/i);
  });

  it('retains disabled metadata without embedding disabled source', () => {
    const runtime = buildSharedHelperRuntime([
      { name: 'draft', isEnabled: false, code: 'not valid code!' },
    ]);
    expect(runtime).toContain('enabled: false');
    expect(runtime).not.toContain('not valid code!');
  });
});
