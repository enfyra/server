import { sanitizeExtensionBuildName } from '../../src/modules/extension-definition/utils/compiler.util';
import {
  assertValidVueSFC,
  isProbablyVueSFC,
} from '../../src/modules/extension-definition/utils/validation.util';

describe('GitHub Advanced Security hardening', () => {
  it('validates Vue SFC tags without case-sensitive HTML regex assumptions', () => {
    const sfc = '<SCRIPT setup>const ok = true</script >';

    expect(isProbablyVueSFC(sfc)).toBe(true);
    expect(() => assertValidVueSFC(sfc)).not.toThrow();
  });

  it('sanitizes extension build names before path or browser key usage', () => {
    expect(sanitizeExtensionBuildName('../bad/name";alert(1)//')).toBe(
      '___bad_name__alert_1___',
    );
    expect(sanitizeExtensionBuildName('')).toBe('extension');
  });
});
