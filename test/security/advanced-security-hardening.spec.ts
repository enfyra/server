import os from 'os';
import path from 'path';
import { resolveUploadedTempFilePath } from '../../src/http/routes/file.routes';
import { sanitizeExtensionBuildName } from '../../src/modules/extension-definition/utils/compiler.util';
import { ImageProcessorHelper } from '../../src/modules/file-management/utils/image-processor.helper';
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

  it('rejects lowercase auto-injected extension component tags', () => {
    expect(() =>
      assertValidVueSFC('<template><UButton>Save</UButton></template>'),
    ).not.toThrow();
    expect(() =>
      assertValidVueSFC('<template><ubutton>Save</ubutton></template>'),
    ).toThrow(/use <UButton> instead of <ubutton>/);
  });

  it('rejects manual component resolution in extension SFCs', () => {
    const sfc = [
      '<template><div /></template>',
      '<script setup>',
      "const UButton = resolveComponent('UButton')",
      '</script>',
    ].join('\n');

    expect(() => assertValidVueSFC(sfc)).toThrow(/do not call resolveComponent/);
  });

  it('sanitizes extension build names before path or browser key usage', () => {
    expect(sanitizeExtensionBuildName('../bad/name";alert(1)//')).toBe(
      '___bad_name__alert_1___',
    );
    expect(sanitizeExtensionBuildName('')).toBe('extension');
  });

  it('accepts only Enfyra-owned upload temp files before streaming them', () => {
    const tempPath = path.join(os.tmpdir(), 'enfyra-upload-test');

    expect(resolveUploadedTempFilePath({ path: tempPath })).toBe(
      path.resolve(tempPath),
    );
    expect(() =>
      resolveUploadedTempFilePath({ path: path.join(os.tmpdir(), 'other') }),
    ).toThrow(/Invalid uploaded temp file path/);
    expect(() =>
      resolveUploadedTempFilePath({ path: '/etc/passwd' }),
    ).toThrow(/Invalid uploaded temp file path/);
  });

  it('selects image output formats through explicit branches', () => {
    const calls: string[] = [];
    const processor = {
      webp: () => {
        calls.push('webp');
        return processor;
      },
    };

    expect(ImageProcessorHelper.setImageFormat(processor as any, 'webp')).toBe(
      processor,
    );
    expect(ImageProcessorHelper.setImageFormat(processor as any, 'unknown')).toBe(
      processor,
    );
    expect(calls).toEqual(['webp']);
  });
});
