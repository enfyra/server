import { describe, expect, it, beforeEach } from 'vitest';
import {
  decryptRecordFields,
  encodeEncryptedFieldPlainValue,
  encryptRecordFields,
  isEncryptedFieldValue,
} from '../../src/shared/utils/encrypted-field.util';

describe('encrypted field utilities', () => {
  beforeEach(() => {
    process.env.SECRET_KEY = 'test-encryption-key';
  });

  it('encrypts and decrypts marked record fields', () => {
    const columns = [
      { name: 'publicValue', isEncrypted: false },
      { name: 'secretValue', isEncrypted: true },
    ];

    const encrypted = encryptRecordFields(
      { publicValue: 'visible', secretValue: 'hidden' },
      columns,
    );

    expect(encrypted.publicValue).toBe('visible');
    expect(encrypted.secretValue).not.toBe('hidden');
    expect(isEncryptedFieldValue(encrypted.secretValue)).toBe(true);

    const decrypted = decryptRecordFields(encrypted, columns);
    expect(decrypted).toEqual({
      publicValue: 'visible',
      secretValue: 'hidden',
    });
  });

  it('rejects client-submitted ciphertext', () => {
    const encrypted = encodeEncryptedFieldPlainValue('hidden');

    expect(() =>
      encryptRecordFields({ secretValue: encrypted }, [
        { name: 'secretValue', isEncrypted: true },
      ]),
    ).toThrow('Encrypted field values must be submitted as plaintext');
  });

  it('preserves scalar string values that look like JSON', () => {
    const columns = [{ name: 'secretValue', isEncrypted: true }];
    const encrypted = encryptRecordFields({ secretValue: '123' }, columns);

    expect(decryptRecordFields(encrypted, columns).secretValue).toBe('123');
  });
});
