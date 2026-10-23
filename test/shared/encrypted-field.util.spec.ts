import { describe, expect, it, beforeEach } from 'vitest';
import {
  decodeEncryptedFieldValue,
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

  it.each(['enc:v1:', 'enc:v1:iv:tag', 'enc:v1:iv:tag:data:extra'])(
    'rejects malformed encrypted field structure %s',
    (value) => {
      expect(() => decodeEncryptedFieldValue(value)).toThrow('Invalid encrypted field format');
    },
  );

  it.each(['enc:v1:::', 'enc:v1:iv::data', 'enc:v1:iv:tag:'])(
    'does not accept empty ciphertext components %s',
    (value) => {
      expect(() => decodeEncryptedFieldValue(value)).toThrow();
    },
  );

  it('rejects tampered authenticated ciphertext', () => {
    const encrypted = encodeEncryptedFieldPlainValue('hidden');
    const dataStart = encrypted.lastIndexOf(':') + 1;
    const replacement = encrypted.charAt(dataStart) === 'A' ? 'B' : 'A';
    const tampered = `${encrypted.slice(0, dataStart)}${replacement}${encrypted.slice(dataStart + 1)}`;

    expect(() => decodeEncryptedFieldValue(tampered)).toThrow();
  });

  it('preserves scalar string values that look like JSON', () => {
    const columns = [{ name: 'secretValue', isEncrypted: true }];
    const encrypted = encryptRecordFields({ secretValue: '123' }, columns);

    expect(decryptRecordFields(encrypted, columns).secretValue).toBe('123');
  });

  it('round-trips non-string values through the JSON envelope', () => {
    const columns = [
      { name: 'numberValue', isEncrypted: true },
      { name: 'objectValue', isEncrypted: true },
    ];

    const encrypted = encryptRecordFields(
      { numberValue: 123, objectValue: { nested: true } },
      columns,
    );

    expect(decryptRecordFields(encrypted, columns)).toEqual({
      numberValue: 123,
      objectValue: { nested: true },
    });
  });

});
