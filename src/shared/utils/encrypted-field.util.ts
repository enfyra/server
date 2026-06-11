import {
  createCipheriv,
  createDecipheriv,
  createHash,
  randomBytes,
} from 'node:crypto';

const ENCRYPTED_PREFIX = 'enc:v1:';
const KEY_ENV = 'SECRET_KEY';

function getMasterKey(): Buffer {
  const value = process.env[KEY_ENV];
  if (!value) throw new Error(`${KEY_ENV} is required`);
  return createHash('sha256')
    .update('enfyra-data-encryption-v1:')
    .update(value)
    .digest();
}

function encode(value: Buffer): string {
  return value.toString('base64url');
}

function decode(value: string): Buffer {
  return Buffer.from(value, 'base64url');
}

export function isEncryptedFieldValue(value: unknown): boolean {
  return typeof value === 'string' && value.startsWith(ENCRYPTED_PREFIX);
}

export function encodeEncryptedFieldPlainValue(value: unknown): string {
  if (isEncryptedFieldValue(value)) {
    throw new Error('Encrypted field values must be submitted as plaintext');
  }

  const plaintext = JSON.stringify({ value });
  const iv = randomBytes(12);
  const cipher = createCipheriv('aes-256-gcm', getMasterKey(), iv);
  const encrypted = Buffer.concat([
    cipher.update(plaintext, 'utf8'),
    cipher.final(),
  ]);
  const tag = cipher.getAuthTag();
  return `${ENCRYPTED_PREFIX}${encode(iv)}:${encode(tag)}:${encode(encrypted)}`;
}

export function decodeEncryptedFieldValue(value: unknown): unknown {
  if (!isEncryptedFieldValue(value)) return value;

  const text = String(value);
  const parts = text.slice(ENCRYPTED_PREFIX.length).split(':');
  if (parts.length !== 3) {
    throw new Error('Invalid encrypted field format');
  }

  const [ivText, tagText, encryptedText] = parts;
  const decipher = createDecipheriv(
    'aes-256-gcm',
    getMasterKey(),
    decode(ivText),
  );
  decipher.setAuthTag(decode(tagText));
  const decrypted = Buffer.concat([
    decipher.update(decode(encryptedText)),
    decipher.final(),
  ]).toString('utf8');

  const payload = JSON.parse(decrypted);
  return payload?.value;
}

export function encryptRecordFields(record: any, columns: any[]): any {
  if (!record || typeof record !== 'object' || Array.isArray(record)) {
    return record;
  }

  let next = record;
  for (const column of columns) {
    if (column?.isEncrypted !== true) continue;
    const field = column.name;
    if (!(field in next)) continue;
    const value = next[field];
    if (value === null || value === undefined) continue;
    if (next === record) next = { ...record };
    next[field] = encodeEncryptedFieldPlainValue(value);
  }
  return next;
}

export function decryptRecordFields(record: any, columns: any[]): any {
  if (!record || typeof record !== 'object' || Array.isArray(record)) {
    return record;
  }

  let next = record;
  for (const column of columns) {
    if (column?.isEncrypted !== true) continue;
    const field = column.name;
    if (!(field in next)) continue;
    const value = next[field];
    if (!isEncryptedFieldValue(value)) continue;
    if (next === record) next = { ...record };
    next[field] = decodeEncryptedFieldValue(value);
  }
  return next;
}

export function decryptResultFields(result: any, columns: any[]): any {
  if (Array.isArray(result)) {
    return result.map((record) => decryptRecordFields(record, columns));
  }
  return decryptRecordFields(result, columns);
}
