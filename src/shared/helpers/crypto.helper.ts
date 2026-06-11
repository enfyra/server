import {
  createHash,
  createHmac,
  generateKeyPair,
  randomBytes,
  randomUUID,
} from 'node:crypto';
import { promisify } from 'node:util';

const generateKeyPairAsync = promisify(generateKeyPair);
const MAX_RANDOM_BYTES = 4096;
const RANDOM_ENCODINGS = new Set(['hex', 'base64', 'base64url']);
const DIGEST_ENCODINGS = new Set(['hex', 'base64', 'base64url']);

export type CryptoEncoding = 'hex' | 'base64' | 'base64url';

export interface SshKeyPair {
  publicKey: string;
  privateKey: string;
}

export interface CryptoHelper {
  randomUUID: () => string;
  randomBytes: (size?: number, encoding?: CryptoEncoding) => string;
  sha256: (value: string, encoding?: CryptoEncoding) => string;
  hmacSha256: (
    value: string,
    secret: string,
    encoding?: CryptoEncoding,
  ) => string;
  generateSshKeyPair: (comment?: string) => Promise<SshKeyPair>;
}

function normalizeEncoding(
  value: CryptoEncoding | undefined,
  allowed: Set<string>,
): CryptoEncoding {
  const encoding = value || 'hex';
  if (!allowed.has(encoding)) return 'hex';
  return encoding;
}

function normalizeRandomSize(size?: number): number {
  const value = Math.trunc(Number(size) || 32);
  return Math.max(1, Math.min(value, MAX_RANDOM_BYTES));
}

function decodeBase64Url(value: string): Buffer {
  return Buffer.from(value, 'base64url');
}

function sshString(value: string | Buffer): Buffer {
  const input = Buffer.isBuffer(value) ? value : Buffer.from(value);
  const length = Buffer.alloc(4);
  length.writeUInt32BE(input.length, 0);
  return Buffer.concat([length, input]);
}

function rsaPublicKeyToOpenSsh(publicKey: any, comment?: string): string {
  const jwk = publicKey.export({ format: 'jwk' }) as {
    e: string;
    n: string;
  };
  const e = decodeBase64Url(jwk.e);
  const n = decodeBase64Url(jwk.n);
  const nValue = n[0] & 0x80 ? Buffer.concat([Buffer.from([0]), n]) : n;
  const body = Buffer.concat([sshString('ssh-rsa'), sshString(e), sshString(nValue)]);
  const suffix = comment ? ` ${comment}` : '';
  return `ssh-rsa ${body.toString('base64')}${suffix}`;
}

export function createCryptoHelper(): CryptoHelper {
  return {
    randomUUID,
    randomBytes(size?: number, encoding?: CryptoEncoding) {
      return randomBytes(normalizeRandomSize(size)).toString(
        normalizeEncoding(encoding, RANDOM_ENCODINGS),
      );
    },
    sha256(value: string, encoding?: CryptoEncoding) {
      return createHash('sha256')
        .update(String(value))
        .digest(normalizeEncoding(encoding, DIGEST_ENCODINGS));
    },
    hmacSha256(value: string, secret: string, encoding?: CryptoEncoding) {
      return createHmac('sha256', String(secret))
        .update(String(value))
        .digest(normalizeEncoding(encoding, DIGEST_ENCODINGS));
    },
    async generateSshKeyPair(comment?: string) {
      const { publicKey, privateKey } = await generateKeyPairAsync('rsa', {
        modulusLength: 4096,
        publicExponent: 0x10001,
      });
      return {
        publicKey: rsaPublicKeyToOpenSsh(publicKey, comment),
        privateKey: privateKey.export({
          type: 'pkcs1',
          format: 'pem',
        }) as string,
      };
    },
  };
}
