import { describe, expect, it, vi } from 'vitest';
import { IsolatedExecutorService } from '@enfyra/kernel';
import { WebsocketContextFactory } from '../../src/modules/websocket';
import { DynamicContextFactory } from '../../src/shared/services';

function createService() {
  return new IsolatedExecutorService({
    packageCacheService: {
      getPackages: async () => [],
    } as any,
    packageCdnLoaderService: {
      getPackageSources: () => [],
    } as any,
  });
}

function createContextFactory() {
  const apiTokenService = {
    createForUser: vi.fn(async ({ userId, name, expiresAt }) => ({
      id: 'token-1',
      name,
      prefix: 'efy_pat_test',
      last4: 'test',
      expiresAt: expiresAt || 'never',
      token: `efy_pat_${userId}`,
    })),
    verifyForScript: vi.fn(async () => ({
      userId: 'user-1',
      tokenId: 'token-1',
      expiresAt: 'never',
    })),
  };
  return {
    apiTokenService,
    factory: new DynamicContextFactory({
    bcryptService: {
      hash: async (plain: string) => `hashed:${plain}`,
      compare: async (plain: string, hash: string) => hash === `hashed:${plain}`,
    } as any,
    apiTokenService: apiTokenService as any,
    userCacheService: {} as any,
    envService: { get: () => 'test-secret' } as any,
    websocketContextFactory: new WebsocketContextFactory({
      dynamicWebSocketGateway: {},
    }),
    }),
  };
}

describe('dynamic context env and crypto helpers', () => {
  it('exposes only non-sensitive exact env keys through isolated execution', async () => {
    process.env.NODE_NAME = 'test-node';
    process.env.PORT = '1105';
    process.env.DB_URI = 'postgresql://secret';
    process.env.DB_REPLICA_URIS = 'postgresql://replica-secret';
    process.env.REDIS_URI = 'redis://secret';
    process.env.SECRET_KEY = 'secret-key';
    process.env.ADMIN_PASSWORD = 'admin-secret';

    const service = createService();
    try {
      const ctx = createContextFactory().factory.createBase();
      const result = await service.run(
        `return {
          nodeName: $ctx.$env.NODE_NAME,
          port: $ctx.$env.PORT,
          dbUri: $ctx.$env.DB_URI,
          replicaUris: $ctx.$env.DB_REPLICA_URIS,
          redisUri: $ctx.$env.REDIS_URI,
          secretKey: $ctx.$env.SECRET_KEY,
          adminPassword: $ctx.$env.ADMIN_PASSWORD,
          processType: typeof process
        };`,
        ctx,
        5000,
      );

      expect(result).toEqual({
        nodeName: 'test-node',
        port: '1105',
        dbUri: undefined,
        replicaUris: undefined,
        redisUri: undefined,
        secretKey: undefined,
        adminPassword: undefined,
        processType: 'undefined',
      });
    } finally {
      service.onDestroy();
    }
  });

  it('exposes bounded crypto helpers without exposing legacy ssh helper', async () => {
    const service = createService();
    try {
      const ctx = createContextFactory().factory.createBase();
      const result = await service.run(
        `const pair = await $ctx.$helpers.$crypto.generateSshKeyPair('test-host');
         let sshError = null;
         try {
           await $ctx.$helpers.$ssh.generateKeyPair('test-host');
         } catch (error) {
           sshError = error && error.message ? error.message : String(error);
         }
         return {
           uuidLength: $ctx.$helpers.$crypto.randomUUID().length,
           randomHexLength: $ctx.$helpers.$crypto.randomBytes(8).length,
           randomClampLength: $ctx.$helpers.$crypto.randomBytes(5000).length,
           sha: $ctx.$helpers.$crypto.sha256('hello'),
           hmac: $ctx.$helpers.$crypto.hmacSha256('hello', 'secret'),
           privateKey: pair.privateKey,
           publicKey: pair.publicKey,
           sshError
         };`,
        ctx,
        15000,
      );

      expect(result.uuidLength).toBe(36);
      expect(result.randomHexLength).toBe(16);
      expect(result.randomClampLength).toBe(8192);
      expect(result.sha).toHaveLength(64);
      expect(result.hmac).toHaveLength(64);
      expect(result.privateKey).toContain('BEGIN RSA PRIVATE KEY');
      expect(result.publicKey).toMatch(/^ssh-rsa /);
      expect(result.publicKey).toContain(' test-host');
      expect(result.sshError).toContain('Helper not found');
    } finally {
      service.onDestroy();
    }
  });

  it('exposes request headers and rawBody through isolated execution', async () => {
    const service = createService();
    try {
      const ctx = createContextFactory().factory.createBase({
        body: { event_type: 'transaction.completed' },
        req: {
          headers: { 'paddle-signature': 'ts=1;h1=test' },
          rawBody: '{"event_type":"transaction.completed"}',
        } as any,
      });
      const result = await service.run(
        `return {
          signature: $ctx.$req.headers['paddle-signature'],
          rawBody: $ctx.$req.rawBody,
          body: $ctx.$body
        };`,
        ctx,
        5000,
      );

      expect(result).toEqual({
        signature: 'ts=1;h1=test',
        rawBody: '{"event_type":"transaction.completed"}',
        body: { event_type: 'transaction.completed' },
      });
    } finally {
      service.onDestroy();
    }
  });

  it('exposes PAT issuance and verification through the dynamic helper', async () => {
    const service = createService();
    const { factory, apiTokenService } = createContextFactory();
    try {
      const result = await service.run(
        `const created = await $ctx.$helpers.$pat.create({
           userId: 'user-42',
           name: 'Script token',
           expiresAt: 'never'
         });
         const verified = await $ctx.$helpers.$pat.verify(created.token);
         return { created, verified };`,
        factory.createBase(),
        5000,
      );

      expect(result).toEqual({
        created: {
          id: 'token-1',
          name: 'Script token',
          prefix: 'efy_pat_test',
          last4: 'test',
          expiresAt: 'never',
          token: 'efy_pat_user-42',
        },
        verified: {
          userId: 'user-1',
          tokenId: 'token-1',
          expiresAt: 'never',
        },
      });
      expect(apiTokenService.createForUser).toHaveBeenCalledWith({
        userId: 'user-42',
        name: 'Script token',
        expiresAt: 'never',
      });
      expect(apiTokenService.verifyForScript).toHaveBeenCalledWith('efy_pat_user-42');
    } finally {
      service.onDestroy();
    }
  });
});
