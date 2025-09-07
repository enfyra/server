import { Test, TestingModule } from '@nestjs/testing';
import { BcryptService } from '../../../src/core/auth/services/bcrypt.service';

describe('BcryptService', () => {
  let service: BcryptService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [BcryptService],
    }).compile();

    service = module.get<BcryptService>(BcryptService);
  });

  describe('hash', () => {
    it('should hash password successfully', async () => {
      const password = 'testPassword123';

      const hashedPassword = await service.hash(password);

      expect(hashedPassword).toBeDefined();
      expect(hashedPassword).not.toBe(password);
      expect(hashedPassword.length).toBeGreaterThan(password.length);
    });

    it('should generate different hashes for same password', async () => {
      const password = 'testPassword123';

      const hash1 = await service.hash(password);
      const hash2 = await service.hash(password);

      expect(hash1).not.toBe(hash2);
    });

    it('should handle empty password', async () => {
      const emptyPassword = '';

      const hashedPassword = await service.hash(emptyPassword);

      expect(hashedPassword).toBeDefined();
      expect(hashedPassword).not.toBe(emptyPassword);
    });

    it('should handle special characters', async () => {
      const specialPassword = '!@#$%^&*()_+{}|:"<>?[]\\;\'.,/';

      const hashedPassword = await service.hash(specialPassword);

      expect(hashedPassword).toBeDefined();
      expect(hashedPassword).not.toBe(specialPassword);
    });

    it('should handle unicode characters', async () => {
      const unicodePassword = 'пароль密码パスワード🔐';

      const hashedPassword = await service.hash(unicodePassword);

      expect(hashedPassword).toBeDefined();
      expect(hashedPassword).not.toBe(unicodePassword);
    });

    it('should handle very long passwords', async () => {
      const longPassword = 'a'.repeat(1000);

      const hashedPassword = await service.hash(longPassword);

      expect(hashedPassword).toBeDefined();
      expect(hashedPassword).not.toBe(longPassword);
    });
  });

  describe('compare', () => {
    it('should return true for matching password and hash', async () => {
      const password = 'testPassword123';
      const hashedPassword = await service.hash(password);

      const result = await service.compare(password, hashedPassword);

      expect(result).toBe(true);
    });

    it('should return false for non-matching password and hash', async () => {
      const password = 'testPassword123';
      const wrongPassword = 'wrongPassword456';
      const hashedPassword = await service.hash(password);

      const result = await service.compare(wrongPassword, hashedPassword);

      expect(result).toBe(false);
    });

    it('should return false for empty password against hash', async () => {
      const password = 'testPassword123';
      const hashedPassword = await service.hash(password);

      const result = await service.compare('', hashedPassword);

      expect(result).toBe(false);
    });

    it('should return false for password against empty/invalid hash', async () => {
      const password = 'testPassword123';

      const result = await service.compare(password, '');

      expect(result).toBe(false);
    });

    it('should handle case sensitivity correctly', async () => {
      const password = 'TestPassword123';
      const hashedPassword = await service.hash(password);

      const result1 = await service.compare('testpassword123', hashedPassword);
      const result2 = await service.compare('TESTPASSWORD123', hashedPassword);
      const result3 = await service.compare('TestPassword123', hashedPassword);

      expect(result1).toBe(false);
      expect(result2).toBe(false);
      expect(result3).toBe(true);
    });

    it('should handle special characters in comparison', async () => {
      const specialPassword = '!@#$%^&*()_+{}|:"<>?[]\\;\'.,/';
      const hashedPassword = await service.hash(specialPassword);

      const result = await service.compare(specialPassword, hashedPassword);

      expect(result).toBe(true);
    });

    it('should handle unicode characters in comparison', async () => {
      const unicodePassword = 'пароль密码パスワード🔐';
      const hashedPassword = await service.hash(unicodePassword);

      const result = await service.compare(unicodePassword, hashedPassword);

      expect(result).toBe(true);
    });
  });

  describe('Performance Tests', () => {
    it('should handle multiple hash operations concurrently', async () => {
      const passwords = Array.from({ length: 10 }, (_, i) => `password${i}`);

      const promises = passwords.map((pwd) => service.hash(pwd));
      const hashes = await Promise.all(promises);

      expect(hashes).toHaveLength(10);
      expect(new Set(hashes).size).toBe(10); // All hashes should be unique
    });

    it('should handle multiple compare operations concurrently', async () => {
      const password = 'testPassword123';
      const hashedPassword = await service.hash(password);

      const promises = Array.from({ length: 20 }, () =>
        service.compare(password, hashedPassword),
      );
      const results = await Promise.all(promises);

      expect(results).toHaveLength(20);
      expect(results.every((r) => r === true)).toBe(true);
    });

    it('should complete hash operation within reasonable time', async () => {
      const password = 'testPassword123';

      const startTime = Date.now();
      await service.hash(password);
      const duration = Date.now() - startTime;

      expect(duration).toBeLessThan(1000); // Should complete within 1 second
    });

    it('should complete compare operation within reasonable time', async () => {
      const password = 'testPassword123';
      const hashedPassword = await service.hash(password);

      const startTime = Date.now();
      await service.compare(password, hashedPassword);
      const duration = Date.now() - startTime;

      expect(duration).toBeLessThan(100); // Should complete within 100ms
    });
  });

  describe('Security Tests', () => {
    it('should produce cryptographically secure hashes', async () => {
      const password = 'testPassword123';

      const hashes = await Promise.all(
        Array.from({ length: 10 }, () => service.hash(password)),
      );

      // All hashes should be unique (extremely high probability)
      const uniqueHashes = new Set(hashes);
      expect(uniqueHashes.size).toBe(10);
    }, 10000);

    it('should resist timing attacks on compare', async () => {
      const password = 'testPassword123';
      const hashedPassword = await service.hash(password);

      // Test with different length wrong passwords
      const wrongPasswords = [
        'a',
        'ab',
        'abc',
        'abcd',
        'abcdefghijklmnop',
        'a'.repeat(100),
      ];

      const timings = [];
      for (const wrongPwd of wrongPasswords) {
        const start = process.hrtime.bigint();
        await service.compare(wrongPwd, hashedPassword);
        const end = process.hrtime.bigint();
        timings.push(Number(end - start));
      }

      // All comparisons should return false
      const results = await Promise.all(
        wrongPasswords.map((pwd) => service.compare(pwd, hashedPassword)),
      );
      expect(results.every((r) => r === false)).toBe(true);
    });

    it('should handle null and undefined inputs gracefully', async () => {
      await expect(service.hash(null as any)).rejects.toThrow();
      await expect(service.hash(undefined as any)).rejects.toThrow();
      await expect(service.compare(null as any, 'hash')).rejects.toThrow();
      await expect(service.compare('password', null as any)).rejects.toThrow();
    });
  });

  describe('Edge Cases', () => {
    it('should handle very short passwords', async () => {
      const shortPassword = 'a';

      const hashedPassword = await service.hash(shortPassword);
      const isMatch = await service.compare(shortPassword, hashedPassword);

      expect(isMatch).toBe(true);
    });

    it('should handle passwords with only spaces', async () => {
      const spacesPassword = '   ';

      const hashedPassword = await service.hash(spacesPassword);
      const isMatch = await service.compare(spacesPassword, hashedPassword);

      expect(isMatch).toBe(true);
    });

    it('should handle passwords with newlines and tabs', async () => {
      const specialWhitespace = 'password\n\t\r';

      const hashedPassword = await service.hash(specialWhitespace);
      const isMatch = await service.compare(specialWhitespace, hashedPassword);

      expect(isMatch).toBe(true);
    });

    it('should differentiate similar passwords', async () => {
      const password1 = 'password123';
      const password2 = 'password124';
      const hashedPassword1 = await service.hash(password1);

      const isMatch1 = await service.compare(password1, hashedPassword1);
      const isMatch2 = await service.compare(password2, hashedPassword1);

      expect(isMatch1).toBe(true);
      expect(isMatch2).toBe(false);
    });
  });
});
