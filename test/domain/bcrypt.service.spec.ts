import { BcryptService } from 'src/domain/auth';

describe('BcryptService', () => {
  it('compare succeeds when password longer than 72 matches hashed truncated input', async () => {
    const svc = new BcryptService();
    const long = 'p'.repeat(100);
    const hash = await svc.hash(long, 4);
    expect(await svc.compare(long, hash)).toBe(true);
  });

  it('compare fails for wrong password', async () => {
    const svc = new BcryptService();
    const hash = await svc.hash('secret', 4);
    expect(await svc.compare('other', hash)).toBe(false);
  });

  it('hash returns bcrypt-shaped string', async () => {
    const svc = new BcryptService();
    const hash = await svc.hash('a', 4);
    expect(hash).toMatch(/^\$2[aby]\$/);
    expect(hash.length).toBeGreaterThan(20);
  });
});
