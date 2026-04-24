import { autoSlug } from '../../src/shared/utils/auto-slug.helper';

describe('autoSlug', () => {
  it('works with default separator', () => {
    expect(autoSlug('Hello World')).toBe('hello-world');
  });

  it('works with custom separator', () => {
    expect(autoSlug('Hello World', { separator: '_' })).toBe('hello_world');
  });

  it('handles regex metacharacter in separator without ReDoS', () => {
    const start = Date.now();
    const result = autoSlug('Hello World Test', { separator: '.' });
    const elapsed = Date.now() - start;
    expect(elapsed).toBeLessThan(100);
    expect(result).toBe('hello.world.test');
  });

  it('handles + separator (regex metacharacter)', () => {
    const result = autoSlug('Hello World', { separator: '+' });
    expect(result).toBe('hello+world');
  });

  it('handles * separator (regex metacharacter)', () => {
    const result = autoSlug('Hello World', { separator: '*' });
    expect(result).toBe('hello*world');
  });

  it('collapses consecutive separators', () => {
    expect(autoSlug('Hello   World')).toBe('hello-world');
  });

  it('trims leading/trailing separators', () => {
    expect(autoSlug('  Hello World  ')).toBe('hello-world');
  });

  it('respects maxLength', () => {
    const result = autoSlug('A very long title that exceeds the limit', {
      maxLength: 10,
    });
    expect(result.length).toBeLessThanOrEqual(10);
  });

  it('handles Vietnamese characters', () => {
    expect(autoSlug('Xin chào thế giới')).toBe('xin-chao-the-gioi');
  });

  it('handles đ/Đ explicitly', () => {
    expect(autoSlug('Đà Nẵng')).toBe('da-nang');
  });
});
