import { foldForSqlSearch } from '../../src/shared/utils/unaccent-fold.util';

describe('foldForSqlSearch', () => {
  it('folds Latin accents like NFKD + marks', () => {
    expect(foldForSqlSearch('Résumé')).toBe('resume');
    expect(foldForSqlSearch('CAFÉ')).toBe('cafe');
    expect(foldForSqlSearch('naïve')).toBe('naive');
  });
  it('lowercases ASCII', () => {
    expect(foldForSqlSearch('Alpha')).toBe('alpha');
  });
  it('handles null/undefined', () => {
    expect(foldForSqlSearch(null)).toBe('');
    expect(foldForSqlSearch(undefined)).toBe('');
  });
  it('preserves CJK when no combining peel', () => {
    expect(foldForSqlSearch('你好')).toBe('你好');
  });
});
