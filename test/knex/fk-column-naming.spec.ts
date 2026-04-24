import {
  getForeignKeyColumnName,
  getJunctionColumnNames,
} from '../../src/domain/query-dsl/utils/sql-schema-naming.util';

describe('getForeignKeyColumnName', () => {
  it('should append Id to simple camelCase property names', () => {
    expect(getForeignKeyColumnName('customer')).toBe('customerId');
    expect(getForeignKeyColumnName('role')).toBe('roleId');
    expect(getForeignKeyColumnName('table')).toBe('tableId');
  });

  it('should preserve underscores in snake_case property names (bug fix)', () => {
    // This was the bug: previously converted ca_si → caSiId instead of ca_siId
    expect(getForeignKeyColumnName('ca_si')).toBe('ca_siId');
    expect(getForeignKeyColumnName('nghe_si')).toBe('nghe_siId');
    expect(getForeignKeyColumnName('the_loai')).toBe('the_loaiId');
  });

  it('should handle mixed naming property names', () => {
    expect(getForeignKeyColumnName('sourceTable')).toBe('sourceTableId');
    expect(getForeignKeyColumnName('targetTable')).toBe('targetTableId');
    expect(getForeignKeyColumnName('created_by')).toBe('created_byId');
  });

  it('should handle single character property names', () => {
    expect(getForeignKeyColumnName('a')).toBe('aId');
    expect(getForeignKeyColumnName('x')).toBe('xId');
  });

  it('should handle properties with multiple underscores', () => {
    expect(getForeignKeyColumnName('bai_hat_yeu_thich')).toBe(
      'bai_hat_yeu_thichId',
    );
    expect(getForeignKeyColumnName('danh_sach_phat')).toBe('danh_sach_phatId');
  });

  it('should handle properties ending with underscore', () => {
    expect(getForeignKeyColumnName('test_')).toBe('test_Id');
  });

  it('should produce consistent results with template literal pattern', () => {
    // Ensure getForeignKeyColumnName matches the `${name}Id` pattern
    // that createTable uses, for any property name
    const testCases = [
      'ca_si',
      'customer',
      'the_loai',
      'sourceTable',
      'nghe_si',
    ];
    for (const name of testCases) {
      expect(getForeignKeyColumnName(name)).toBe(`${name}Id`);
    }
  });
});

describe('getJunctionColumnNames', () => {
  it('should produce correct junction column names for snake_case tables', () => {
    const { sourceColumn, targetColumn } = getJunctionColumnNames(
      'album',
      'the_loai',
      'the_loai',
    );
    expect(sourceColumn).toBe('albumId');
    // self-referencing: target uses propertyName
    expect(targetColumn).toBe('the_loaiId');
  });

  it('should produce correct junction column names for different tables', () => {
    const { sourceColumn, targetColumn } = getJunctionColumnNames(
      'album',
      'the_loai',
      'genre',
    );
    expect(sourceColumn).toBe('albumId');
    expect(targetColumn).toBe('genreId');
  });
});
