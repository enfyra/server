import { autoSlug, generateUniqueSlug, batchAutoSlug } from '../../src/shared/utils/auto-slug.helper';

describe('AutoSlug Helper', () => {
  describe('autoSlug', () => {
    describe('Basic functionality', () => {
      it('should convert simple string to slug', () => {
        expect(autoSlug('Hello World')).toBe('hello-world');
        expect(autoSlug('Test String')).toBe('test-string');
      });

      it('should handle empty and invalid inputs', () => {
        expect(autoSlug('')).toBe('');
        expect(autoSlug('   ')).toBe('');
        expect(autoSlug(null as any)).toBe('');
        expect(autoSlug(undefined as any)).toBe('');
      });

      it('should remove special characters', () => {
        expect(autoSlug('Hello@World#123!')).toBe('helloworld123');
        expect(autoSlug('Test & Development')).toBe('test-development');
        expect(autoSlug('Price: $100')).toBe('price-100');
      });
    });

    describe('Vietnamese support', () => {
      it('should normalize Vietnamese characters', () => {
        expect(autoSlug('Tiếng Việt')).toBe('tieng-viet');
        expect(autoSlug('Bánh mì phở')).toBe('banh-mi-pho');
        expect(autoSlug('Đại học Bách khoa')).toBe('dai-hoc-bach-khoa');
      });

      it('should handle complex Vietnamese text', () => {
        expect(autoSlug('Chào mừng đến với Việt Nam')).toBe('chao-mung-den-voi-viet-nam');
        expect(autoSlug('Phần mềm quản lý')).toBe('phan-mem-quan-ly');
        expect(autoSlug('Hệ thống thông tin')).toBe('he-thong-thong-tin');
      });

      it('should handle mixed Vietnamese and English', () => {
        expect(autoSlug('Hello Việt Nam')).toBe('hello-viet-nam');
        expect(autoSlug('API Management System')).toBe('api-management-system');
      });
    });

    describe('Other languages', () => {
      it('should handle European characters', () => {
        expect(autoSlug('Café résumé')).toBe('cafe-resume');
        expect(autoSlug('Niño España')).toBe('nino-espana');
        expect(autoSlug('München')).toBe('munchen');
      });

      it('should handle Chinese characters (basic)', () => {
        expect(autoSlug('你好世界')).toBe('');
        expect(autoSlug('Hello 世界')).toBe('hello');
      });
    });

    describe('Options', () => {
      it('should respect custom separator', () => {
        expect(autoSlug('Hello World', { separator: '_' })).toBe('hello_world');
        expect(autoSlug('Test String', { separator: '|' })).toBe('test|string');
      });

      it('should respect lowercase option', () => {
        expect(autoSlug('Hello World', { lowercase: false })).toBe('Hello-World');
        expect(autoSlug('TEST STRING', { lowercase: false })).toBe('TEST-STRING');
      });

      it('should respect trim option', () => {
        expect(autoSlug('  Hello World  ', { trim: false })).toBe('hello-world');
        expect(autoSlug('  Test  ', { trim: true })).toBe('test');
      });

      it('should respect maxLength option', () => {
        const longText = 'This is a very long string that should be truncated at some point';
        const result20 = autoSlug(longText, { maxLength: 20 });
        const result10 = autoSlug(longText, { maxLength: 10 });
        expect(result20.length).toBeLessThanOrEqual(20);
        expect(result10.length).toBeLessThanOrEqual(10);
      });

      it('should break at word boundaries when truncating', () => {
        const result = autoSlug('This is a very long text', { maxLength: 15 });
        expect(result).not.toMatch(/-$/); // Should not end with separator
        expect(result.length).toBeLessThanOrEqual(15);
      });
    });

    describe('Edge cases', () => {
      it('should handle multiple spaces and separators', () => {
        expect(autoSlug('Hello    World')).toBe('hello-world');
        expect(autoSlug('Test---String')).toBe('test-string');
        expect(autoSlug('Multiple   ---   Separators')).toBe('multiple-separators');
      });

      it('should handle leading and trailing separators', () => {
        expect(autoSlug('---Hello World---')).toBe('hello-world');
        expect(autoSlug('   Test String   ')).toBe('test-string');
      });

      it('should handle numbers', () => {
        expect(autoSlug('Version 2.0.1')).toBe('version-201');
        expect(autoSlug('Chapter 10 - Introduction')).toBe('chapter-10-introduction');
      });
    });
  });

  describe('generateUniqueSlug', () => {
    it('should return original slug if unique', () => {
      expect(generateUniqueSlug('test-slug', ['other-slug', 'another-slug'])).toBe('test-slug');
    });

    it('should append number if slug exists', () => {
      expect(generateUniqueSlug('test-slug', ['test-slug'])).toBe('test-slug-1');
      expect(generateUniqueSlug('test-slug', ['test-slug', 'test-slug-1'])).toBe('test-slug-2');
    });

    it('should handle multiple conflicts', () => {
      const existing = ['test-slug', 'test-slug-1', 'test-slug-2', 'test-slug-4'];
      expect(generateUniqueSlug('test-slug', existing)).toBe('test-slug-3');
    });

    it('should work with empty existing slugs array', () => {
      expect(generateUniqueSlug('test-slug', [])).toBe('test-slug');
      expect(generateUniqueSlug('test-slug')).toBe('test-slug');
    });
  });

  describe('batchAutoSlug', () => {
    it('should convert array of strings to slugs', () => {
      const inputs = ['Hello World', 'Tiếng Việt', 'Test String'];
      const expected = ['hello-world', 'tieng-viet', 'test-string'];
      expect(batchAutoSlug(inputs)).toEqual(expected);
    });

    it('should apply options to all slugs', () => {
      const inputs = ['Hello World', 'Test String'];
      const expected = ['hello_world', 'test_string'];
      expect(batchAutoSlug(inputs, { separator: '_' })).toEqual(expected);
    });

    it('should handle empty array', () => {
      expect(batchAutoSlug([])).toEqual([]);
    });

    it('should handle array with empty strings', () => {
      const inputs = ['Hello', '', 'World'];
      const expected = ['hello', '', 'world'];
      expect(batchAutoSlug(inputs)).toEqual(expected);
    });
  });

  describe('Real-world use cases', () => {
    it('should handle blog post titles', () => {
      expect(autoSlug('10 Tips for Better Web Development')).toBe('10-tips-for-better-web-development');
      expect(autoSlug('How to Build a REST API with Node.js')).toBe('how-to-build-a-rest-api-with-nodejs');
    });

    it('should handle Vietnamese folder names', () => {
      expect(autoSlug('Tài liệu dự án')).toBe('tai-lieu-du-an');
      expect(autoSlug('Hình ảnh sản phẩm')).toBe('hinh-anh-san-pham');
      expect(autoSlug('Báo cáo tháng 12/2024')).toBe('bao-cao-thang-122024');
    });

    it('should handle product names', () => {
      expect(autoSlug('iPhone 15 Pro Max (256GB)')).toBe('iphone-15-pro-max-256gb');
      expect(autoSlug('Samsung Galaxy S24 Ultra')).toBe('samsung-galaxy-s24-ultra');
    });

    it('should handle user names and titles', () => {
      expect(autoSlug('Nguyễn Văn An - CEO')).toBe('nguyen-van-an-ceo');
      expect(autoSlug('Dr. Trần Thị Bình')).toBe('dr-tran-thi-binh');
    });
  });
});