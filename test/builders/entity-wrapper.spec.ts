import { Project, QuoteKind } from 'ts-morph';
import { wrapEntityClass } from '../../src/modules/code-generation/builders/entity-wrapper';
import { ValidationException } from '../../src/core/exceptions/custom-exceptions';

describe('EntityWrapper - Simplified Approach', () => {
  let project: Project;

  beforeEach(() => {
    project = new Project({
      manipulationSettings: {
        quoteKind: QuoteKind.Single,
      },
    });
  });

  describe('Class-level Constraints Only', () => {
    it('should create @Unique decorator when requested', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [{ value: ['path'] }],
        indexes: [],
        usedImports,
        validEntityFields: ['path'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      
      expect(uniqueDecorators).toHaveLength(1);
      expect(uniqueDecorators[0].getArguments()[0].getText()).toBe("['path']");
      expect(usedImports.has('Unique')).toBe(true);
    });

    it('should create @Index decorator when requested', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [],
        indexes: [{ value: ['userId'] }],
        usedImports,
        validEntityFields: ['userId'],
      });

      const decorators = classDeclaration.getDecorators();
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      expect(indexDecorators).toHaveLength(1);
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['userId']");
      expect(usedImports.has('Index')).toBe(true);
    });

    it('should handle composite unique constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [{ value: ['parent', 'slug'] }],
        indexes: [],
        usedImports,
        validEntityFields: ['parent', 'slug'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      
      expect(uniqueDecorators).toHaveLength(1);
      expect(uniqueDecorators[0].getArguments()[0].getText()).toBe("['parent', 'slug']");
    });

    it('should handle composite index constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [],
        indexes: [{ value: ['user', 'category'] }],
        usedImports,
        validEntityFields: ['user', 'category'],
      });

      const decorators = classDeclaration.getDecorators();
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      expect(indexDecorators).toHaveLength(1);
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['category', 'user']"); // Sorted alphabetically
    });

    it('should handle both unique and index constraints together', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [{ value: ['email'] }],
        indexes: [{ value: ['userId'] }],
        usedImports,
        validEntityFields: ['email', 'userId'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(1);
      expect(uniqueDecorators[0].getArguments()[0].getText()).toBe("['email']");
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['userId']");
    });

    it('should prevent duplicate unique constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['parent', 'slug'] },
          { value: ['slug', 'parent'] }, // Same constraint, different order
        ],
        indexes: [],
        usedImports,
        validEntityFields: ['parent', 'slug'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      
      // Should only have 1 unique decorator (duplicates removed)
      expect(uniqueDecorators).toHaveLength(1);
      expect(uniqueDecorators[0].getArguments()[0].getText()).toBe("['parent', 'slug']");
    });

    it('should prevent duplicate index constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [],
        indexes: [
          { value: ['user', 'category'] },
          { value: ['category', 'user'] }, // Same constraint, different order
        ],
        usedImports,
        validEntityFields: ['user', 'category'],
      });

      const decorators = classDeclaration.getDecorators();
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      // Should only have 1 index decorator (duplicates removed)
      expect(indexDecorators).toHaveLength(1);
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['category', 'user']");
    });

    it('should prevent index duplicates when unique exists for same fields', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [{ value: ['parent', 'slug'] }],
        indexes: [{ value: ['slug', 'parent'] }], // Same fields as unique
        usedImports,
        validEntityFields: ['parent', 'slug'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      // Should have 1 unique and 0 indexes (unique prevents duplicate index)
      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(0);
    });
  });

  describe('Field Validation', () => {
    it('should throw errors for constraints with non-existent fields', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Test unique constraint with non-existent field
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['nonExistentField'] }, // Should throw error
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['existingField'],
        });
      }).toThrow(ValidationException);

      // Test index constraint with non-existent field
      expect(() => {
        wrapEntityClass({
          sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
          className: 'TestEntity2',
          tableName: 'test_entity2',
          uniques: [],
          indexes: [
            { value: ['anotherNonExistentField'] }, // Should throw error
          ],
          usedImports: new Set(),
          validEntityFields: ['existingField'],
        });
      }).toThrow(ValidationException);
    });

    it('should throw errors for constraints with mixed valid/invalid fields', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Test constraint with mixed valid/invalid fields
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['validField', 'invalidField'] }, // Should throw error
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['validField', 'name', 'email'],
        });
      }).toThrow(ValidationException);
    });

    it('should include system fields in validation', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['createdAt'] }, // System field - should be valid
          { value: ['updatedAt'] }, // System field - should be valid
        ],
        indexes: [
          { value: ['id'] }, // System field - should be valid
        ],
        usedImports,
        validEntityFields: [], // Empty but system fields should still work
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      expect(uniqueDecorators).toHaveLength(2);
      expect(indexDecorators).toHaveLength(1);
    });
  });

  describe('Edge Cases', () => {
    it('should handle empty arrays gracefully', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [],
        indexes: [],
        usedImports,
        validEntityFields: [],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      expect(uniqueDecorators).toHaveLength(0);
      expect(indexDecorators).toHaveLength(0);
      expect(usedImports.has('Unique')).toBe(false);
      expect(usedImports.has('Index')).toBe(false);
    });

    it('should handle undefined parameters', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: undefined,
          indexes: undefined,
          usedImports,
        });
      }).not.toThrow();
    });

    it('should filter out empty field arrays', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: [] }, // Empty array - should be skipped
          { value: ['validField'] },
        ],
        indexes: [
          { value: [] }, // Empty array - should be skipped
          { value: ['anotherField'] },
        ],
        usedImports,
        validEntityFields: ['validField', 'anotherField'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');
      
      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(1);
    });

    it('should preserve decorator order consistently', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [{ value: ['a', 'b'] }],
        indexes: [{ value: ['c', 'd'] }],
        usedImports,
        validEntityFields: ['a', 'b', 'c', 'd'],
      });

      const decorators = classDeclaration.getDecorators();
      
      // @Entity should always be first
      expect(decorators[0].getName()).toBe('Entity');
      
      // @Unique should come before @Index
      const decoratorNames = decorators.map(d => d.getName());
      const uniqueIndex = decoratorNames.indexOf('Unique');
      const indexIndex = decoratorNames.indexOf('Index');
      
      if (uniqueIndex !== -1 && indexIndex !== -1) {
        expect(uniqueIndex).toBeLessThan(indexIndex);
      }
    });
  });
});