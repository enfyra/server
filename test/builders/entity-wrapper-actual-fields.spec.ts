import { Project, QuoteKind } from 'ts-morph';
import { wrapEntityClass } from '../../src/modules/code-generation/builders/entity-wrapper';
import { ValidationException } from '../../src/core/exceptions/custom-exceptions';

describe('EntityWrapper - Actual Fields Validation', () => {
  let project: Project;

  beforeEach(() => {
    project = new Project({
      manipulationSettings: {
        quoteKind: QuoteKind.Single,
      },
    });
  });

  describe('Actual Entity Fields Validation', () => {
    it('should validate against actual entity fields when provided', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Mock scenario: Only field1, field2 exist in actual entity
      const actualEntityFields = new Set(['field1', 'field2', 'id', 'createdAt', 'updatedAt']);

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['field1'] }, // Valid - exists in actual entity
          { value: ['field2'] }, // Valid - exists in actual entity
          { value: ['field1', 'field2'] }, // Valid - both exist
        ],
        indexes: [
          { value: ['id'] }, // Valid - system field exists (not blocked by unique)
        ],
        usedImports,
        validEntityFields: ['field1', 'field2'], // Only include fields that actually exist
        actualEntityFields,
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      // Should have constraints for fields that actually exist
      expect(uniqueDecorators).toHaveLength(3); // field1, field2, field1+field2
      expect(indexDecorators).toHaveLength(1); // Only id

      const uniqueArgs = uniqueDecorators.map(d => d.getArguments()[0].getText()).sort();
      expect(uniqueArgs).toEqual(["['field1', 'field2']", "['field1']", "['field2']"]);

      const indexArgs = indexDecorators.map(d => d.getArguments()[0].getText());
      expect(indexArgs).toEqual(["['id']"]);
    });

    it('should work without actualEntityFields (backward compatibility)', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['field1'] },
          // Can't include nonExistentField - would throw error
        ],
        indexes: [
          { value: ['field2'] },
        ],
        usedImports,
        validEntityFields: ['field1', 'field2'], // nonExistentField not in valid fields
        // actualEntityFields not provided - should work as before
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(1);
    });

    it('should throw errors for system fields missing from actual entity', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Entity has custom fields but missing some system fields (edge case)
      const actualEntityFields = new Set(['customField', 'id', 'createdAt']); // Missing updatedAt

      // Test missing system field in unique constraint
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['updatedAt'] }, // Invalid - system field missing from actual entity
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['customField'],
          actualEntityFields,
        });
      }).toThrow(ValidationException);
      // Test missing system field in index constraint
      expect(() => {
        wrapEntityClass({
          sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
          className: 'TestEntity2',
          tableName: 'test_entity2',
          uniques: [],
          indexes: [
            { value: ['updatedAt'] }, // Invalid - doesn't exist in actual entity
          ],
          usedImports: new Set(),
          validEntityFields: ['customField'],
          actualEntityFields,
        });
      }).toThrow(ValidationException);    });

    it('should throw errors for empty actualEntityFields', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const actualEntityFields = new Set<string>(); // Empty set

      // Should throw error since no fields exist in actual entity
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['field1'] }, // Invalid - field doesn't exist in empty actual entity
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['field1'],
          actualEntityFields,
        });
      }).toThrow(ValidationException);
      // Even system fields should fail if not in actual entity
      expect(() => {
        wrapEntityClass({
          sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
          className: 'TestEntity2',
          tableName: 'test_entity2',
          uniques: [
            { value: ['id'] }, // Invalid - even system field doesn't exist
          ],
          indexes: [],
          usedImports: new Set(),
          validEntityFields: [],
          actualEntityFields,
        });
      }).toThrow(ValidationException);    });

    it('should throw errors for actual field mismatches', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const actualEntityFields = new Set(['field1']); // Only field1 exists

      // Test unique constraint with missing actual field
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['field2'] }, // Invalid - not in actual entity
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['field1', 'field2'], // Valid in schema
          actualEntityFields,
        });
      }).toThrow(ValidationException);
      // Test index constraint with missing actual field
      expect(() => {
        wrapEntityClass({
          sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
          className: 'TestEntity2',
          tableName: 'test_entity2',
          uniques: [],
          indexes: [
            { value: ['field3'] }, // Invalid - not in actual entity
          ],
          usedImports: new Set(),
          validEntityFields: ['field1', 'field2', 'field3'], // Valid in schema
          actualEntityFields,
        });
      }).toThrow(ValidationException);    });

    it('should handle valid complex scenarios', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const actualEntityFields = new Set(['validField1', 'validField2', 'id', 'createdAt', 'updatedAt']);

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['validField1'] }, // ✅ Valid in both layers
          { value: ['validField2'] }, // ✅ Valid in both layers
          { value: ['validField1', 'validField2'] }, // ✅ Valid composite
        ],
        indexes: [
          { value: ['createdAt'] }, // ✅ System field, not blocked by unique
        ],
        usedImports,
        validEntityFields: ['validField1', 'validField2'], // Only include valid actual fields
        actualEntityFields,
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      // Should have constraints that pass both validation layers
      expect(uniqueDecorators).toHaveLength(3); // validField1, validField2, validField1+validField2
      expect(indexDecorators).toHaveLength(1); // createdAt

      const uniqueArgs = uniqueDecorators.map(d => d.getArguments()[0].getText()).sort();
      expect(uniqueArgs).toEqual(["['validField1', 'validField2']", "['validField1']", "['validField2']"]);

      const indexArgs = indexDecorators.map(d => d.getArguments()[0].getText());
      expect(indexArgs).toEqual(["['createdAt']"]);
    });

    it('should throw errors for complex invalid scenarios', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const actualEntityFields = new Set(['validField1', 'validField2', 'id', 'createdAt', 'updatedAt']);

      // Test field valid in schema but not in actual entity
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['validButNotActual'] }, // ❌ Valid in schema but not in actual entity
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['validField1', 'validField2', 'validButNotActual'],
          actualEntityFields,
        });
      }).toThrow(ValidationException);
      // Test mixed valid/invalid actual fields
      expect(() => {
        wrapEntityClass({
          sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
          className: 'TestEntity2',
          tableName: 'test_entity2',
          uniques: [
            { value: ['validField1', 'validButNotActual'] }, // ❌ Mixed valid/invalid actual
          ],
          indexes: [],
          usedImports: new Set(),
          validEntityFields: ['validField1', 'validField2', 'validButNotActual'],
          actualEntityFields,
        });
      }).toThrow(ValidationException);    });
  });
});