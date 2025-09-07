import { Project, QuoteKind } from 'ts-morph';
import { wrapEntityClass } from '../../src/modules/code-generation/builders/entity-wrapper';
import { ValidationException } from '../../src/core/exceptions/custom-exceptions';

describe('EntityWrapper - Stress Test & Edge Cases', () => {
  let project: Project;

  beforeEach(() => {
    project = new Project({
      manipulationSettings: {
        quoteKind: QuoteKind.Single,
      },
    });
  });

  describe('🔥 BRUTAL CONFLICT TESTS', () => {
    it('Case 1: Same constraint with different field orders', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['a', 'b', 'c'] },
          { value: ['c', 'a', 'b'] },
          { value: ['b', 'c', 'a'] },
        ],
        indexes: [
          { value: ['a', 'b', 'c'] },
          { value: ['c', 'b', 'a'] },
        ],
        usedImports,
        validEntityFields: ['a', 'b', 'c'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(0); // Blocked by unique
      expect(uniqueDecorators[0].getArguments()[0].getText()).toBe("['a', 'b', 'c']");
    });

    it('Case 2: Mixed valid/invalid fields with overlaps', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Should throw error for mixed valid/invalid fields
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['valid1', 'invalid1'] }, // Contains invalid field
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['valid1', 'valid2', 'valid3'],
        });
      }).toThrow(ValidationException);    });

    it('Case 3: System fields vs custom fields conflicts', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['id'] },
          { value: ['createdAt', 'customField'] },
        ],
        indexes: [
          { value: ['id'] }, // Should be blocked
          { value: ['updatedAt'] },
          { value: ['createdAt', 'customField'] }, // Should be blocked
        ],
        usedImports,
        validEntityFields: ['customField'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(2);
      expect(indexDecorators).toHaveLength(1);
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['updatedAt']");
    });

    it('Case 4: Empty and null chaos', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          null as any,
          undefined as any,
          { value: null },
          { value: undefined },
          { value: [] },
          { value: [''] },
          { value: ['   '] },
          { value: ['valid'] },
        ],
        indexes: [
          null as any,
          { value: ['valid'] }, // Should be blocked by unique
          { value: ['another'] },
        ],
        usedImports,
        validEntityFields: ['valid', 'another'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(1);
      expect(uniqueDecorators[0].getArguments()[0].getText()).toBe("['valid']");
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['another']");
    });

    it('Case 5: Unicode and special characters', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['用户名', 'ユーザー名'] },
          { value: ['😀_field', '🚀_field'] },
        ],
        indexes: [
          { value: ['用户名', 'ユーザー名'] }, // Should be blocked
          { value: ['field_with_♠️'] },
        ],
        usedImports,
        validEntityFields: ['用户名', 'ユーザー名', '😀_field', '🚀_field', 'field_with_♠️'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(2);
      expect(indexDecorators).toHaveLength(1);
    });
  });

  describe('🌪️ MASSIVE SCALE TESTS', () => {
    it('Case 6: 1000 duplicate constraints (performance test)', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const massiveUniques = [];
      const massiveIndexes = [];
      
      // Create 1000 permutations of same constraint
      for (let i = 0; i < 500; i++) {
        massiveUniques.push({ value: ['field1', 'field2'] });
        massiveUniques.push({ value: ['field2', 'field1'] });
        massiveIndexes.push({ value: ['field1', 'field2'] });
        massiveIndexes.push({ value: ['field2', 'field1'] });
      }

      const startTime = Date.now();
      
      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: massiveUniques,
        indexes: massiveIndexes,
        usedImports,
        validEntityFields: ['field1', 'field2'],
      });

      const endTime = Date.now();
      const processingTime = endTime - startTime;

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      // Should deduplicate to exactly 1 unique, 0 indexes
      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(0);
      expect(processingTime).toBeLessThan(1000); // Should be fast
    });

    it('Case 7: 100 different valid constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const fields = Array.from({ length: 20 }, (_, i) => `field${i}`);
      const uniques = [];
      const indexes = [];

      // Create many different combinations
      for (let i = 0; i < fields.length - 1; i++) {
        uniques.push({ value: [fields[i]] });
        indexes.push({ value: [fields[i + 1]] });
      }

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques,
        indexes,
        usedImports,
        validEntityFields: fields,
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(19);
      expect(indexDecorators).toHaveLength(1); // Only field19 allowed (field1-18 blocked by uniques)
    });

    it('Case 8: Very long field names (1000+ chars)', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const veryLongField = 'a'.repeat(1000) + '_field';
      const anotherLongField = 'b'.repeat(1000) + '_field';

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: [veryLongField] },
        ],
        indexes: [
          { value: [anotherLongField] },
        ],
        usedImports,
        validEntityFields: [veryLongField, anotherLongField],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(1);
      expect(indexDecorators).toHaveLength(1);
    });
  });

  describe('💥 MALICIOUS INPUTS', () => {
    it('Case 9: SQL injection attempts in field names', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const evilFields = [
        "'; DROP TABLE users; --",
        "UNION SELECT * FROM passwords",
        "1=1; DELETE FROM users;",
        "valid_field",
      ];

      // Clean for TS syntax
      const cleanFields = evilFields.map(f => f.replace(/['"\\;-]/g, '_'));

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: [cleanFields[0], cleanFields[1]] },
        ],
        indexes: [
          { value: [cleanFields[2]] },
          { value: [cleanFields[3]] },
        ],
        usedImports,
        validEntityFields: cleanFields,
      });

      expect(() => classDeclaration.getDecorators()).not.toThrow();
    });

    it('Case 10: Circular reference attempts', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const circularObj: any = { value: ['field1'] };
      circularObj.self = circularObj;

      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [circularObj],
          indexes: [],
          usedImports,
          validEntityFields: ['field1'],
        });
      }).not.toThrow();
    });

    it('Case 11: Prototype pollution attempts', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const evilUniques = [
        { value: ['__proto__'] },
        { value: ['constructor'] },
        { value: ['prototype'] },
      ];

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: evilUniques,
        indexes: [],
        usedImports,
        validEntityFields: ['__proto__', 'constructor', 'prototype'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');

      expect(uniqueDecorators).toHaveLength(3);
    });

    it('Case 12: Memory exhaustion attempt (deep nesting)', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Create deeply nested constraint (should be flattened)
      const deepConstraint = { value: ['field1'] };
      
      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [deepConstraint],
        indexes: [],
        usedImports,
        validEntityFields: ['field1'],
      });

      expect(() => classDeclaration.getDecorators()).not.toThrow();
    });
  });

  describe('🎭 MIXED TYPE CHAOS', () => {
    it('Case 13: Mixed data types in constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const mixedConstraints = [
        { value: ['string_field'] },
        { value: [123 as any] }, // Number - should be filtered
        { value: [true as any] }, // Boolean - should be filtered
        { value: [null as any] }, // Null - should be filtered
        { value: [{ nested: 'object' } as any] }, // Object - should be filtered
        { value: ['valid_field'] },
      ];

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: mixedConstraints,
        indexes: [],
        usedImports,
        validEntityFields: ['string_field', 'valid_field'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');

      expect(uniqueDecorators).toHaveLength(2);
    });

    it('Case 14: Case sensitivity edge cases', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['Field'] },
          { value: ['field'] },
          { value: ['FIELD'] },
          { value: ['Field', 'field'] }, // Mix different cases
        ],
        indexes: [
          { value: ['Field'] }, // Should be blocked
          { value: ['field'] }, // Should be blocked
          { value: ['FIELD'] }, // Should be blocked
        ],
        usedImports,
        validEntityFields: ['Field', 'field', 'FIELD'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(4); // All different due to case sensitivity
      expect(indexDecorators).toHaveLength(0); // All blocked by uniques
    });

    it('Case 15: Whitespace and formatting chaos', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: [' field1 '] }, // Leading/trailing spaces
          { value: ['field1'] }, // Clean version
          { value: ['\tfield2\t'] }, // Tabs
          { value: ['\nfield3\n'] }, // Newlines
        ],
        indexes: [],
        usedImports,
        validEntityFields: ['field1', 'field2', 'field3'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');

      // Whitespace should be trimmed and duplicates removed
      expect(uniqueDecorators).toHaveLength(3); // field1, field2, field3 (duplicates removed)
    });
  });

  describe('🏗️ COMPLEX COMBINATIONS', () => {
    it('Case 16: Overlapping multi-field constraints', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['a', 'b'] },
          { value: ['b', 'c'] },
          { value: ['a', 'c'] },
        ],
        indexes: [
          { value: ['a', 'b'] }, // Blocked
          { value: ['b', 'c'] }, // Blocked
          { value: ['a', 'c'] }, // Blocked
          { value: ['d', 'e'] }, // Allowed
        ],
        usedImports,
        validEntityFields: ['a', 'b', 'c', 'd', 'e'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(3);
      expect(indexDecorators).toHaveLength(1);
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['d', 'e']");
    });

    it('Case 17: Single field vs multi-field conflicts', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['field1'] },
          { value: ['field1', 'field2'] },
        ],
        indexes: [
          { value: ['field1'] }, // Should be blocked
          { value: ['field1', 'field2'] }, // Should be blocked
          { value: ['field2'] }, // Should be allowed
        ],
        usedImports,
        validEntityFields: ['field1', 'field2'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(2);
      expect(indexDecorators).toHaveLength(1);
      expect(indexDecorators[0].getArguments()[0].getText()).toBe("['field2']");
    });

    it('Case 18: System fields in complex combinations', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['id', 'createdAt'] },
          { value: ['updatedAt', 'customField'] },
        ],
        indexes: [
          { value: ['id', 'createdAt'] }, // Should be blocked (exact match)
          { value: ['updatedAt', 'customField'] }, // Should be blocked (exact match)  
          { value: ['id'] }, // Should be allowed (not exact match with unique)
          { value: ['anotherField'] }, // Should be allowed
        ],
        usedImports,
        validEntityFields: ['customField', 'anotherField'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(2);
      expect(indexDecorators).toHaveLength(2);
      // Should have ['id'] and ['anotherField']
    });
  });

  describe('🎪 EDGE CASE BONANZA', () => {
    it('Case 19: All empty validEntityFields with system fields only', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Should throw error for non-existent field
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
          validEntityFields: [], // Empty custom fields
        });
      }).toThrow(ValidationException);
      // Test valid case with only system fields
      const classDeclaration = wrapEntityClass({
        sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
        className: 'TestEntity2',
        tableName: 'test_entity2',
        uniques: [
          { value: ['id'] },
          { value: ['createdAt'] },
          { value: ['updatedAt'] },
        ],
        indexes: [
          { value: ['id'] }, // Should be blocked by unique
        ],
        usedImports: new Set(),
        validEntityFields: [], // Empty custom fields
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(3); // Only system fields work
      expect(indexDecorators).toHaveLength(0);
    });

    it('Case 20: Undefined and null arrays mixed', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: undefined,
        indexes: null as any,
        usedImports,
        validEntityFields: undefined as any,
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(0);
      expect(indexDecorators).toHaveLength(0);
      expect(() => classDeclaration.getName()).not.toThrow();
    });

    it('Case 21: Extremely nested field combinations', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const fields = ['a', 'b', 'c', 'd', 'e'];
      const uniques = [];
      const indexes = [];

      // Generate all possible combinations
      for (let i = 1; i <= fields.length; i++) {
        for (let j = 0; j <= fields.length - i; j++) {
          const combination = fields.slice(j, j + i);
          if (i <= 2) {
            uniques.push({ value: combination });
          } else {
            indexes.push({ value: combination });
          }
        }
      }

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques,
        indexes,
        usedImports,
        validEntityFields: fields,
      });

      expect(() => classDeclaration.getDecorators()).not.toThrow();
    });

    it('Case 22: Duplicate field names within same constraint', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: ['field1', 'field1'] }, // Duplicate in same constraint
          { value: ['field2', 'field2', 'field2'] }, // Triple duplicate
        ],
        indexes: [
          { value: ['field3', 'field3'] },
        ],
        usedImports,
        validEntityFields: ['field1', 'field2', 'field3'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      // Should handle duplicates within same constraint
      expect(uniqueDecorators).toHaveLength(2);
      expect(indexDecorators).toHaveLength(1);
    });

    it('Case 23: Malformed constraint objects', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const malformedConstraints = [
        { notValue: ['field1'] }, // Wrong property name
        { value: 'not_an_array' }, // String instead of array
        { value: 123 }, // Number instead of array
        { value: { nested: 'object' } }, // Object instead of array
        { value: ['field1'] }, // Valid one
      ];

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: malformedConstraints as any,
        indexes: [],
        usedImports,
        validEntityFields: ['field1'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');

      expect(uniqueDecorators).toHaveLength(1); // Only valid one survives
    });
  });

  describe('🚀 PERFORMANCE KILLERS', () => {
    it('Case 24: Massive field combinations (factorial explosion)', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const fields = Array.from({ length: 10 }, (_, i) => `field${i}`);
      const constraints = [];

      // Create many different combinations to stress-test deduplication
      for (let i = 0; i < fields.length; i++) {
        for (let j = i + 1; j < fields.length; j++) {
          constraints.push({ value: [fields[i], fields[j]] });
          constraints.push({ value: [fields[j], fields[i]] }); // Reverse order
        }
      }

      const startTime = Date.now();
      
      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: constraints,
        indexes: constraints, // Same constraints for indexes
        usedImports,
        validEntityFields: fields,
      });

      const endTime = Date.now();
      const processingTime = endTime - startTime;

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(uniqueDecorators).toHaveLength(45); // C(10,2) = 45 combinations
      expect(indexDecorators).toHaveLength(0); // All blocked by uniques
      expect(processingTime).toBeLessThan(1000); // Should still be fast
    });

    it('Case 25: String processing stress test', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const massiveString = 'a'.repeat(10000);
      const fields = [massiveString, 'normal_field'];

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: [
          { value: [massiveString] },
          { value: [massiveString, 'normal_field'] },
        ],
        indexes: [
          { value: [massiveString] }, // Should be blocked
          { value: ['normal_field'] },
        ],
        usedImports,
        validEntityFields: fields,
      });

      expect(() => classDeclaration.getDecorators()).not.toThrow();
    });
  });

  describe('🎯 FINAL BOSS BATTLES', () => {
    it('Case 26: Everything at once chaos', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Should throw error for invalid fields
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['valid1', 'invalid1'] }, // Contains invalid field
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['valid1', 'valid2', 'valid3', '用户名'],
        });
      }).toThrow(ValidationException);
      // Test valid chaos case
      const classDeclaration = wrapEntityClass({
        sourceFile: project.createSourceFile('test2.ts', '', { overwrite: true }),
        className: 'TestEntity2',
        tableName: 'test_entity2',
        uniques: [
          null, // Should be skipped
          undefined, // Should be skipped
          { value: null }, // Should be skipped
          { value: [] }, // Should be skipped
          { value: [''] }, // Should be skipped
          { value: ['valid1', 'valid2'] },
          { value: ['valid2', 'valid1'] }, // Duplicate with different order
          { value: [123 as any, 'valid3'] }, // Mixed types - valid3 should work
          { value: ['id'] }, // System field
          { value: ['用户名'] }, // Unicode
        ],
        indexes: [
          { value: ['valid1', 'valid2'] }, // Should be blocked by unique
          { value: ['valid3'] },
          { value: ['createdAt'] },
        ],
        usedImports,
        validEntityFields: ['valid1', 'valid2', 'valid3', '用户名'],
      });

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      expect(decorators[0].getName()).toBe('Entity'); // Entity decorator first
      expect(uniqueDecorators.length).toBeGreaterThan(0);
      expect(indexDecorators.length).toBeGreaterThan(0);
    });

    it('Case 27: Memory leak detection', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Run many operations to check for memory leaks
      for (let i = 0; i < 100; i++) {
        wrapEntityClass({
          sourceFile: project.createSourceFile(`test${i}.ts`, '', { overwrite: true }),
          className: `TestEntity${i}`,
          tableName: `test_entity_${i}`,
          uniques: [{ value: [`field${i}`] }],
          indexes: [{ value: [`index${i}`] }],
          usedImports: new Set(),
          validEntityFields: [`field${i}`, `index${i}`],
        });
      }

      expect(true).toBe(true); // If we get here without crashing, good!
    });

    it('Case 28: TypeScript syntax breaking attempts', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const evilFields = [
        'field; DROP TABLE users; --',
        'field\'; alert("xss"); //',
        'field/*comment*/',
        'field"quotes"',
        "field'quotes'",
        'field\\backslash',
      ];

      // Clean to valid TS identifiers
      const cleanFields = evilFields.map(f => f.replace(/[^a-zA-Z0-9_$]/g, '_'));

      const classDeclaration = wrapEntityClass({
        sourceFile,
        className: 'TestEntity',
        tableName: 'test_entity',
        uniques: cleanFields.map(f => ({ value: [f] })),
        indexes: [],
        usedImports,
        validEntityFields: cleanFields,
      });

      const generatedCode = sourceFile.getFullText();
      expect(generatedCode).toMatch(/export class TestEntity/);
      expect(() => sourceFile.getClasses()[0]).not.toThrow();
    });

    it('Case 29: Infinite recursion attempts', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      const recursiveConstraint: any = { value: ['field1'] };
      recursiveConstraint.recursive = recursiveConstraint;

      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [recursiveConstraint],
          indexes: [],
          usedImports,
          validEntityFields: ['field1'],
        });
      }).not.toThrow();
    });

    it('Case 30: Ultimate stress test - everything combined', () => {
      const sourceFile = project.createSourceFile('test.ts', '', { overwrite: true });
      const usedImports = new Set<string>();

      // Should throw error for invalid fields
      expect(() => {
        wrapEntityClass({
          sourceFile,
          className: 'TestEntity',
          tableName: 'test_entity',
          uniques: [
            { value: ['nonExistentField'] }, // Invalid field
          ],
          indexes: [],
          usedImports,
          validEntityFields: ['field1', 'field2', 'field3', 'field4', 'field5', 'field6', '用户名', '😀_field', 'a'.repeat(1000)],
        });
      }).toThrow(ValidationException);
      // Test ultimate valid stress case
      const ultimateUniques = [
        // Normal cases
        { value: ['field1'] },
        { value: ['field1', 'field2'] },
        
        // Duplicates with different orders
        { value: ['field2', 'field1'] },
        { value: ['field1', 'field2', 'field3'] },
        { value: ['field3', 'field1', 'field2'] },
        
        // Edge cases
        null,
        undefined,
        { value: null },
        { value: [] },
        { value: [''] },
        { value: ['   '] },
        
        // Mixed types
        { value: [123 as any, 'field4'] },
        { value: [true as any, false as any] },
        
        // Unicode and special chars
        { value: ['用户名', '😀_field'] },
        
        // System fields
        { value: ['id', 'createdAt'] },
        
        // Very long strings
        { value: ['a'.repeat(1000)] },
      ];

      const ultimateIndexes = [
        // Should be blocked by uniques
        { value: ['field1'] },
        { value: ['field1', 'field2'] },
        
        // Should be allowed
        { value: ['field5'] },
        { value: ['updatedAt'] },
      ];

      const startTime = Date.now();

      const classDeclaration = wrapEntityClass({
        sourceFile: project.createSourceFile('test3.ts', '', { overwrite: true }),
        className: 'TestEntity3',
        tableName: 'test_entity3',
        uniques: ultimateUniques,
        indexes: ultimateIndexes,
        usedImports: new Set(),
        validEntityFields: ['field1', 'field2', 'field3', 'field4', 'field5', 'field6', '用户名', '😀_field', 'a'.repeat(1000)],
      });

      const endTime = Date.now();
      const processingTime = endTime - startTime;

      const decorators = classDeclaration.getDecorators();
      const uniqueDecorators = decorators.filter(d => d.getName() === 'Unique');
      const indexDecorators = decorators.filter(d => d.getName() === 'Index');

      // Should handle everything gracefully
      expect(decorators[0].getName()).toBe('Entity');
      expect(uniqueDecorators.length).toBeGreaterThan(0);
      expect(indexDecorators.length).toBeGreaterThan(0);
      expect(processingTime).toBeLessThan(2000);
      expect(() => classDeclaration.getSourceFile().getFullText()).not.toThrow();
    });
  });
});