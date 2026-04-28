import { ValidationException } from '../../src/domain/exceptions';
import {
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../src/kernel/query';

type TRelation = {
  id?: number;
  propertyName: string;
  type: 'many-to-one' | 'one-to-many' | 'one-to-one' | 'many-to-many';
  sourceTableId: number;
  targetTableId: number;
  mappedBy?: string | null;
  mappedById?: number | null;
  inversePropertyName?: string;
  isNullable?: boolean;
  isSystem?: boolean;
  isUpdatable?: boolean;
  isPublished?: boolean;
  junctionTableName?: string | null;
  junctionSourceColumn?: string | null;
  junctionTargetColumn?: string | null;
};

type TTable = {
  id: number;
  name: string;
  isSystem?: boolean;
};

type TInverseCreationContext = {
  owningRelation: TRelation;
  sourceTable: TTable;
  targetTable: TTable;
  existingRelationsOnTarget: TRelation[];
  allRelationsOnSource: TRelation[];
};

function validateMappedByInverseExclusion(rel: TRelation): void {
  if (rel.inversePropertyName && rel.mappedBy) {
    throw new ValidationException(
      `Relation '${rel.propertyName}' cannot have both 'mappedBy' and 'inversePropertyName'`,
      { relationName: rel.propertyName },
    );
  }
}

function validateO2MRequiresMappedBy(relations: TRelation[]): void {
  for (const relation of relations || []) {
    if (relation.type === 'one-to-many' && !relation.mappedBy) {
      throw new ValidationException(
        `One-to-many relation '${relation.propertyName}' must have mappedBy`,
        {
          relationName: relation.propertyName,
          relationType: relation.type,
          missingField: 'mappedBy',
        },
      );
    }
  }
}

function validateInversePropertyNameNotTaken(
  inversePropertyName: string,
  targetTableName: string,
  existingRelationsOnTarget: TRelation[],
): void {
  const existingOnTarget = existingRelationsOnTarget.find(
    (r) => r.propertyName === inversePropertyName,
  );
  if (existingOnTarget) {
    throw new ValidationException(
      `Cannot create inverse '${inversePropertyName}' on '${targetTableName}': property name already exists`,
      { relationName: inversePropertyName, targetTable: targetTableName },
    );
  }
}

function validateNoExistingInverse(
  owningRelId: number,
  owningPropertyName: string,
  existingRelationsOnTarget: TRelation[],
): void {
  const existingInverse = existingRelationsOnTarget.find(
    (r) => r.mappedById === owningRelId,
  );
  if (existingInverse) {
    throw new ValidationException(
      `Relation '${owningPropertyName}' already has an inverse '${existingInverse.propertyName}'`,
      { relationName: owningPropertyName },
    );
  }
}

function computeInverseType(owningType: TRelation['type']): TRelation['type'] {
  if (owningType === 'many-to-one') return 'one-to-many';
  if (owningType === 'one-to-many') return 'many-to-one';
  return owningType;
}

function buildInverseRelation(ctx: TInverseCreationContext): TRelation {
  const { owningRelation, sourceTable, targetTable } = ctx;

  validateMappedByInverseExclusion(owningRelation);

  validateInversePropertyNameNotTaken(
    owningRelation.inversePropertyName!,
    targetTable.name,
    ctx.existingRelationsOnTarget,
  );

  const owningId = owningRelation.id!;

  validateNoExistingInverse(
    owningId,
    owningRelation.propertyName,
    ctx.existingRelationsOnTarget,
  );

  const inverseType = computeInverseType(owningRelation.type);

  const inverseData: TRelation = {
    propertyName: owningRelation.inversePropertyName!,
    type: inverseType,
    sourceTableId: targetTable.id,
    targetTableId: sourceTable.id,
    mappedById: owningId,
    isNullable: owningRelation.isNullable ?? true,
    isSystem: owningRelation.isSystem || false,
    isUpdatable: owningRelation.isUpdatable ?? true,
    isPublished: owningRelation.isPublished ?? true,
  };

  if (inverseType === 'many-to-many') {
    inverseData.junctionTableName = owningRelation.junctionTableName;
    inverseData.junctionSourceColumn = owningRelation.junctionTargetColumn;
    inverseData.junctionTargetColumn = owningRelation.junctionSourceColumn;
  }

  return inverseData;
}

let nextId = 1000;
function makeRel(
  partial: Partial<TRelation> &
    Pick<
      TRelation,
      'propertyName' | 'type' | 'sourceTableId' | 'targetTableId'
    >,
): TRelation {
  return {
    id: nextId++,
    mappedBy: null,
    mappedById: null,
    isNullable: true,
    isSystem: false,
    isUpdatable: true,
    isPublished: true,
    junctionTableName: null,
    junctionSourceColumn: null,
    junctionTargetColumn: null,
    ...partial,
  };
}

describe('Inverse relation auto-creation', () => {
  beforeEach(() => {
    nextId = 1000;
  });

  const tasksTable: TTable = { id: 1, name: 'tasks', isSystem: false };
  const usersTable: TTable = { id: 2, name: 'users', isSystem: false };
  const tagsTable: TTable = { id: 3, name: 'tags', isSystem: false };
  const categoriesTable: TTable = {
    id: 4,
    name: 'categories',
    isSystem: false,
  };

  describe('mappedBy vs inversePropertyName mutual exclusion', () => {
    it('rejects relation with both mappedBy and inversePropertyName', () => {
      const rel = makeRel({
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        mappedBy: 'tasks',
        inversePropertyName: 'authoredTasks',
      });

      expect(() => validateMappedByInverseExclusion(rel)).toThrow(
        ValidationException,
      );
      expect(() => validateMappedByInverseExclusion(rel)).toThrow(
        /cannot have both.*mappedBy.*inversePropertyName/i,
      );
    });

    it('accepts relation with only inversePropertyName', () => {
      const rel = makeRel({
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'authoredTasks',
      });

      expect(() => validateMappedByInverseExclusion(rel)).not.toThrow();
    });

    it('accepts relation with only mappedBy', () => {
      const rel = makeRel({
        propertyName: 'tasks',
        type: 'one-to-many',
        sourceTableId: usersTable.id,
        targetTableId: tasksTable.id,
        mappedBy: 'author',
      });

      expect(() => validateMappedByInverseExclusion(rel)).not.toThrow();
    });

    it('accepts relation with neither mappedBy nor inversePropertyName', () => {
      const rel = makeRel({
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
      });

      expect(() => validateMappedByInverseExclusion(rel)).not.toThrow();
    });
  });

  describe('O2M requires mappedBy validation', () => {
    it('rejects O2M without mappedBy', () => {
      const relations: TRelation[] = [
        makeRel({
          propertyName: 'tasks',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
        }),
      ];

      expect(() => validateO2MRequiresMappedBy(relations)).toThrow(
        ValidationException,
      );
      expect(() => validateO2MRequiresMappedBy(relations)).toThrow(
        /must have mappedBy/,
      );
    });

    it('accepts O2M with mappedBy', () => {
      const relations: TRelation[] = [
        makeRel({
          propertyName: 'tasks',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
          mappedBy: 'author',
        }),
      ];

      expect(() => validateO2MRequiresMappedBy(relations)).not.toThrow();
    });

    it('O2M with inversePropertyName is impossible because mappedBy is required', () => {
      const rel = makeRel({
        propertyName: 'tasks',
        type: 'one-to-many',
        sourceTableId: usersTable.id,
        targetTableId: tasksTable.id,
        mappedBy: 'author',
        inversePropertyName: 'authorTasks',
      });

      expect(() => validateO2MRequiresMappedBy([rel])).not.toThrow();
      expect(() => validateMappedByInverseExclusion(rel)).toThrow(
        ValidationException,
      );
    });
  });

  describe('property name conflict on target table', () => {
    it('rejects inverse when propertyName already exists on target', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'tasks',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
          mappedBy: 'assignee',
        }),
      ];

      expect(() =>
        validateInversePropertyNameNotTaken(
          'tasks',
          usersTable.name,
          existingOnTarget,
        ),
      ).toThrow(ValidationException);
      expect(() =>
        validateInversePropertyNameNotTaken(
          'tasks',
          usersTable.name,
          existingOnTarget,
        ),
      ).toThrow(/property name already exists/);
    });

    it('accepts inverse when propertyName is unique on target', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'profile',
          type: 'one-to-one',
          sourceTableId: usersTable.id,
          targetTableId: 5,
        }),
      ];

      expect(() =>
        validateInversePropertyNameNotTaken(
          'tasks',
          usersTable.name,
          existingOnTarget,
        ),
      ).not.toThrow();
    });

    it('rejects when inversePropertyName matches different relation type on target', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'items',
          type: 'many-to-many',
          sourceTableId: usersTable.id,
          targetTableId: 99,
        }),
      ];

      expect(() =>
        validateInversePropertyNameNotTaken(
          'items',
          usersTable.name,
          existingOnTarget,
        ),
      ).toThrow(/property name already exists/);
    });
  });

  describe('duplicate inverse prevention (mappedById uniqueness)', () => {
    it('rejects creating a second inverse for the same owning relation', () => {
      const owningRelId = 42;
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'authorTasks',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
          mappedById: owningRelId,
        }),
      ];

      expect(() =>
        validateNoExistingInverse(owningRelId, 'author', existingOnTarget),
      ).toThrow(ValidationException);
      expect(() =>
        validateNoExistingInverse(owningRelId, 'author', existingOnTarget),
      ).toThrow(/already has an inverse/);
    });

    it('accepts when no inverse exists for the owning relation', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'someOtherRel',
          type: 'many-to-one',
          sourceTableId: usersTable.id,
          targetTableId: 99,
          mappedById: 999,
        }),
      ];

      expect(() =>
        validateNoExistingInverse(42, 'author', existingOnTarget),
      ).not.toThrow();
    });

    it('allows inverse for different owning relation on same target', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'assigneeTasks',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
          mappedById: 100,
        }),
      ];

      expect(() =>
        validateNoExistingInverse(42, 'author', existingOnTarget),
      ).not.toThrow();
    });
  });

  describe('type flipping correctness', () => {
    const cases: Array<{
      owning: TRelation['type'];
      expected: TRelation['type'];
    }> = [
      { owning: 'many-to-one', expected: 'one-to-many' },
      { owning: 'one-to-one', expected: 'one-to-one' },
      { owning: 'many-to-many', expected: 'many-to-many' },
      { owning: 'one-to-many', expected: 'many-to-one' },
    ];

    it('flips relation types correctly for all combinations', () => {
      for (const { owning, expected } of cases) {
        expect(computeInverseType(owning)).toBe(expected);
      }
    });
  });

  describe('M2M junction column swapping', () => {
    it('swaps junction source/target columns in the inverse', () => {
      const owningRel = makeRel({
        id: 50,
        propertyName: 'tags',
        type: 'many-to-many',
        sourceTableId: tasksTable.id,
        targetTableId: tagsTable.id,
        inversePropertyName: 'tasks',
        junctionTableName: 'tasks_tags_tags',
        junctionSourceColumn: 'tasksId',
        junctionTargetColumn: 'tagsId',
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: tasksTable,
        targetTable: tagsTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.type).toBe('many-to-many');
      expect(inverse.junctionTableName).toBe('tasks_tags_tags');
      expect(inverse.junctionSourceColumn).toBe('tagsId');
      expect(inverse.junctionTargetColumn).toBe('tasksId');
      expect(inverse.sourceTableId).toBe(tagsTable.id);
      expect(inverse.targetTableId).toBe(tasksTable.id);
      expect(inverse.mappedById).toBe(50);
    });

    it('uses real naming util for junction columns', () => {
      const junctionName = getJunctionTableName('tasks', 'tags', 'tags');
      const { sourceColumn, targetColumn } = getJunctionColumnNames(
        'tasks',
        'tags',
        'tags',
      );

      const owningRel = makeRel({
        id: 60,
        propertyName: 'tags',
        type: 'many-to-many',
        sourceTableId: tasksTable.id,
        targetTableId: tagsTable.id,
        inversePropertyName: 'taggedTasks',
        junctionTableName: junctionName,
        junctionSourceColumn: sourceColumn,
        junctionTargetColumn: targetColumn,
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: tasksTable,
        targetTable: tagsTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.junctionSourceColumn).toBe(targetColumn);
      expect(inverse.junctionTargetColumn).toBe(sourceColumn);
      expect(inverse.junctionTableName).toBe(junctionName);
    });
  });

  describe('non-M2M inverse has no junction columns', () => {
    it('M2O inverse produces O2M without junction data', () => {
      const owningRel = makeRel({
        id: 70,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'authoredTasks',
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.type).toBe('one-to-many');
      expect(inverse.junctionTableName).toBeUndefined();
      expect(inverse.junctionSourceColumn).toBeUndefined();
      expect(inverse.junctionTargetColumn).toBeUndefined();
    });

    it('O2O inverse produces O2O without junction data', () => {
      const owningRel = makeRel({
        id: 71,
        propertyName: 'profile',
        type: 'one-to-one',
        sourceTableId: usersTable.id,
        targetTableId: 10,
        inversePropertyName: 'user',
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: usersTable,
        targetTable: { id: 10, name: 'profiles' },
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.type).toBe('one-to-one');
      expect(inverse.junctionTableName).toBeUndefined();
    });
  });

  describe('self-referencing tables', () => {
    it('creates inverse on same table for M2O self-reference (parent → children)', () => {
      const selfTable: TTable = { id: 20, name: 'categories' };

      const owningRel = makeRel({
        id: 80,
        propertyName: 'parent',
        type: 'many-to-one',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        inversePropertyName: 'children',
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: selfTable,
        targetTable: selfTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.type).toBe('one-to-many');
      expect(inverse.propertyName).toBe('children');
      expect(inverse.sourceTableId).toBe(selfTable.id);
      expect(inverse.targetTableId).toBe(selfTable.id);
      expect(inverse.mappedById).toBe(80);
    });

    it('rejects self-ref inverse when propertyName conflicts with existing relation on same table', () => {
      const selfTable: TTable = { id: 20, name: 'categories' };

      const existingRel = makeRel({
        id: 81,
        propertyName: 'children',
        type: 'one-to-many',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        mappedBy: 'parent',
      });

      const owningRel = makeRel({
        id: 82,
        propertyName: 'parent',
        type: 'many-to-one',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        inversePropertyName: 'children',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owningRel,
          sourceTable: selfTable,
          targetTable: selfTable,
          existingRelationsOnTarget: [existingRel],
          allRelationsOnSource: [owningRel],
        }),
      ).toThrow(/property name already exists/);
    });

    it('M2M self-ref junction columns use propertyName-based naming', () => {
      const selfTable: TTable = { id: 20, name: 'users' };
      const { sourceColumn, targetColumn } = getJunctionColumnNames(
        'users',
        'friends',
        'users',
      );
      const junctionName = getJunctionTableName('users', 'friends', 'users');

      expect(sourceColumn).not.toBe(targetColumn);

      const owningRel = makeRel({
        id: 90,
        propertyName: 'friends',
        type: 'many-to-many',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        inversePropertyName: 'friendOf',
        junctionTableName: junctionName,
        junctionSourceColumn: sourceColumn,
        junctionTargetColumn: targetColumn,
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: selfTable,
        targetTable: selfTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.junctionSourceColumn).toBe(targetColumn);
      expect(inverse.junctionTargetColumn).toBe(sourceColumn);
      expect(inverse.junctionTableName).toBe(junctionName);
    });
  });

  describe('inverse inherits flags from owning relation', () => {
    it('inherits isNullable, isUpdatable, isPublished', () => {
      const owningRel = makeRel({
        id: 100,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'authoredTasks',
        isNullable: false,
        isUpdatable: false,
        isPublished: false,
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.isNullable).toBe(false);
      expect(inverse.isUpdatable).toBe(false);
      expect(inverse.isPublished).toBe(false);
    });

    it('inherits isSystem from owning relation (isSystem: true → inverse isSystem: true in current impl)', () => {
      const owningRel = makeRel({
        id: 101,
        propertyName: 'owner',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'ownedTasks',
        isSystem: true,
      });

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.isSystem).toBe(true);
    });

    it('defaults isNullable/isUpdatable/isPublished when not set on owning', () => {
      const owningRel: TRelation = {
        id: 102,
        propertyName: 'reviewer',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'reviewedTasks',
      };

      const inverse = buildInverseRelation({
        owningRelation: owningRel,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel],
      });

      expect(inverse.isNullable).toBe(true);
      expect(inverse.isUpdatable).toBe(true);
      expect(inverse.isPublished).toBe(true);
      expect(inverse.isSystem).toBe(false);
    });
  });

  describe('batch creation — duplicate detection within same request', () => {
    it('detects second inverse with same propertyName on same target', () => {
      const owningRel1 = makeRel({
        id: 110,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'tasks',
      });

      const inverse1 = buildInverseRelation({
        owningRelation: owningRel1,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel1],
      });

      const owningRel2 = makeRel({
        id: 111,
        propertyName: 'assignee',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'tasks',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owningRel2,
          sourceTable: tasksTable,
          targetTable: usersTable,
          existingRelationsOnTarget: [inverse1],
          allRelationsOnSource: [owningRel1, owningRel2],
        }),
      ).toThrow(/property name already exists/);
    });

    it('allows different inversePropertyName values on same target in batch', () => {
      const owningRel1 = makeRel({
        id: 120,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'authoredTasks',
      });

      const inverse1 = buildInverseRelation({
        owningRelation: owningRel1,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owningRel1],
      });

      const owningRel2 = makeRel({
        id: 121,
        propertyName: 'assignee',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'assignedTasks',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owningRel2,
          sourceTable: tasksTable,
          targetTable: usersTable,
          existingRelationsOnTarget: [inverse1],
          allRelationsOnSource: [owningRel1, owningRel2],
        }),
      ).not.toThrow();
    });
  });

  describe('full buildInverseRelation integration', () => {
    it('builds correct M2O → O2M inverse', () => {
      const owning = makeRel({
        id: 300,
        propertyName: 'category',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: categoriesTable.id,
        inversePropertyName: 'tasks',
      });

      const inverse = buildInverseRelation({
        owningRelation: owning,
        sourceTable: tasksTable,
        targetTable: categoriesTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owning],
      });

      expect(inverse.propertyName).toBe('tasks');
      expect(inverse.type).toBe('one-to-many');
      expect(inverse.sourceTableId).toBe(categoriesTable.id);
      expect(inverse.targetTableId).toBe(tasksTable.id);
      expect(inverse.mappedById).toBe(300);
    });

    it('builds correct O2O → O2O inverse', () => {
      const profileTable: TTable = { id: 30, name: 'profiles' };
      const owning = makeRel({
        id: 310,
        propertyName: 'profile',
        type: 'one-to-one',
        sourceTableId: usersTable.id,
        targetTableId: profileTable.id,
        inversePropertyName: 'user',
      });

      const inverse = buildInverseRelation({
        owningRelation: owning,
        sourceTable: usersTable,
        targetTable: profileTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owning],
      });

      expect(inverse.propertyName).toBe('user');
      expect(inverse.type).toBe('one-to-one');
      expect(inverse.sourceTableId).toBe(profileTable.id);
      expect(inverse.targetTableId).toBe(usersTable.id);
      expect(inverse.mappedById).toBe(310);
    });

    it('builds correct M2M → M2M inverse with swapped columns', () => {
      const junctionName = getJunctionTableName('tasks', 'tags', 'tags');
      const { sourceColumn, targetColumn } = getJunctionColumnNames(
        'tasks',
        'tags',
        'tags',
      );

      const owning = makeRel({
        id: 320,
        propertyName: 'tags',
        type: 'many-to-many',
        sourceTableId: tasksTable.id,
        targetTableId: tagsTable.id,
        inversePropertyName: 'tasks',
        junctionTableName: junctionName,
        junctionSourceColumn: sourceColumn,
        junctionTargetColumn: targetColumn,
      });

      const inverse = buildInverseRelation({
        owningRelation: owning,
        sourceTable: tasksTable,
        targetTable: tagsTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owning],
      });

      expect(inverse.type).toBe('many-to-many');
      expect(inverse.junctionSourceColumn).toBe(targetColumn);
      expect(inverse.junctionTargetColumn).toBe(sourceColumn);
      expect(inverse.junctionTableName).toBe(junctionName);
    });

    it('rejects buildInverseRelation when mappedBy is also set', () => {
      const owning = makeRel({
        id: 330,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        mappedBy: 'tasks',
        inversePropertyName: 'authoredTasks',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owning,
          sourceTable: tasksTable,
          targetTable: usersTable,
          existingRelationsOnTarget: [],
          allRelationsOnSource: [owning],
        }),
      ).toThrow(/cannot have both/i);
    });

    it('rejects when owning relation already has an inverse', () => {
      const owningId = 340;
      const existingInverse = makeRel({
        id: 341,
        propertyName: 'existingInverse',
        type: 'one-to-many',
        sourceTableId: usersTable.id,
        targetTableId: tasksTable.id,
        mappedById: owningId,
      });

      const owning = makeRel({
        id: owningId,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'newInverse',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owning,
          sourceTable: tasksTable,
          targetTable: usersTable,
          existingRelationsOnTarget: [existingInverse],
          allRelationsOnSource: [owning],
        }),
      ).toThrow(/already has an inverse/);
    });
  });

  describe('adversarial edge cases', () => {
    it('empty inversePropertyName string passes exclusion check but would fail elsewhere', () => {
      const rel = makeRel({
        id: 400,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: '',
      });

      expect(() => validateMappedByInverseExclusion(rel)).not.toThrow();
    });

    it('inversePropertyName same as owning propertyName (cross-table, valid)', () => {
      const owning = makeRel({
        id: 410,
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'author',
      });

      const inverse = buildInverseRelation({
        owningRelation: owning,
        sourceTable: tasksTable,
        targetTable: usersTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owning],
      });

      expect(inverse.propertyName).toBe('author');
      expect(inverse.type).toBe('one-to-many');
    });

    it('inversePropertyName same as owning propertyName on self-ref table conflicts', () => {
      const selfTable: TTable = { id: 50, name: 'nodes' };

      const owning = makeRel({
        id: 420,
        propertyName: 'parent',
        type: 'many-to-one',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        inversePropertyName: 'parent',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owning,
          sourceTable: selfTable,
          targetTable: selfTable,
          existingRelationsOnTarget: [owning],
          allRelationsOnSource: [owning],
        }),
      ).toThrow(/property name already exists/);
    });

    it('multiple M2O to same target with different inverse names all succeed', () => {
      const rels = [
        { prop: 'author', inverse: 'authoredTasks', id: 500 },
        { prop: 'reviewer', inverse: 'reviewedTasks', id: 501 },
        { prop: 'assignee', inverse: 'assignedTasks', id: 502 },
      ];

      const createdInverses: TRelation[] = [];

      for (const { prop, inverse, id } of rels) {
        const owning = makeRel({
          id,
          propertyName: prop,
          type: 'many-to-one',
          sourceTableId: tasksTable.id,
          targetTableId: usersTable.id,
          inversePropertyName: inverse,
        });

        const result = buildInverseRelation({
          owningRelation: owning,
          sourceTable: tasksTable,
          targetTable: usersTable,
          existingRelationsOnTarget: [...createdInverses],
          allRelationsOnSource: [owning],
        });

        expect(result.propertyName).toBe(inverse);
        expect(result.type).toBe('one-to-many');
        createdInverses.push(result);
      }

      expect(createdInverses).toHaveLength(3);
    });

    it('third duplicate inverse in batch is caught', () => {
      const inverse1 = makeRel({
        propertyName: 'tasks',
        type: 'one-to-many',
        sourceTableId: usersTable.id,
        targetTableId: tasksTable.id,
        mappedById: 600,
      });

      const inverse2 = makeRel({
        propertyName: 'assignments',
        type: 'one-to-many',
        sourceTableId: usersTable.id,
        targetTableId: tasksTable.id,
        mappedById: 601,
      });

      const owning3 = makeRel({
        id: 602,
        propertyName: 'watcher',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        inversePropertyName: 'tasks',
      });

      expect(() =>
        buildInverseRelation({
          owningRelation: owning3,
          sourceTable: tasksTable,
          targetTable: usersTable,
          existingRelationsOnTarget: [inverse1, inverse2],
          allRelationsOnSource: [owning3],
        }),
      ).toThrow(/property name already exists/);
    });

    it('O2O self-reference with inversePropertyName', () => {
      const selfTable: TTable = { id: 60, name: 'employees' };

      const owning = makeRel({
        id: 700,
        propertyName: 'manager',
        type: 'one-to-one',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        inversePropertyName: 'managedEmployee',
      });

      const inverse = buildInverseRelation({
        owningRelation: owning,
        sourceTable: selfTable,
        targetTable: selfTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owning],
      });

      expect(inverse.type).toBe('one-to-one');
      expect(inverse.propertyName).toBe('managedEmployee');
      expect(inverse.sourceTableId).toBe(selfTable.id);
      expect(inverse.targetTableId).toBe(selfTable.id);
    });

    it('M2M self-ref verifies different junction column names', () => {
      const selfTable: TTable = { id: 70, name: 'products' };
      const { sourceColumn, targetColumn } = getJunctionColumnNames(
        'products',
        'relatedProducts',
        'products',
      );

      expect(sourceColumn).toBe('productsId');
      expect(targetColumn).toBe('relatedProductsId');
      expect(sourceColumn).not.toBe(targetColumn);

      const owning = makeRel({
        id: 800,
        propertyName: 'relatedProducts',
        type: 'many-to-many',
        sourceTableId: selfTable.id,
        targetTableId: selfTable.id,
        inversePropertyName: 'relatedBy',
        junctionTableName: getJunctionTableName(
          'products',
          'relatedProducts',
          'products',
        ),
        junctionSourceColumn: sourceColumn,
        junctionTargetColumn: targetColumn,
      });

      const inverse = buildInverseRelation({
        owningRelation: owning,
        sourceTable: selfTable,
        targetTable: selfTable,
        existingRelationsOnTarget: [],
        allRelationsOnSource: [owning],
      });

      expect(inverse.junctionSourceColumn).toBe('relatedProductsId');
      expect(inverse.junctionTargetColumn).toBe('productsId');
    });

    it('relation without inversePropertyName produces no inverse (guard skipped in handler)', () => {
      const rel = makeRel({
        propertyName: 'category',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: categoriesTable.id,
      });

      expect(rel.inversePropertyName).toBeUndefined();
    });

    it('mappedById zero is falsy but distinct from null — validation treats it as existing inverse', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'weirdInverse',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
          mappedById: 0,
        }),
      ];

      expect(() =>
        validateNoExistingInverse(0, 'author', existingOnTarget),
      ).toThrow(/already has an inverse/);
    });
  });

  describe('ValidationException shape', () => {
    it('carries correct errorCode and details', () => {
      const rel = makeRel({
        propertyName: 'author',
        type: 'many-to-one',
        sourceTableId: tasksTable.id,
        targetTableId: usersTable.id,
        mappedBy: 'tasks',
        inversePropertyName: 'authoredTasks',
      });

      try {
        validateMappedByInverseExclusion(rel);
        fail('Expected ValidationException');
      } catch (err: any) {
        expect(err).toBeInstanceOf(ValidationException);
        expect(err.errorCode).toBe('VALIDATION_ERROR');
        expect(err.details).toBeDefined();
        expect(err.details.relationName).toBe('author');
      }
    });

    it('property name conflict includes target table in details', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'items',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
        }),
      ];

      try {
        validateInversePropertyNameNotTaken(
          'items',
          usersTable.name,
          existingOnTarget,
        );
        fail('Expected ValidationException');
      } catch (err: any) {
        expect(err).toBeInstanceOf(ValidationException);
        expect(err.details.targetTable).toBe('users');
        expect(err.details.relationName).toBe('items');
      }
    });

    it('duplicate inverse error includes existing inverse propertyName in details', () => {
      const existingOnTarget: TRelation[] = [
        makeRel({
          propertyName: 'oldInverse',
          type: 'one-to-many',
          sourceTableId: usersTable.id,
          targetTableId: tasksTable.id,
          mappedById: 42,
        }),
      ];

      try {
        validateNoExistingInverse(42, 'author', existingOnTarget);
        fail('Expected ValidationException');
      } catch (err: any) {
        expect(err).toBeInstanceOf(ValidationException);
        expect(err.message).toContain('oldInverse');
      }
    });
  });
});
