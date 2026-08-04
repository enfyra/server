import { ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { MongoSchemaDiffService } from '../../src/engines/mongo/services/mongo-schema-diff.service';

describe('MongoSchemaDiffService junction changes', () => {
  it('renames an owning junction when the same Mongo relation changes property name', () => {
    const service = new MongoSchemaDiffService({
      mongoService: {} as any,
      queryBuilderService: {} as any,
    });
    const relationId = new ObjectId('507f1f77bcf86cd799439011');
    const diff = service.generateMongoSchemaDiff(
      {
        name: 'course',
        columns: [],
        relations: [
          {
            _id: new ObjectId('507f1f77bcf86cd799439012'),
            propertyName: 'teachers',
            type: 'many-to-many',
            junctionTableName: 'j_unchanged',
            junctionSourceColumn: 'sourceId',
            junctionTargetColumn: 'targetId',
          },
          {
            _id: relationId,
            propertyName: 'students',
            type: 'many-to-many',
            junctionTableName: 'j_old',
            junctionSourceColumn: 'sourceId',
            junctionTargetColumn: 'targetId',
          },
        ],
      },
      {
        name: 'course',
        columns: [],
        relations: [
          {
            _id: new ObjectId('507f1f77bcf86cd799439012'),
            propertyName: 'teachers',
            type: 'many-to-many',
            junctionTableName: 'j_unchanged',
            junctionSourceColumn: 'sourceId',
            junctionTargetColumn: 'targetId',
          },
          {
            _id: new ObjectId(relationId.toHexString()),
            propertyName: 'learners',
            type: 'many-to-many',
            junctionTableName: 'j_new',
            junctionSourceColumn: 'sourceId',
            junctionTargetColumn: 'targetId',
          },
        ],
      },
    );

    expect(diff.junctionCollections).toEqual({
      create: [],
      drop: [],
      rename: [{ oldName: 'j_old', newName: 'j_new' }],
    });
  });

  it('persists logical auto-generated index fields without the physical _id tie-breaker', async () => {
    const createIndex = vi.fn().mockResolvedValue('course_teacher_fk_idx');
    const update = vi.fn().mockResolvedValue(undefined);
    const service = new MongoSchemaDiffService({
      mongoService: {
        getDb: () => ({
          collection: () => ({ createIndex }),
        }),
      } as any,
      queryBuilderService: { update } as any,
    });
    const before = {
      name: 'course',
      columns: [],
      relations: [],
      indexes: [['createdAt'], ['updatedAt']],
    };
    const after = {
      ...before,
      relations: [
        {
          propertyName: 'teacher',
          type: 'many-to-one',
          foreignKeyColumn: 'teacher',
        },
      ],
    };
    const diff = service.generateMongoSchemaDiff(before, after);

    await service.executeMongoSchemaDiff('course', diff, before, after);

    expect(update).toHaveBeenCalledWith(
      'enfyra_table',
      expect.anything(),
      {
        indexes: [['createdAt'], ['updatedAt'], ['teacher']],
      },
    );
  });
});
