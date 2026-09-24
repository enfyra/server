import { describe, expect, it } from 'vitest';
import { JoinRegistry } from '@enfyra/kernel';

const metadata = {
  tables: new Map([
    [
      'posts',
      {
        relations: [
          {
            propertyName: 'owner',
            type: 'many-to-one',
            targetTableName: 'users',
          },
        ],
      },
    ],
    [
      'comments',
      {
        relations: [
          {
            propertyName: 'owner',
            type: 'many-to-one',
            targetTableName: 'moderators',
          },
        ],
      },
    ],
  ]),
};

describe('JoinRegistry contracts', () => {
  it('rejects conflicting specifications that collide on the public join ID', () => {
    const registry = new JoinRegistry();
    expect(
      registry.registerWithParent('posts', 'owner', metadata),
    ).toBe('owner');
    expect(() =>
      registry.registerWithParent('comments', 'owner', metadata),
    ).toThrow(/join.*collision/i);
  });

  it('merges purposes and strengthens the join type for the same specification', () => {
    const registry = new JoinRegistry();
    registry.registerWithParent(
      'posts',
      'owner',
      metadata,
      'left',
      null,
      'data',
    );
    registry.registerWithParent(
      'posts',
      'owner',
      metadata,
      'inner',
      null,
      'filter',
    );
    expect(registry.get('owner')).toMatchObject({
      parentTable: 'posts',
      targetTable: 'users',
      joinType: 'inner',
      purposes: ['data', 'filter'],
    });
  });

  it('does not expose mutable registry entries', () => {
    const registry = new JoinRegistry();
    registry.registerWithParent('posts', 'owner', metadata);

    const direct = registry.get('owner')!;
    direct.id = 'changed';
    direct.purposes.push('filter');
    direct.relationMeta.targetTableName = 'changed';

    const listed = registry.getAll()[0];
    listed.joinType = 'inner';
    listed.purposes.push('sort');

    expect(registry.get('owner')).toMatchObject({
      id: 'owner',
      targetTable: 'users',
      joinType: 'left',
      purposes: ['data'],
      relationMeta: { targetTableName: 'users' },
    });
    expect(registry.has('changed')).toBe(false);
  });

  it('restores checkpoints atomically', () => {
    const registry = new JoinRegistry();
    registry.registerWithParent('posts', 'owner', metadata);
    const checkpoint = registry.createCheckpoint();
    checkpoint.push({ ...checkpoint[0], purposes: null as any });

    expect(() => registry.restoreCheckpoint(checkpoint)).toThrow();
    expect(registry.getAll()).toHaveLength(1);
    expect(registry.get('owner')?.targetTable).toBe('users');
  });

  it('restores nested checkpoint metadata without sharing references', () => {
    const registry = new JoinRegistry();
    registry.registerWithParent('posts', 'owner', metadata);
    const checkpoint = registry.createCheckpoint();

    checkpoint[0].relationMeta.targetTableName = 'changed';
    registry.restoreCheckpoint(checkpoint);
    checkpoint[0].relationMeta.targetTableName = 'changed-again';

    expect(registry.get('owner')?.relationMeta.targetTableName).toBe('changed');
  });
});
