import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '@enfyra/kernel';

describe('Provision M2M inverse junction column swap', () => {
  function buildInverseJunctionColumns(owningRelation: {
    tableName: string;
    propertyName: string;
    targetTable: string;
    junctionSourceColumn?: string;
    junctionTargetColumn?: string;
  }) {
    const owningSourceColumn =
      owningRelation.junctionSourceColumn ||
      getForeignKeyColumnName(owningRelation.tableName);
    const owningTargetColumn =
      owningRelation.junctionTargetColumn ||
      getForeignKeyColumnName(owningRelation.targetTable);
    return {
      junctionTableName: getJunctionTableName(
        owningRelation.tableName,
        owningRelation.propertyName,
        owningRelation.targetTable,
      ),
      junctionSourceColumn: owningTargetColumn,
      junctionTargetColumn: owningSourceColumn,
    };
  }

  it('should swap junction columns from owning side for enfyra_pre_hook.methods', () => {
    const owning = {
      tableName: 'enfyra_pre_hook',
      propertyName: 'methods',
      targetTable: 'enfyra_method',
      junctionSourceColumn: 'preHookDefinitionId',
      junctionTargetColumn: 'methodDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('methodDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('preHookDefinitionId');
    expect(inverse.junctionTableName).toBe(
      getJunctionTableName(
        'enfyra_pre_hook',
        'methods',
        'enfyra_method',
      ),
    );
  });

  it('should swap junction columns from owning side for enfyra_post_hook.methods', () => {
    const owning = {
      tableName: 'enfyra_post_hook',
      propertyName: 'methods',
      targetTable: 'enfyra_method',
      junctionSourceColumn: 'postHookDefinitionId',
      junctionTargetColumn: 'methodDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('methodDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('postHookDefinitionId');
  });

  it('should swap junction columns for enfyra_route.availableMethods', () => {
    const owning = {
      tableName: 'enfyra_route',
      propertyName: 'availableMethods',
      targetTable: 'enfyra_method',
      junctionSourceColumn: 'routeDefinitionId',
      junctionTargetColumn: 'methodDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('methodDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('routeDefinitionId');
  });

  it('should swap junction columns for enfyra_method.routes', () => {
    const owning = {
      tableName: 'enfyra_method',
      propertyName: 'routes',
      targetTable: 'enfyra_route',
      junctionSourceColumn: 'methodDefinitionId',
      junctionTargetColumn: 'routeDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('routeDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('methodDefinitionId');
  });

  it('should swap junction columns for enfyra_route_permission.allowedUsers', () => {
    const owning = {
      tableName: 'enfyra_route_permission',
      propertyName: 'allowedUsers',
      targetTable: 'enfyra_user',
      junctionSourceColumn: 'routePermissionDefinitionId',
      junctionTargetColumn: 'userDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('userDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('routePermissionDefinitionId');
  });

  it('should fallback to getForeignKeyColumnName when owning has no explicit junction columns', () => {
    const owning = {
      tableName: 'tag_definition',
      propertyName: 'posts',
      targetTable: 'post_definition',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('post_definitionId');
    expect(inverse.junctionTargetColumn).toBe('tag_definitionId');
  });

  it('getJunctionColumnNames should produce consistent columns for owning side', () => {
    const { sourceColumn, targetColumn } = getJunctionColumnNames(
      'enfyra_pre_hook',
      'methods',
      'enfyra_method',
    );
    expect(sourceColumn).toBe('sourceId');
    expect(targetColumn).toBe('targetId');
  });

  it('should never produce mixed naming when swapping from owning with explicit columns', () => {
    const snakeCaseTables = [
      {
        tableName: 'enfyra_pre_hook',
        propertyName: 'methods',
        targetTable: 'enfyra_method',
        junctionSourceColumn: 'preHookDefinitionId',
        junctionTargetColumn: 'methodDefinitionId',
      },
      {
        tableName: 'enfyra_post_hook',
        propertyName: 'methods',
        targetTable: 'enfyra_method',
        junctionSourceColumn: 'postHookDefinitionId',
        junctionTargetColumn: 'methodDefinitionId',
      },
      {
        tableName: 'enfyra_guard',
        propertyName: 'methods',
        targetTable: 'enfyra_method',
        junctionSourceColumn: 'enfyra_guardId',
        junctionTargetColumn: 'enfyra_methodId',
      },
    ];
    for (const owning of snakeCaseTables) {
      const inverse = buildInverseJunctionColumns(owning);
      expect(inverse.junctionSourceColumn).toBe(owning.junctionTargetColumn);
      expect(inverse.junctionTargetColumn).toBe(owning.junctionSourceColumn);
    }
  });

  it('self-referencing M2M should swap correctly', () => {
    const owning = {
      tableName: 'enfyra_user',
      propertyName: 'friends',
      targetTable: 'enfyra_user',
      junctionSourceColumn: 'enfyra_userId',
      junctionTargetColumn: 'friendsId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('friendsId');
    expect(inverse.junctionTargetColumn).toBe('enfyra_userId');
  });
});
