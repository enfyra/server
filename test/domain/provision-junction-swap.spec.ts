import {
  getForeignKeyColumnName,
  getJunctionTableName,
  getJunctionColumnNames,
} from '../../src/kernel/query';

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

  it('should swap junction columns from owning side for pre_hook_definition.methods', () => {
    const owning = {
      tableName: 'pre_hook_definition',
      propertyName: 'methods',
      targetTable: 'method_definition',
      junctionSourceColumn: 'preHookDefinitionId',
      junctionTargetColumn: 'methodDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('methodDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('preHookDefinitionId');
    expect(inverse.junctionTableName).toBe(
      'pre_hook_definition_methods_method_definition',
    );
  });

  it('should swap junction columns from owning side for post_hook_definition.methods', () => {
    const owning = {
      tableName: 'post_hook_definition',
      propertyName: 'methods',
      targetTable: 'method_definition',
      junctionSourceColumn: 'postHookDefinitionId',
      junctionTargetColumn: 'methodDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('methodDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('postHookDefinitionId');
  });

  it('should swap junction columns for route_definition.availableMethods', () => {
    const owning = {
      tableName: 'route_definition',
      propertyName: 'availableMethods',
      targetTable: 'method_definition',
      junctionSourceColumn: 'routeDefinitionId',
      junctionTargetColumn: 'methodDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('methodDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('routeDefinitionId');
  });

  it('should swap junction columns for method_definition.routes', () => {
    const owning = {
      tableName: 'method_definition',
      propertyName: 'routes',
      targetTable: 'route_definition',
      junctionSourceColumn: 'methodDefinitionId',
      junctionTargetColumn: 'routeDefinitionId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('routeDefinitionId');
    expect(inverse.junctionTargetColumn).toBe('methodDefinitionId');
  });

  it('should swap junction columns for route_permission_definition.allowedUsers', () => {
    const owning = {
      tableName: 'route_permission_definition',
      propertyName: 'allowedUsers',
      targetTable: 'user_definition',
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
      'pre_hook_definition',
      'methods',
      'method_definition',
    );
    expect(sourceColumn).toBe('pre_hook_definitionId');
    expect(targetColumn).toBe('method_definitionId');
  });

  it('should never produce mixed naming when swapping from owning with explicit columns', () => {
    const snakeCaseTables = [
      {
        tableName: 'pre_hook_definition',
        propertyName: 'methods',
        targetTable: 'method_definition',
        junctionSourceColumn: 'preHookDefinitionId',
        junctionTargetColumn: 'methodDefinitionId',
      },
      {
        tableName: 'post_hook_definition',
        propertyName: 'methods',
        targetTable: 'method_definition',
        junctionSourceColumn: 'postHookDefinitionId',
        junctionTargetColumn: 'methodDefinitionId',
      },
      {
        tableName: 'guard_definition',
        propertyName: 'methods',
        targetTable: 'method_definition',
        junctionSourceColumn: 'guard_definitionId',
        junctionTargetColumn: 'method_definitionId',
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
      tableName: 'user_definition',
      propertyName: 'friends',
      targetTable: 'user_definition',
      junctionSourceColumn: 'user_definitionId',
      junctionTargetColumn: 'friendsId',
    };
    const inverse = buildInverseJunctionColumns(owning);
    expect(inverse.junctionSourceColumn).toBe('friendsId');
    expect(inverse.junctionTargetColumn).toBe('user_definitionId');
  });
});
