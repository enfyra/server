import type {
  ColumnModifyDef,
  VersionedSchemaMigration,
} from '../shared/types/schema-migration.types';
import {
  ENFYRA_COLUMN_TYPE_OPTIONS,
  MONGO_COLUMN_TYPE_OPTIONS,
} from '../shared/types/column-type.types';
import { MONGO_COLUMN_TYPE_MIGRATIONS } from '../shared/utils/column-type.util';

/**
 * Declarations that converge a stored `code` column onto its native large-text
 * storage. The metadata type is unchanged; the physical contract is reapplied.
 */
function codeColumnModifications(
  columns: readonly string[],
): ColumnModifyDef[] {
  return columns.map((name) => ({
    from: {
      name,
      sqlType: { type: 'code' },
      mongoType: { type: 'code' },
    },
    to: {
      name,
      sqlType: { type: 'code' },
      mongoType: { type: 'code' },
    },
  }));
}

/**
 * Declarations that converge a stored `simple-json` column onto large-text SQL
 * storage. The logical type is unchanged; the physical contract is reapplied.
 */
function jsonColumnModifications(
  columns: readonly string[],
): ColumnModifyDef[] {
  return columns.map((name) => ({
    from: {
      name,
      sqlType: { type: 'simple-json' },
      mongoType: { type: 'json' },
    },
    to: {
      name,
      sqlType: { type: 'simple-json' },
      mongoType: { type: 'json' },
    },
  }));
}

const snapshotMigrations: VersionedSchemaMigration[] = [
  {
    fromVersion: '2.2.19-patch-1',
    toVersion: '2.3.0',
    schema: {
      mongoColumnTypesToModify: [...MONGO_COLUMN_TYPE_MIGRATIONS],
      tables: [
        {
          _unique: { name: { _eq: 'enfyra_setting' } },
          columnsToRemove: ['uniquesIndexesRepaired'],
        },
        {
          _unique: { name: { _eq: 'enfyra_column' } },
          columnsToModify: [
            {
              from: {
                name: 'type',
                sqlType: { type: 'varchar' },
                mongoType: { type: 'string' },
              },
              to: {
                name: 'type',
                sqlType: {
                  type: 'enum',
                  options: [...ENFYRA_COLUMN_TYPE_OPTIONS],
                },
                mongoType: {
                  type: 'enum',
                  options: [...MONGO_COLUMN_TYPE_OPTIONS],
                },
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_oauth_config' } },
          columnsToModify: [
            {
              from: {
                name: 'sourceCode',
                sqlType: { type: 'code' },
                mongoType: { type: 'code' },
                description:
                  'Optional script that returns an object merged into newly created OAuth users. Existing identity fields take precedence.',
              },
              to: {
                name: 'sourceCode',
                sqlType: { type: 'code' },
                mongoType: { type: 'code' },
                description:
                  'Optional OAuth lifecycle script executed inside the login transaction after the user is resolved. Receives @USER and normalized @DATA.oauth; return values are ignored.',
              },
            },
            {
              from: {
                name: 'compiledCode',
                sqlType: { type: 'code' },
                mongoType: { type: 'code' },
                description:
                  'Server-compiled JavaScript code executed by the OAuth user provisioning runtime',
              },
              to: {
                name: 'compiledCode',
                sqlType: { type: 'code' },
                mongoType: { type: 'code' },
                description:
                  'Server-compiled JavaScript code executed by the OAuth lifecycle runtime',
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_route_handler' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_pre_hook' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
          relationsToModify: [
            {
              from: {
                propertyName: 'route',
                inversePropertyName: 'preHook',
              },
              to: {
                propertyName: 'route',
                inversePropertyName: 'preHooks',
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_post_hook' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
          relationsToModify: [
            {
              from: {
                propertyName: 'route',
                inversePropertyName: 'postHook',
              },
              to: {
                propertyName: 'route',
                inversePropertyName: 'postHooks',
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_bootstrap_script' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_websocket' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_websocket_event' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_extension' } },
          columnsToModify: codeColumnModifications(['code', 'compiledCode']),
        },
        {
          _unique: { name: { _eq: 'enfyra_flow_step' } },
          columnsToModify: codeColumnModifications([
            'sourceCode',
            'compiledCode',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_guard_rule' } },
          columnsToModify: [
            {
              from: {
                name: 'type',
                sqlType: {
                  type: 'enum',
                  options: [
                    'rate_limit_by_ip',
                    'rate_limit_by_user',
                    'rate_limit_by_route',
                    'ip_whitelist',
                    'ip_blacklist',
                  ],
                },
                mongoType: {
                  type: 'enum',
                  options: [
                    'rate_limit_by_ip',
                    'rate_limit_by_user',
                    'rate_limit_by_route',
                    'ip_whitelist',
                    'ip_blacklist',
                  ],
                },
              },
              to: {
                name: 'type',
                sqlType: {
                  type: 'enum',
                  options: [
                    'rate_limit_by_ip',
                    'rate_limit_by_user',
                    'rate_limit_by_route',
                    'rate_limit_by_operation',
                    'ip_whitelist',
                    'ip_blacklist',
                  ],
                },
                mongoType: {
                  type: 'enum',
                  options: [
                    'rate_limit_by_ip',
                    'rate_limit_by_user',
                    'rate_limit_by_route',
                    'rate_limit_by_operation',
                    'ip_whitelist',
                    'ip_blacklist',
                  ],
                },
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_guard_alert' } },
          columnsToModify: [
            {
              from: {
                name: 'scope',
                sqlType: { type: 'enum', options: ['ip', 'user', 'route'] },
                mongoType: { type: 'enum', options: ['ip', 'user', 'route'] },
              },
              to: {
                name: 'scope',
                sqlType: {
                  type: 'enum',
                  options: ['ip', 'user', 'route', 'operation'],
                },
                mongoType: {
                  type: 'enum',
                  options: ['ip', 'user', 'route', 'operation'],
                },
              },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_api_token' } },
          relationsToModify: [
            {
              from: { propertyName: 'user', onDelete: 'SET NULL' },
              to: { propertyName: 'user', onDelete: 'CASCADE' },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_package' } },
          relationsToModify: [
            {
              from: { propertyName: 'installedBy', isNullable: false },
              to: { propertyName: 'installedBy', isNullable: true },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_relation' } },
          relationsToModify: [
            {
              from: { propertyName: 'targetTable', isNullable: false },
              to: { propertyName: 'targetTable', isNullable: true },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_graphql_permission' } },
          relationsToModify: [
            {
              from: {
                propertyName: 'role',
                inversePropertyName: 'graphqlPermissions',
              },
              to: { propertyName: 'role', inversePropertyName: null },
            },
            {
              from: {
                propertyName: 'allowedUsers',
                inversePropertyName: 'allowedGraphqlPermissions',
              },
              to: { propertyName: 'allowedUsers', inversePropertyName: null },
            },
          ],
        },
        {
          _unique: { name: { _eq: 'enfyra_menu_permission' } },
          relationsToModify: [
            {
              from: {
                propertyName: 'role',
                inversePropertyName: 'menuPermissions',
              },
              to: { propertyName: 'role', inversePropertyName: null },
            },
          ],
        },
      ],
    },
  },
  {
    fromVersion: '2.3.0',
    toVersion: '2.3.1',
    schema: {
      tables: [
        {
          _unique: { name: { _eq: 'enfyra_route_handler' } },
          tableToModify: {
            from: { uniques: [['route', 'method']] },
            to: { uniques: [['route', 'method'], ['routeMethodConfig']] },
          },
        },
      ],
    },
  },
  { fromVersion: '2.3.1', toVersion: '2.3.2', schema: { tables: [] } },
  {
    fromVersion: '2.3.2',
    toVersion: '2.3.3',
    schema: {
      tablesToDrop: ['enfyra_bootstrap_script'],
      tables: [
        {
          _unique: { name: { _eq: 'enfyra_column' } },
          columnsToModify: jsonColumnModifications([
            'defaultValue',
            'options',
            'metadata',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_column_rule' } },
          columnsToModify: jsonColumnModifications(['value']),
        },
        {
          _unique: { name: { _eq: 'enfyra_relation' } },
          columnsToModify: jsonColumnModifications(['metadata']),
        },
        {
          _unique: { name: { _eq: 'enfyra_table' } },
          columnsToModify: jsonColumnModifications([
            'uniques',
            'indexes',
            'metadata',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_field_permission' } },
          columnsToModify: jsonColumnModifications(['condition']),
        },
        {
          _unique: { name: { _eq: 'enfyra_route_method_config_file_field' } },
          columnsToModify: jsonColumnModifications(['allowedMimeTypes']),
        },
        {
          _unique: { name: { _eq: 'enfyra_schema_migration' } },
          columnsToModify: jsonColumnModifications(['beforeSnapshot']),
        },
        {
          _unique: { name: { _eq: 'enfyra_menu' } },
          columnsToModify: jsonColumnModifications(['permission']),
        },
        {
          _unique: { name: { _eq: 'enfyra_file_permission' } },
          columnsToModify: jsonColumnModifications(['allowedDomains']),
        },
        {
          _unique: { name: { _eq: 'enfyra_storage_config' } },
          columnsToModify: jsonColumnModifications(['credentials']),
        },
        {
          _unique: { name: { _eq: 'enfyra_flow_trigger' } },
          columnsToModify: jsonColumnModifications(['config']),
        },
        {
          _unique: { name: { _eq: 'enfyra_flow_step' } },
          columnsToModify: jsonColumnModifications(['config']),
        },
        {
          _unique: { name: { _eq: 'enfyra_flow_execution' } },
          columnsToModify: jsonColumnModifications([
            'payload',
            'completedSteps',
            'error',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_guard_rule' } },
          columnsToModify: jsonColumnModifications(['config']),
        },
        {
          _unique: { name: { _eq: 'enfyra_graphql' } },
          columnsToModify: jsonColumnModifications(['metadata']),
        },
        {
          _unique: { name: { _eq: 'enfyra_runtime_reload_log' } },
          columnsToModify: jsonColumnModifications([
            'chain',
            'steps',
            'payload',
          ]),
        },
        {
          _unique: { name: { _eq: 'enfyra_system_error' } },
          columnsToModify: jsonColumnModifications(['details']),
        },
        {
          _unique: { name: { _eq: 'enfyra_user_log' } },
          columnsToModify: jsonColumnModifications(['entries']),
        },
      ],
    },
  },
];

export default snapshotMigrations;
