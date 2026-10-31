import type { VersionedDataMigration } from '../shared/types/schema-migration.types';
import {
  ENFYRA_COLUMN_TYPE_OPTIONS,
  MONGO_COLUMN_TYPE_OPTIONS,
} from '../shared/types/column-type.types';

/**
 * Record targets that hold for every install regardless of the recorded version:
 * per-table overrides and column defaults the snapshot cannot express because they
 * belong to seeded rows rather than to a table contract. Re-applied on every
 * upgrade, so they are idempotent by construction.
 */
export const standingDataCorrections = {
  enfyra_field_permission: [
    {
      _unique: {
        _and: [
          {
            action: {
              _eq: 'update',
            },
          },
          {
            role: {
              _eq: null,
            },
          },
          {
            column: {
              name: {
                _eq: 'password',
              },
              table: {
                name: {
                  _eq: 'enfyra_user',
                },
              },
            },
          },
          {
            description: {
              _eq: 'Allow authenticated user to update own password via /me',
            },
          },
        ],
      },
      isSystem: true,
    },
  ],
  enfyra_menu: [
    {
      _unique: { path: { _eq: '/settings/oauth' } },
      permission: { or: [
        { route: '/enfyra_oauth_config', methods: ['GET'] },
        { route: '/enfyra_oauth_account', methods: ['GET'] },
        { route: '/enfyra_auth_origin', methods: ['GET'] },
      ] },
    },
    {
      _unique: { path: { _eq: '/settings/admin/logs' } },
      description: 'Trace system errors and user script logs',
      path: '/settings/logs',
      permission: {
        or: [
          { route: '/enfyra_system_error', methods: ['GET'] },
          { route: '/enfyra_user_log', methods: ['GET'] },
        ],
      },
    },
    {
      _unique: {
        path: {
          _eq: '/settings/admin/runtime',
        },
      },
      isPublic: false,
      path: '/settings/runtime',
    },
  ],
  enfyra_route: [
    {
      _unique: { path: { _eq: '/enfyra_auth_origin' } },
      isSystem: true,
      publicMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_relation',
        },
      },
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_menu_permission',
        },
      },
      isSystem: true,
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/reload',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/reload/metadata',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/reload/routes',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/reload/graphql',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/reload/guards',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/script/validate',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/menu/reorder',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/test/run',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/flow/trigger/:id',
        },
      },
      isSystem: true,
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/redis/overview',
        },
      },
      isSystem: true,
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/redis/keys',
        },
      },
      isSystem: true,
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/redis/key',
        },
      },
      isSystem: true,
      availableMethods: ['GET', 'POST', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/admin/redis/key/ttl',
        },
      },
      isSystem: true,
      availableMethods: ['PATCH'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_package',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_cors_origin',
        },
      },
      publicMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/login',
        },
      },
      publicMethods: ['POST'],
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/logout',
        },
      },
      publicMethods: ['POST'],
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/refresh-token',
        },
      },
      publicMethods: ['POST'],
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/api-tokens',
        },
      },
      skipRoleGuardMethods: ['GET', 'POST'],
      availableMethods: ['GET', 'POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/api-tokens/:id',
        },
      },
      skipRoleGuardMethods: ['DELETE'],
      availableMethods: ['DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/token/exchange',
        },
      },
      publicMethods: ['POST'],
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/oauth/exchange',
        },
      },
      publicMethods: ['POST'],
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/set-cookies',
        },
      },
      publicMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/providers',
        },
      },
      isSystem: true,
      publicMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/:provider',
        },
      },
      publicMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/auth/:provider/callback',
        },
      },
      publicMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/me',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET', 'PATCH'],
      availableMethods: ['GET', 'PATCH'],
    },
    {
      _unique: {
        path: {
          _eq: '/assets/:id',
        },
      },
      publicMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/metadata',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/metadata/:name',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_folder/tree',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_route',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_table',
        },
      },
      publicMethods: [],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_setting',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/me/oauth-accounts',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/graphql-schema',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_method',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_pre_hook',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_post_hook',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_route_handler',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_route_permission',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: { path: { _eq: '/enfyra_route_method_config' } },
      availableMethods: ['GET', 'PATCH'],
    },
    {
      _unique: { path: { _eq: '/enfyra_route_method_config_file_field' } },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: { path: { _eq: '/enfyra_audit' } },
      availableMethods: ['GET', 'DELETE'],
    },
    {
      _unique: { path: { _eq: '/admin/code-audit/revert' } },
      availableMethods: ['POST'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_menu',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_extension',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_extension/preview',
        },
      },
      publicMethods: [],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_storage_config',
        },
      },
      publicMethods: [],
      skipRoleGuardMethods: ['GET'],
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_oauth_config',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_oauth_account',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_websocket',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_websocket_event',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_flow',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_flow_step',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_flow_execution',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_flow_trigger',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_file_permission',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_guard_rule',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_user',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_role',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_folder',
        },
      },
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_graphql_operation',
        },
      },
      mainTable: 'enfyra_graphql_operation',
      isSystem: true,
      availableMethods: ['GET'],
    },
    {
      _unique: {
        path: {
          _eq: '/enfyra_graphql_permission',
        },
      },
      mainTable: 'enfyra_graphql_permission',
      isSystem: true,
      availableMethods: ['GET', 'POST', 'PATCH', 'DELETE'],
    },
  ],
  enfyra_graphql_operation: [
    {
      _unique: {
        name: {
          _eq: 'QUERY',
        },
      },
      name: 'QUERY',
      label: 'Query',
      description: 'Read GraphQL records, relations, and aggregate metadata',
      order: 0,
      isSystem: true,
    },
    {
      _unique: {
        name: {
          _eq: 'CREATE',
        },
      },
      name: 'CREATE',
      label: 'Create',
      description: 'Create records through GraphQL mutations',
      order: 10,
      isSystem: true,
    },
    {
      _unique: {
        name: {
          _eq: 'UPDATE',
        },
      },
      name: 'UPDATE',
      label: 'Update',
      description: 'Update records through GraphQL mutations',
      order: 20,
      isSystem: true,
    },
    {
      _unique: {
        name: {
          _eq: 'DELETE',
        },
      },
      name: 'DELETE',
      label: 'Delete',
      description: 'Delete records through GraphQL mutations',
      order: 30,
      isSystem: true,
    },
  ],
  enfyra_route_method_config: [
    {
      _unique: {
        _and: [
          { route: { path: { _eq: '/enfyra_audit' } } },
          { method: { name: { _eq: 'DELETE' } } },
        ],
      },
      available: true,
    },
  ],
  enfyra_table: [
    {
      _unique: {
        name: {
          _eq: 'enfyra_file',
        },
      },
      validateBody: false,
    },
  ],
  enfyra_column: [
    {
      _unique: {
        _and: [
          {
            table: {
              name: {
                _eq: 'enfyra_route',
              },
            },
          },
          {
            name: {
              _eq: 'icon',
            },
          },
        ],
      },
      defaultValue: 'lucide:route',
    },
    {
      _unique: {
        _and: [
          {
            table: {
              name: {
                _eq: 'enfyra_menu',
              },
            },
          },
          {
            name: {
              _eq: 'icon',
            },
          },
        ],
      },
      defaultValue: 'lucide:menu',
    },
    {
      _unique: {
        _and: [
          {
            table: {
              name: {
                _eq: 'enfyra_extension',
              },
            },
          },
          {
            name: {
              _eq: 'type',
            },
          },
        ],
      },
      options: ['page', 'widget', 'global'],
      description:
        'Type of extension (full page, embeddable widget, or global shell extension)',
    },
    {
      _unique: {
        _and: [
          {
            table: {
              name: {
                _eq: 'enfyra_folder',
              },
            },
          },
          {
            name: {
              _eq: 'icon',
            },
          },
        ],
      },
      defaultValue: 'lucide:folder',
    },
    {
      _unique: {
        _and: [
          {
            table: {
              name: {
                _eq: 'enfyra_flow',
              },
            },
          },
          {
            name: {
              _eq: 'icon',
            },
          },
        ],
      },
      defaultValue: 'lucide:workflow',
    },
  ],
};

const dataMigrations: VersionedDataMigration[] = [
  {
    fromVersion: '2.2.19-patch-1',
    toVersion: '2.3.0',
    data: {
      enfyra_column: [
        {
          _unique: {
            _and: [
              { table: { name: { _eq: 'enfyra_column' } } },
              { name: { _eq: 'type' } },
            ],
          },
          sqlType: {
            type: 'enum',
            options: [...ENFYRA_COLUMN_TYPE_OPTIONS],
          },
          mongoType: {
            type: 'enum',
            options: [...MONGO_COLUMN_TYPE_OPTIONS],
          },
        },
      ],
    },
  },
  {
    fromVersion: '2.3.0',
    toVersion: '2.3.1',
    data: {},
  },
  {
    fromVersion: '2.3.1',
    toVersion: '2.3.2',
    data: {
      enfyra_menu: [{ _unique: { path: { _eq: '/dashboard' } }, isSystem: false }],
    },
  },
  {
    fromVersion: '2.3.2',
    toVersion: '2.3.3',
    data: {
      _deletedRecords: [
        { table: 'enfyra_route', filter: { path: { _eq: '/enfyra_bootstrap_script' } } },
        { table: 'enfyra_menu', filter: { path: { _eq: '/settings/bootstrap' } } },
        { table: 'enfyra_menu', filter: { path: { _eq: '/settings/extensions' } } },
      ],
    },
  },
  {
    fromVersion: '2.3.3',
    toVersion: '2.4.0',
    data: {
      enfyra_column: [{
        _unique: { _and: [{ table: { name: { _eq: 'enfyra_column' } } }, { name: { _eq: 'type' } }] },
        sqlType: { type: 'enum', options: [...ENFYRA_COLUMN_TYPE_OPTIONS] },
        mongoType: { type: 'enum', options: [...MONGO_COLUMN_TYPE_OPTIONS] },
      }],
    },
  },
  {
    fromVersion: '2.4.0',
    toVersion: '2.4.1',
    data: {
      _deletedRecords: [
        { table: 'enfyra_route', filter: { path: { _eq: '/enfyra_auth_header' } } },
        { table: 'enfyra_route', filter: { path: { _eq: '/admin/auth-header/reorder' } } },
        { table: 'enfyra_menu', filter: { path: { _eq: '/settings/auth-headers' } } },
      ],
    },
  },
];

export default dataMigrations;
