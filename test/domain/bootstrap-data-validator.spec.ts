import * as fs from 'fs';
import * as path from 'path';
import { validateBootstrapDataFiles } from '../../src/domain/bootstrap/utils/bootstrap-data-validator.util';

const ROOT = path.resolve(__dirname, '../../data');

function loadJson(file: string) {
  return JSON.parse(fs.readFileSync(path.join(ROOT, file), 'utf8'));
}

describe('validateBootstrapDataFiles', () => {
  it('accepts current bootstrap route metadata', () => {
    const issues = validateBootstrapDataFiles({
      snapshot: loadJson('snapshot.json'),
      defaultData: loadJson('default-data.json'),
      dataMigration: loadJson('data-migration.json'),
    });

    expect(issues).toEqual([]);
  });

  it('reports unknown route mainTable and method names', () => {
    const issues = validateBootstrapDataFiles({
      snapshot: {
        method_definition: {},
        route_definition: {},
      },
      defaultData: {
        method_definition: [{ method: 'GET' }],
        route_definition: [
          {
            path: '/bad',
            mainTable: 'missing_definition',
            availableMethods: ['GET', 'NOPE'],
          },
        ],
      },
      dataMigration: {},
    });

    expect(issues.map((issue) => issue.field)).toEqual([
      'mainTable',
      'availableMethods',
    ]);
  });

  it('reports broken bootstrap references outside route records', () => {
    const issues = validateBootstrapDataFiles({
      snapshot: {
        route_definition: {},
        method_definition: {},
        menu_definition: {},
        gql_definition: {},
        websocket_definition: {},
        websocket_event_definition: {},
        flow_definition: {},
        flow_step_definition: {},
        known_table: {},
      },
      defaultData: {
        method_definition: [{ method: 'GET' }],
        route_definition: [{ path: '/ok', availableMethods: ['GET'] }],
        menu_definition: [
          {
            path: '/menu',
            permission: { route: '/missing', actions: ['POST'] },
          },
        ],
        pre_hook_definition: [{ route: '/missing-hook', methods: ['GET'] }],
        gql_definition: [{ table: { name: 'missing_table' } }],
        websocket_definition: [{ name: 'admin' }],
        websocket_event_definition: [{ gateway: { name: 'missing_ws' } }],
        flow_definition: [{ name: 'main_flow' }],
        flow_step_definition: [{ flow: { name: 'missing_flow' } }],
      },
      dataMigration: {},
    });

    expect(issues.map((issue) => [issue.table, issue.field])).toEqual([
      ['pre_hook_definition', 'route'],
      ['menu_definition', 'permission'],
      ['menu_definition', 'actions'],
      ['gql_definition', 'table'],
      ['websocket_event_definition', 'gateway'],
      ['flow_step_definition', 'flow'],
    ]);
  });
});
