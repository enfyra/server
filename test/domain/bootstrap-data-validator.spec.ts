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
        enfyra_method: {},
        enfyra_route: {},
      },
      defaultData: {
        enfyra_method: [{ name: 'GET' }],
        enfyra_route: [
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
        enfyra_route: {},
        enfyra_method: {},
        enfyra_menu: {},
        enfyra_graphql: {},
        enfyra_websocket: {},
        enfyra_websocket_event: {},
        enfyra_flow: {},
        enfyra_flow_step: {},
        known_table: {},
      },
      defaultData: {
        enfyra_method: [{ name: 'GET' }],
        enfyra_route: [{ path: '/ok', availableMethods: ['GET'] }],
        enfyra_menu: [
          {
            path: '/menu',
            permission: { route: '/missing', methods: ['NOPE'] },
          },
        ],
        enfyra_pre_hook: [{ route: '/missing-hook', methods: ['GET'] }],
        enfyra_graphql: [{ table: { name: 'missing_table' } }],
        enfyra_websocket: [{ name: 'admin' }],
        enfyra_websocket_event: [{ gateway: { name: 'missing_ws' } }],
        enfyra_flow: [{ name: 'main_flow' }],
        enfyra_flow_step: [{ flow: { name: 'missing_flow' } }],
      },
      dataMigration: {},
    });

    expect(issues.map((issue) => [issue.table, issue.field])).toEqual([
      ['enfyra_pre_hook', 'route'],
      ['enfyra_menu', 'permission'],
      ['enfyra_menu', 'methods'],
      ['enfyra_graphql', 'table'],
      ['enfyra_websocket_event', 'gateway'],
      ['enfyra_flow_step', 'flow'],
    ]);
  });
});
