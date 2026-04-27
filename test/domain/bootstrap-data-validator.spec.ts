import * as fs from 'fs';
import * as path from 'path';
import { validateBootstrapDataFiles } from 'src/domain/bootstrap/utils/bootstrap-data-validator.util';

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
});
