import { lookupFieldOrRelation } from './lookup-field-or-relation';
import { resolvePathWithJoin } from './resolve-path-with-join';

const OPERATORS = [
  '_eq',
  '_neq',
  '_gt',
  '_gte',
  '_lt',
  '_lte',
  '_in',
  '_not_in',
  '_between',
  '_not',
  '_is_null',
  '_count',
  '_eq_set',
  '_contains',
  '_starts_with',
  '_ends_with',
];

export function buildJoinTree({
  meta,
  fields,
  filter,
  sort,
  rootAlias,
  metadataGetter,
  log = [],
}: {
  meta: any;
  fields?: string | string[];
  filter?: any;
  sort?: string[];
  rootAlias: string;
  metadataGetter: (tableName: string) => any;
  log?: string[];
}): {
  joinArr: { alias: string; parentAlias: string; propertyPath: string }[];
  selectArr: string[];
  sortArr: { alias: string; field: string; fullPath: string; direction: 'ASC' | 'DESC' }[];
} {
  const joinArr: {
    alias: string;
    parentAlias: string;
    propertyPath: string;
  }[] = [];
  const selectSet = new Set<string>();
  const sortArr: {
    alias: string;
    field: string;
    fullPath: string;
    direction: 'ASC' | 'DESC';
  }[] = [];

  const addJoin = (path: string[]) => {
    if (path.length === 0) return;
    let currentMeta = meta;
    let currentAlias = rootAlias;
    let parentAlias = rootAlias;

    for (let i = 0; i < path.length; i++) {
      const segment = path[i];
      const found = lookupFieldOrRelation(currentMeta, segment);
      if (!found || found.kind !== 'relation') return;

      parentAlias = currentAlias;
      currentAlias = `${rootAlias}_${path.slice(0, i + 1).join('_')}`;
      const propertyPath = segment;

      if (!joinArr.find((j) => j.alias === currentAlias)) {
        joinArr.push({ alias: currentAlias, parentAlias, propertyPath });
        log.push?.(
          `+ Add join path: ${parentAlias}.${propertyPath} → alias = ${currentAlias}`,
        );
      }

      currentMeta = metadataGetter(found.type);
    }

    return {
      parentAlias,
      propertyPath: path[path.length - 1],
      alias: currentAlias,
    };
  };

  const addSelect = (path: string[]) => {
    addJoin(path.slice(0, -1));
    for (let i = 0; i < path.length - 1; i++) {
      const subPath = path.slice(0, i + 1);
      const res = resolvePathWithJoin({
        meta,
        path: subPath,
        rootAlias,
        addJoin,
        metadataGetter,
      });
      if (!res) {
        log.push?.(`! Skip select: path not resolved for ${subPath.join('.')}`);
        continue;
      }
      selectSet.add(`${res.alias}.id`);
      log.push?.(`+ Add select (relation auto id): ${res.alias}.id`);
    }

    const res = resolvePathWithJoin({ meta, path, rootAlias, addJoin, metadataGetter });
    if (!res) {
      log.push?.(`! Skip select: path not resolved for ${path.join('.')}`);
      return;
    }

    if (res.lastField.kind === 'field') {
      const fieldToAdd = `${res.alias}.${res.lastField.propertyName}`;
      selectSet.add(fieldToAdd);
      log.push?.(`+ Add select: ${fieldToAdd}`);
    } else {
      const result = addJoin(path);
      if (result) {
        log.push?.(
          `+ Add join (select relation): ${result.parentAlias}.${result.propertyPath} → alias: ${result.alias}`,
        );
      }
      selectSet.add(`${res.alias}.id`);
      log.push?.(`+ Add select (relation.id): ${res.alias}.id`);
    }
  };

  const addWildcardSelect = (path: string[]) => {
    for (let i = 0; i < path.length; i++) {
      const subPath = path.slice(0, i + 1);
      const res = resolvePathWithJoin({
        meta,
        path: subPath,
        rootAlias,
        addJoin,
        metadataGetter,
      });
      if (!res) {
        log.push?.(`! Skip select: path not resolved for ${subPath.join('.')}`);
        continue;
      }
      selectSet.add(`${res.alias}.id`);
      log.push?.(`+ Add select (relation auto id): ${res.alias}.id`);
    }

    const res = resolvePathWithJoin({ meta, path, rootAlias, addJoin, metadataGetter });
    if (!res) return;

    for (const col of res.lastMeta.columns) {
      const colName = col.name || col.propertyName;
      if (
        res.lastMeta.relations?.some((r: any) => r.propertyName === colName)
      ) {
        log.push?.(
          `- Skip column "${colName}" because it conflicts with relation`,
        );
        continue;
      }
      selectSet.add(`${res.alias}.${colName}`);
      log.push?.(`+ Add select ( * ): ${res.alias}.${colName}`);
    }

    for (const rel of res.lastMeta.relations) {
      const relPath = [...path, rel.propertyName];
      const childResult = addJoin(relPath);
      if (childResult) {
        selectSet.add(`${childResult.alias}.id`);
        log.push?.(
          `+ Add select (wildcard relation.id): ${childResult.alias}.id`,
        );
        
      }
    }
  };

  const normalizePaths = (input?: string | string[]): string[][] => {
    if (!input) return [];
    if (typeof input === 'string') {
      return input.split(',').map((s) => s.trim().split('.'));
    }
    return input.map((s) => s.trim().split('.'));
  };

  const fieldPaths = normalizePaths(fields ? fields : '*');
  selectSet.add(`${rootAlias}.id`);
  log.push?.(`+ Add select: ${rootAlias}.id`);
  log.push?.(`Build from fields: ${JSON.stringify(fieldPaths)}`);
  for (const path of fieldPaths) {
    const last = path[path.length - 1];
    if (last === '*') addWildcardSelect(path.slice(0, -1));
    else addSelect(path);
  }

  const extractPathsFromFilter = (
    f: any,
    basePath: string[] = [],
    currentMeta = meta,
  ) => {
    if (!f || typeof f !== 'object') return;
    if (Array.isArray(f)) {
      for (const item of f) extractPathsFromFilter(item, basePath, currentMeta);
      return;
    }
    for (const key in f) {
      if (['_and', '_or'].includes(key)) {
        extractPathsFromFilter(f[key], basePath, currentMeta);
      } else if (!OPERATORS.includes(key)) {
        const path = [...basePath, key];
        const found = lookupFieldOrRelation(currentMeta, key);
        if (!found) continue;

        if (found.kind === 'relation') {
          const result = addJoin(path);
          if (result) {
            log.push?.(
              `+ Add join (filter): ${result.parentAlias}.${result.propertyPath} → alias: ${result.alias}`,
            );
          }

          const nextMeta = metadataGetter(found.type);
          const val = f[key];
          if (typeof val === 'object') {
            extractPathsFromFilter(val, path, nextMeta);
          }
        } else {
          const val = f[key];
          if (typeof val === 'object') {
            extractPathsFromFilter(val, path, currentMeta);
          }
        }
      }
    }
  };
  log.push?.(`Build from filter`);
  extractPathsFromFilter(filter);

  const sortPaths = normalizePaths(sort);
  log.push?.(`Build from sort: ${JSON.stringify(sortPaths)}`);
  for (const path of sortPaths) {
    const result = addJoin(path.slice(0, -1));
    if (result) {
      log.push?.(
        `+ Add join (sort): ${result.parentAlias}.${result.propertyPath}`,
      );
    }

    const res = resolvePathWithJoin({ meta, path, rootAlias, addJoin, metadataGetter });
    if (!res) continue;

    if (res.lastField.kind === 'field') {
      // Store full path for matching with parsedSort
      const fullPath = path.join('.');
      sortArr.push({
        alias: res.alias,
        field: res.lastField.propertyName,
        fullPath: fullPath,
        direction: 'ASC',
      });
      log.push?.(`+ Add sort: ${res.alias}.${res.lastField.propertyName} (path: ${fullPath})`);
    }
  }
  return {
    joinArr,
    selectArr: Array.from(selectSet),
    sortArr,
  };
}
