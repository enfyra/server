import { lookupFieldOrRelation } from './lookup-field-or-relation';

/**
 * Resolve a path (e.g. ['table', 'relation', 'field']) with joins
 * @param meta Current table metadata
 * @param path Path segments to resolve
 * @param rootAlias Root table alias
 * @param addJoin Function to add join
 * @param metadataGetter Function to get metadata for a table name
 */
export function resolvePathWithJoin({
  meta,
  path,
  rootAlias,
  addJoin,
  metadataGetter,
}: {
  meta: any;
  path: string[];
  rootAlias: string;
  addJoin: (path: string[]) => any;
  metadataGetter: (tableName: string) => any;
}):
  | {
      alias: string;
      parentAlias: string;
      lastMeta: any;
      lastField: {
        kind: 'field' | 'relation';
        propertyName: string;
        type: string;
        relationType?: string;
      };
    }
  | undefined {
  let currentMeta = meta;
  let currentAlias = rootAlias;
  let parentAlias = rootAlias;

  for (let i = 0; i < path.length; i++) {
    const segment = path[i];
    const found = lookupFieldOrRelation(currentMeta, segment);
    if (!found) {
      return undefined;
    }

    if (found.kind === 'field') {
      if (i !== path.length - 1) {
        throw new Error(
          `Invalid path: "${segment}" is a field on table "${currentMeta.name}", but path continues.`,
        );
      }

      return {
        alias: currentAlias,
        parentAlias,
        lastMeta: currentMeta,
        lastField: found,
      };
    }

    const joinPath = path.slice(0, i + 1);
    addJoin(joinPath);

    parentAlias = currentAlias;
    currentAlias = `${currentAlias}_${segment}`;
    currentMeta = metadataGetter(found.type); // Get metadata for target table
    
    if (!currentMeta) {
      return undefined;
    }
  }

  return {
    alias: currentAlias,
    parentAlias,
    lastMeta: currentMeta,
    lastField: lookupFieldOrRelation(currentMeta, 'id'),
  };
}
